import os
import json
import math
import threading
import time
import requests
import fcntl  # For file-based locking (Unix)
import re
from datetime import datetime
from flask import Flask, jsonify, render_template_string

# =============================================================================
# CONFIGURATION
# =============================================================================
LICHESS_USERNAME = "LeelaQueenOdds"
LICHESS_TOKEN = os.environ.get("LICHESS_TOKEN")  # Must be set in your environment
LEADERBOARD_FILE = "leaderboard.json"
ARCHIVE_FILE = "game_archive.json"
UPDATE_INTERVAL = 600  # 10 minutes, in seconds

# Set the cutoff date for rating calculations.
RATING_START_TIMESTAMP = int(datetime(2025, 2, 24).timestamp() * 1000)

# Partition chunk size for full update: 30 minutes in milliseconds.
CHUNK_SIZE = 1800000  # 30 minutes

app = Flask(__name__)

# =============================================================================
# GLOBAL STATE & LOCKS
# =============================================================================
leaderboard = None
archive = None

leaderboard_lock = threading.Lock()

# =============================================================================
# FILE-BASED LOCK (to prevent concurrent updates across processes)
# =============================================================================
def acquire_update_lock():
    lock_file = open("update.lock", "w")
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return lock_file
    except BlockingIOError:
        lock_file.close()
        return None

def release_update_lock(lock_file):
    fcntl.flock(lock_file, fcntl.LOCK_UN)
    lock_file.close()

# =============================================================================
# PERSISTENCE: LOADING & SAVING
# =============================================================================
def load_leaderboard():
    if os.path.exists(LEADERBOARD_FILE):
        with open(LEADERBOARD_FILE, "r") as f:
            return json.load(f)
    # Initialize with basic metadata.
    return {"metadata": {"last_fetch": 0, "prevlinks": [], "next_update": int(time.time()) + UPDATE_INTERVAL}}

def save_leaderboard(lb):
    with open(LEADERBOARD_FILE, "w") as f:
        json.dump(lb, f, indent=4)

def load_archive():
    if os.path.exists(ARCHIVE_FILE):
        with open(ARCHIVE_FILE, "r") as f:
            return json.load(f)
    return {"games": []}

def save_archive(ar):
    with open(ARCHIVE_FILE, "w") as f:
        json.dump(ar, f, indent=4)

# Load persistent data on startup.
leaderboard = load_leaderboard()
archive = load_archive()

# =============================================================================
# STATISTICAL MODEL FUNCTIONS
# =============================================================================
def escore(elo):
    """
    Expected score function.
    (Statistical model using a standard logistic-like formula.)
    """
    return 1 / (1 + 10 ** (elo / 400))

def adjust1(timecontrol):
    """
    Adjustment model for time control (used for older games or lower thresholds).
    Expected timecontrol format: "base+inc" where base and inc are in seconds.
    """
    try:
        time_str, inc_str = timecontrol.split('+')
        t = int(time_str)
        inc = int(inc_str)
    except Exception:
        return 0
    b1 = 150
    b2 = 150
    # Normalize relative to a 32-second time control reference.
    normalized_32 = math.log(180 + math.log(1 + 2) * b1) * b2
    return math.log(t + math.log(1 + inc) * b1) * b2 - normalized_32

def model1(timecontrol):
    """
    Model1 adjustment function.
    """
    try:
        time_str, inc_str = timecontrol.split('+')
        t = int(time_str)
        inc = int(inc_str)
    except Exception:
        return 0
    b1 = 158
    b2 = 251    
    normalized_32 = math.log(180 + math.log(1 + 2) * b1) * b2
    return math.log(t + math.log(1 + inc) * b1) * b2 - normalized_32

def model2(timecontrol):
    """
    Alternate model2 adjustment function.
    """
    try:
        time_str, inc_str = timecontrol.split('+')
        t = int(time_str)
        inc = int(inc_str)
    except Exception:
        return 0
    b1 = 406
    b2 = 390
    ba = 45  # additional constant offset
    normalized_32 = math.log(ba + 180 + math.log(2 + 2) * b1) * b2
    return math.log(ba + t + math.log(2 + inc) * b1) * b2 - normalized_32

# For recent games we choose model1 as our adjust2.
adjust2 = model1

def k_tresh(games_played):
    """
    Returns a K-factor based on the number of games played – higher for fewer games.
    """
    thresholds = [30, 150]
    K_values = [40, 20, 10]
    for i, t in enumerate(thresholds):
        if games_played <= t:
            return K_values[i]
    return K_values[-1]

def inactivity_malus(lead):
    """
    Apply an inactivity penalty of 10 rating points to all non-bot players.
    Ensure that the rating does not fall below 1600.
    """
    for player in lead.keys():
        if lead[player].get('BOT'):
            continue
        lead[player]['rating'] -= 10
        if lead[player]['rating'] < 1600:
            lead[player]['rating'] = 1600
    return lead

# =============================================================================
# TIME CONTROL PARSING FUNCTIONS
# =============================================================================
def parse_time_control_parts(tc):
    """
    Parse a time control string "base+inc" into (base, inc, total_seconds).
    """
    if tc == "unknown":
        return (None, None, None)
    try:
        base_str, inc_str = tc.split("+")
        base = int(base_str)
        inc = int(inc_str)
        total = base + 40 * inc
        return (base, inc, total)
    except Exception:
        return (None, None, None)

def extract_tag_from_pgn(pgn, tag):
    """
    Extract the value for a PGN header tag.
    For example, returns the TimeControl from the PGN.
    """
    pattern = r'\[' + re.escape(tag) + r'\s+"([^"]+)"\]'
    match = re.search(pattern, pgn)
    if match:
        return match.group(1)
    return "unknown"

# =============================================================================
# FETCHING GAMES FROM LICHESS
# =============================================================================
def fetch_games(since, until=None):
    headers = {
        "Authorization": f"Bearer {LICHESS_TOKEN}",
        "Accept": "application/x-ndjson"
    }
    url = f"https://lichess.org/api/games/user/{LICHESS_USERNAME}"
    params = {
        "since": since,
        "max": 300,
        "pgnInJson": "true",
        "clocks": False,
        "moves": False
    }
    if until is not None:
        params["until"] = until

    fetched_games = []
    while True:
        response = requests.get(url, headers=headers, params=params, stream=True)
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 10))
            print(f"[WARNING] Rate limited (HTTP 429). Retrying in {retry_after} seconds.")
            time.sleep(retry_after)
            continue
        elif response.status_code != 200:
            print(f"[ERROR] HTTP {response.status_code} when fetching games.")
            break

        batch = []
        for line in response.iter_lines():
            if line:
                try:
                    batch.append(json.loads(line))
                except Exception as e:
                    print(f"[ERROR] JSON decode error: {e} (line: {line})")
                    continue

        print(f"[DEBUG] Batch fetched: {len(batch)} games")
        if not batch:
            break
        fetched_games.extend(batch)
        params["since"] = batch[-1]["createdAt"] + 1
        time.sleep(1)
        if len(batch) < 300:
            break
        if until is not None and params["since"] > until:
            break

    return fetched_games

# =============================================================================
# UPDATING THE ARCHIVE & LEADERBOARD
# =============================================================================
def update_leaderboard_func():
    global leaderboard, archive
    lock_file = acquire_update_lock()
    if not lock_file:
        print("[INFO] Another process is updating. Skipping this iteration.")
        return

    try:
        with leaderboard_lock:
            last_fetch = leaderboard["metadata"].get("last_fetch", 0)
            now_ms = int(time.time() * 1000)
            # Full update if this is the first run.
            if last_fetch == 0:
                print(f"[INFO] Starting full update from cutoff {RATING_START_TIMESTAMP}")
                start_ts = RATING_START_TIMESTAMP
                all_new_games = []
                while start_ts <= now_ms:
                    chunk_end = min(start_ts + CHUNK_SIZE - 1, now_ms)
                    print(f"[INFO] Fetching games from {start_ts} to {chunk_end}")
                    games_chunk = fetch_games(start_ts, until=chunk_end)
                    print(f"[DEBUG] Fetched {len(games_chunk)} games from chunk.")
                    all_new_games.extend(games_chunk)
                    start_ts = chunk_end + 1
                new_games = all_new_games
            else:
                print(f"[INFO] Incremental update: fetching games since {last_fetch}")
                new_games = fetch_games(last_fetch)
            
            print(f"[INFO] Fetched {len(new_games)} new game(s) from Lichess.")
            # Append new games to the archive avoiding duplicates.
            existing_ids = {game["id"] for game in archive["games"]}
            added_count = 0
            for game in new_games:
                if game["id"] not in existing_ids:
                    archive["games"].append(game)
                    added_count += 1
            print(f"[INFO] Added {added_count} new game(s) to the archive.")
            save_archive(archive)
            
            # Update the last_fetch timestamp.
            if new_games:
                max_ts = max(game["createdAt"] for game in new_games)
            else:
                max_ts = last_fetch
            leaderboard["metadata"]["last_fetch"] = max_ts

            # --- Begin Re-Calculation using the Statistical Model ---
            # Define thresholds (in milliseconds) for bot baseline rating.
            elo_tresh = [
                datetime.strptime('2024.11.01', "%Y.%m.%d").timestamp()*1000,
                datetime.strptime('2024.11.12', "%Y.%m.%d").timestamp()*1000
            ]
            new_leaderboard = {}
            # Consider only games after the rating start date.
            filtered_games = [g for g in archive["games"] if g["createdAt"] >= RATING_START_TIMESTAMP]
            filtered_games.sort(key=lambda g: g["createdAt"])

            for game in filtered_games:
                ts = game["createdAt"]
                # Select bot baseline and adjustment function based on date.
                if ts < elo_tresh[0]:
                    bot_elo = 1950
                    adjust = adjust1
                elif ts < elo_tresh[1]:
                    bot_elo = 2100
                    adjust = adjust1
                else:
                    bot_elo = 2650
                    adjust = adjust2

                # Extract TimeControl information either from the PGN or game header.
                pgn = game.get("pgn", "")
                tc = extract_tag_from_pgn(pgn, "TimeControl")
                if tc == "unknown":
                    tc = "180+2"  # Default/fallback; adjust as needed

                # Adjust the bot’s baseline rating with the time control factor.
                # Also adjust further if LeelaQueenOdds played as Black.
                if game["players"]["black"]["user"]["name"].lower() == LICHESS_USERNAME.lower():
                    # Bot played Black; human was White.
                    player_color = "white"
                    bot_elo_adjusted = bot_elo - adjust(tc) - 200
                else:
                    player_color = "black"
                    bot_elo_adjusted = bot_elo - adjust(tc)

                # Select the human player's information.
                player_info = game["players"][player_color]
                player = player_info["user"]["name"]

                try:
                    header_rating = int(player_info.get("rating", 1600))
                except Exception:
                    header_rating = 1600

                # Set the initial baseline for new players.
                if player not in new_leaderboard:
                    if header_rating >= 2000:
                        starting_rating = 1800
                    elif header_rating >= 1800:
                        starting_rating = header_rating - 200
                    else:
                        starting_rating = 1600
                    new_leaderboard[player] = {
                        "rating": starting_rating,
                        "games": 0,
                        "last_game": "",
                        "tc_base_values": [],
                        "tc_inc_values": []
                    }

                # Record time control values for later average calculation.
                base, inc, total = parse_time_control_parts(tc)
                if base is not None and inc is not None:
                    new_leaderboard[player]["tc_base_values"].append(base)
                    new_leaderboard[player]["tc_inc_values"].append(inc)

                # Determine game result.
                # For our model, define a mapping: win=1, draw=0.5, loss=0.
                # If the human played White, invert the result.
                if game["status"] == "draw":
                    r = 0.5
                elif game.get("winner") == player_color:
                    r = 1.0
                else:
                    r = 0.0

                # Use our statistical model to update rating.
                # The expected score is computed with:
                #    escore( bot_effective - player_rating )
                # and the rating is adjusted by a factor K.
                K_factor = k_tresh(new_leaderboard[player]["games"])
                if r == 0.5:
                    K_factor = K_factor / 2  # Halve K for draws

                # Our model subtracts the human’s rating from the bot’s effective rating.
                rating_diff = bot_elo_adjusted - new_leaderboard[player]["rating"]
                adjustment = (r - escore(rating_diff)) * K_factor
                new_rating = new_leaderboard[player]["rating"] + adjustment

                # Enforce a floor of 1600.
                new_leaderboard[player]["rating"] = max(1600, new_rating)
                new_leaderboard[player]["games"] += 1
                game_date = datetime.utcfromtimestamp(ts / 1000).strftime("%Y.%m.%d")
                new_leaderboard[player]["last_game"] = game_date

            # Compute Average Time Control (converting base seconds to minutes).
            for player in new_leaderboard:
                base_vals = new_leaderboard[player].get("tc_base_values", [])
                inc_vals = new_leaderboard[player].get("tc_inc_values", [])
                if base_vals and inc_vals:
                    avg_base = sum(base_vals) / len(base_vals)
                    avg_inc = sum(inc_vals) / len(inc_vals)
                    avg_minutes = int(round(avg_base / 60))
                    avg_inc = int(round(avg_inc))
                    new_leaderboard[player]["average_time_control"] = f"{avg_minutes}+{avg_inc}"
                else:
                    new_leaderboard[player]["average_time_control"] = "?"
                # Remove temporary lists.
                for field in ("tc_base_values", "tc_inc_values"):
                    if field in new_leaderboard[player]:
                        del new_leaderboard[player][field]

            # Sort the leaderboard by rating (highest first) and update metadata.
            sorted_lb = dict(sorted(new_leaderboard.items(), key=lambda item: item[1]["rating"], reverse=True))
            metadata = leaderboard["metadata"]
            metadata["next_update"] = int(time.time()) + UPDATE_INTERVAL
            sorted_lb["metadata"] = metadata
            leaderboard = sorted_lb
            save_leaderboard(leaderboard)

            print(f"[INFO] Leaderboard updated. Next update at {metadata['next_update']} (epoch seconds).")
    finally:
        release_update_lock(lock_file)

# =============================================================================
# BACKGROUND UPDATER THREAD
# =============================================================================
def background_updater():
    while True:
        try:
            update_leaderboard_func()
        except Exception as e:
            print(f"[ERROR] Exception during update: {e}")
        time.sleep(UPDATE_INTERVAL)

threading.Thread(target=background_updater, daemon=True).start()

# =============================================================================
# FLASK ROUTES & WEBPAGE
# =============================================================================
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>LeelaQueenOdds Leaderboard</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css">
  <link href="https://fonts.googleapis.com/css2?family=Roboto:wght@400;700&display=swap" rel="stylesheet">
  <style>
    body {
      background: linear-gradient(135deg, #ece9e6, #ffffff);
      font-family: 'Roboto', sans-serif;
      padding: 15px;
    }
    .container {
      position: relative;
      background-color: rgba(255, 255, 255, 0.9);
      border-radius: 12px;
      box-shadow: 0 6px 12px rgba(0,0,0,0.1);
      padding: 20px;
      backdrop-filter: blur(8px);
      max-width: 100%;
    }
    h1 {
      font-weight: 700;
      color: #333;
      margin-bottom: 15px;
      font-size: calc(1.3rem + 1vw);
    }
    .countdown {
      font-size: 1rem;
      color: #555;
      margin-bottom: 20px;
    }
    .table {
      margin-bottom: 0;
      width: 100%;
    }
    th {
      background-color: rgba(0, 123, 255, 0.8);
      color: #fff;
      white-space: nowrap;
    }
    tbody tr:nth-child(odd) {
      background-color: rgba(255, 255, 255, 0.7);
    }
    tbody tr:nth-child(even) {
      background-color: rgba(245, 245, 245, 0.7);
    }
    /* Enhanced colors for top 3 places */
    tr.first-place td {
      background-color: rgba(255, 215, 0, 0.3) !important;
    }
    tr.second-place td {
      background-color: rgba(192, 192, 192, 0.3) !important;
    }
    tr.third-place td {
      background-color: rgba(205, 127, 50, 0.3) !important;
    }
    .github-corner svg {
      position: absolute;
      top: 0;
      right: 0;
      border: 0;
      z-index: 10;
    }
    a.lichess-link {
      color: blue;
      text-decoration: none;
    }
    a.lichess-link:hover {
      color: blue;
      text-decoration: none;
    }
    @media (max-width: 768px) {
      body {
        padding: 8px;
      }
      .container {
        padding: 12px;
      }
      .table {
        font-size: 0.85rem;
      }
      th, td {
        padding: 0.4rem !important;
      }
      td:nth-child(1), th:nth-child(1) { min-width: 30px; } /* Rank */
      td:nth-child(2), th:nth-child(2) { min-width: 70px; } /* Player name */
      td:nth-child(3), th:nth-child(3) { min-width: 50px; } /* Rating */
      td:nth-child(4), th:nth-child(4) { min-width: 40px; } /* Games */
      td:nth-child(5), th:nth-child(5) { min-width: 80px; } /* Last Game */
      td:nth-child(6), th:nth-child(6) { min-width: 45px; } /* TC */
    }
  </style>
</head>
<body>
<div class="container">
  <a href="https://github.com/notune/lqo-leaderboard" class="github-corner" aria-label="View source on GitHub" target="_blank">
    <svg xmlns="http://www.w3.org/2000/svg" width="80" height="80" viewBox="0 0 250 250" fill="#151513" style="position: absolute; top: 0; right: 0">
      <path d="M0 0l115 115h15l12 27 108 108V0z" fill="#fff"/>
      <path class="octo-arm" d="M128 109c-15-9-9-19-9-19 3-7 2-11 2-11-1-7 3-2 3-2 4 5 2 11 2 11-3 10 5 15 9 16" style="-webkit-transform-origin: 130px 106px; transform-origin: 130px 106px"/>
      <path class="octo-body" d="M115 115s4 2 5 0l14-14c3-2 6-3 8-3-8-11-15-24 2-41 5-5 10-7 16-7 1-2 3-7 12-11 0 0 5 3 7 16 4 2 8 5 12 9s7 8 9 12c14 3 17 7 17 7-4 8-9 11-11 11 0 6-2 11-7 16-16 16-30 10-41 2 0 3-1 7-5 11l-12 11c-1 1 1 5 1 5z"/>
    </svg>
  </a>
  <h1 class="text-center">LeelaQueenOdds Leaderboard</h1>
  <div class="text-center mb-3 countdown">
    Next update in: <span id="timer"></span>
  </div>
  <div class="table-responsive">
    <table class="table">
      <thead>
        <tr>
          <th>#</th>
          <th>Player</th>
          <th>Rating</th>
          <th>Games</th>
          <th>Last Game</th>
          <th>TC</th>
        </tr>
      </thead>
      <tbody>
      {% for player, data in players[:100] %}
        <tr class="{% if loop.index == 1 %}first-place{% elif loop.index == 2 %}second-place{% elif loop.index == 3 %}third-place{% endif %}">
          <td>{{ loop.index }}</td>
          <td><a href="https://lichess.org/@/{{ player }}" class="lichess-link" target="_blank">{{ player }}</a></td>
          <td>{{ data.rating | round | int }}</td>
          <td>{{ data.games }}</td>
          <td>{{ data.last_game }}</td>
          <td>{{ data.average_time_control }}</td>
        </tr>
      {% endfor %}
      </tbody>
    </table>
  </div>
</div>
<script>
  let nextUpdate = {{ next_update }};
  // Retrieve the failedReloadCount from localStorage or initialize to 0
  let failedReloadCount = parseInt(localStorage.getItem('failedReloadCount')) || 0;

  function updateTimer() {
    let now = Math.floor(Date.now() / 1000);
    let diff = (nextUpdate + 60) - now;

    if (diff <= 0) {
      // Only reload if failed reload count is less than 3
      if (failedReloadCount < 3) {
        failedReloadCount++;
        localStorage.setItem('failedReloadCount', failedReloadCount);
        window.location.reload();
        return;
      } else {
        // Show an error message if we have failed to reload 3 times
        document.getElementById("timer").innerText = "Failed to fetch update. Please reload manually.";
      }
    } else {
      // Reset the failed reload count if the update is successful
      localStorage.setItem('failedReloadCount', 0);
      let minutes = Math.floor(diff / 60);
      let seconds = diff % 60;
      document.getElementById("timer").innerText = minutes + "m " + seconds + "s";
    }
  }

  setInterval(updateTimer, 1000);
  updateTimer();
</script>
</body>
</html>
"""

@app.route("/")
def index():
    with leaderboard_lock:
        # Create a sorted list of players (excluding metadata) for ranking.
        players = [(player, data) for player, data in leaderboard.items() if player != "metadata"]
        players.sort(key=lambda x: x[1]["rating"], reverse=True)
        next_update_val = leaderboard["metadata"].get("next_update", int(time.time()) + UPDATE_INTERVAL)
        return render_template_string(HTML_TEMPLATE, players=players, next_update=next_update_val)

@app.route("/api/leaderboard")
def api_leaderboard_route():
    with leaderboard_lock:
        return jsonify(leaderboard)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
