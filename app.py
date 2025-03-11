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
UPDATE_INTERVAL = 600  # 30 minutes (in seconds)

# Set the cutoff date to February 24, 2025.
RATING_START_TIMESTAMP = int(datetime(2025, 2, 24).timestamp() * 1000)

# Partition chunk size for full update: 30 minutes in milliseconds.
CHUNK_SIZE = 1800000  # 30 minutes

app = Flask(__name__)

# =============================================================================
# GLOBAL STATE & LOCKS
# =============================================================================
# 'leaderboard' holds computed ratings and metadata.
# 'archive' holds raw game data.
leaderboard = None
archive = None

# In-process lock for shared memory.
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
    # If no data exists, initialize metadata.
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
# ELO CALCULATION FUNCTIONS
# =============================================================================
def logistic(x):
    return 1 / (1 + math.exp(-x))

def pentanomial_expected_score(rating_diff):
    D = 173.7
    x = rating_diff / D
    a, b, c, d = -0.8, -0.2, 0.2, 0.8
    p_loss     = logistic(a - x)
    p_nearloss = logistic(b - x) - logistic(a - x)
    p_draw     = logistic(c - x) - logistic(b - x)
    p_nearwin  = logistic(d - x) - logistic(c - x)
    p_win      = 1 - logistic(d - x)
    return 0 * p_loss + 0.25 * p_nearloss + 0.5 * p_draw + 0.75 * p_nearwin + 1 * p_win

def k_factor(games_played):
    if games_played <= 30:
        return 40
    elif games_played <= 150:
        return 20
    else:
        return 10

def update_rating(player_rating, opponent_rating, result, K):
    expected = pentanomial_expected_score(player_rating - opponent_rating)
    actual = {"win": 1, "draw": 0.5, "loss": 0}[result]
    return player_rating + K * (actual - expected)

# =============================================================================
# TIME CONTROL – ADJUSTED BOT ELO & TIME CONTROL PARSING
# =============================================================================
def effective_bot_elo(bot_elo, tc):
    """
    Adjust the bot's effective rating given a time control string.
    Expected format: "base+inc" (both in seconds). Total game time is approximated as base + 40×inc.
    For very fast games (<60 sec) a bonus is applied.
    """
    if tc == "unknown":
        return bot_elo
    try:
        base, inc = tc.split("+")
        base = int(base)
        inc = int(inc)
    except Exception:
        return bot_elo
    total_seconds = base + 40 * inc
    if total_seconds < 60:
        bonus = 200
    elif total_seconds < 420:
        bonus = 200 * (420 - total_seconds) / (420 - 60)
    else:
        bonus = 0
    return bot_elo + int(round(bonus))

def parse_time_control_parts(tc):
    """
    Parse a time control string "base+inc" into its components.
    Returns a tuple (base, inc, total_seconds) if parsed successfully; otherwise (None, None, None).
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
    Extracts the value for a PGN header tag.
    For example, for tag="TimeControl", returns something like "600+15" if present.
    If not found, returns "unknown".
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
            # Append new games to the archive, avoiding duplicates.
            existing_ids = {game["id"] for game in archive["games"]}
            added_count = 0
            for game in new_games:
                if game["id"] not in existing_ids:
                    archive["games"].append(game)
                    added_count += 1
            print(f"[INFO] Added {added_count} new game(s) to the archive.")
            save_archive(archive)

            # Update last_fetch timestamp.
            if new_games:
                max_ts = max(game["createdAt"] for game in new_games)
            else:
                max_ts = last_fetch
            leaderboard["metadata"]["last_fetch"] = max_ts

            new_leaderboard = {}
            filtered_games = [g for g in archive["games"] if g["createdAt"] >= RATING_START_TIMESTAMP]
            filtered_games.sort(key=lambda g: g["createdAt"])
            for game in filtered_games:
                ts = game["createdAt"]
                # Determine the bot's baseline Elo.
                if ts >= datetime(2024, 11, 12).timestamp() * 1000:
                    bot_elo = 2450
                else:
                    bot_elo = 2100

                # Extract TimeControl from the PGN.
                pgn = game.get("pgn", "")
                tc = extract_tag_from_pgn(pgn, "TimeControl")
                
                effective_bot = effective_bot_elo(bot_elo, tc)
                # Determine which side the bot played.
                if game["players"]["black"]["user"]["name"].lower() == LICHESS_USERNAME.lower():
                    player_color = "white"
                else:
                    player_color = "black"
                player_info = game["players"][player_color]
                player = player_info["user"]["name"]
                header_elo = player_info.get("rating", 1600)
                if player not in new_leaderboard:
                    new_leaderboard[player] = {
                        "rating": max(1600, header_elo - 100),
                        "games": 0,
                        "last_game": "",
                        "tc_base_values": [],
                        "tc_inc_values": []
                    }
                # Parse time control parts.
                base, inc, total = parse_time_control_parts(tc)
                if base is not None and inc is not None:
                    new_leaderboard[player]["tc_base_values"].append(base)
                    new_leaderboard[player]["tc_inc_values"].append(inc)
                # Determine game result.
                if game["status"] == "draw":
                    result = "draw"
                elif game.get("winner") == player_color:
                    result = "win"
                else:
                    result = "loss"
                K = k_factor(new_leaderboard[player]["games"])
                new_rating = update_rating(new_leaderboard[player]["rating"], effective_bot, result, K)
                new_leaderboard[player]["rating"] = max(1600, new_rating)
                new_leaderboard[player]["games"] += 1
                game_date = datetime.utcfromtimestamp(ts / 1000).strftime("%Y.%m.%d")
                new_leaderboard[player]["last_game"] = game_date

            # Compute Average Time Control (in minutes+seconds).
            for player in new_leaderboard:
                base_vals = new_leaderboard[player].get("tc_base_values", [])
                inc_vals = new_leaderboard[player].get("tc_inc_values", [])
                if base_vals and inc_vals:
                    avg_base = sum(base_vals) / len(base_vals)
                    avg_inc = sum(inc_vals) / len(inc_vals)
                    # Convert average base seconds to minutes.
                    avg_minutes = int(round(avg_base / 60))
                    avg_inc = int(round(avg_inc))
                    new_leaderboard[player]["average_time_control"] = f"{avg_minutes}+{avg_inc}"
                else:
                    new_leaderboard[player]["average_time_control"] = "?"
                # Clean up temporary lists.
                for field in ("tc_base_values", "tc_inc_values"):
                    if field in new_leaderboard[player]:
                        del new_leaderboard[player][field]
            
            # Sort players by rating (highest first) and update metadata.
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
      /* Compact display for mobile */
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
          <td>{{ player }}</td>
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
  // Countdown timer: wait an extra 60 seconds on display, then auto-reload
  let nextUpdate = {{ next_update }};
  function updateTimer() {
    let now = Math.floor(Date.now() / 1000);
    // Add an extra 60 seconds to give the server time to update
    let diff = (nextUpdate + 60) - now;
    if (diff <= 0) {
      window.location.reload();
      return;
    }
    let minutes = Math.floor(diff / 60);
    let seconds = diff % 60;
    document.getElementById("timer").innerText = minutes + "m " + seconds + "s";
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
        # Build a sorted list of players (exclude metadata) for proper ranking.
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
