#!/usr/bin/env python3
import json, requests, time, math, os, tempfile, logging, shutil
from datetime import datetime

# ────── Logging Setup ──────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="updater.log",
    filemode="a"
)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logging.getLogger('').addHandler(console_handler)

# ────── Constants ──────
LICHESS_USERNAME = "LeelaQueenOdds"
LICHESS_TOKEN = os.environ.get("LICHESS_TOKEN")
ARCHIVE_FILE = "game_archive.json"
LEADERBOARD_FILE = "/var/www/lqo_leaderboard/static/leaderboard.json"
# Use a fixed rating start date
RATING_START_TIMESTAMP = int(datetime(2025, 2, 24).timestamp() * 1000)
BOT_BASE_RATING = 2650
BLACK_PENALTY = 200            # Penalize black by 200 Elo (instead of 100)
INITIAL_PLAYER_RATING = 1800
MALUS_INTERVAL = 30 * 86400 * 1000  # adjust as needed (here written as 30 days in ms)
CHUNK_SIZE = 1800000           # 30 minutes in ms (for API chunking)
# New constant: “recent” games are those finished within the last 3 hours.
REFETCH_DELAY = 3 * 60 * 60 * 1000  # 3 hours in ms

# ────── Helper Functions ──────

def fetch_games_chunk(since, until=None):
    headers = {"Authorization": f"Bearer {LICHESS_TOKEN}", "Accept": "application/x-ndjson"}
    params = {"since": since, "max": 300, "pgnInJson": "true", "clocks": False, "moves": False}
    if until is not None:
        params["until"] = until
    url = f"https://lichess.org/api/games/user/{LICHESS_USERNAME}"
    try:
        response = requests.get(url, headers=headers, params=params, stream=True)
    except Exception as e:
        logging.error(f"Request error: {e}. Retrying in 5s...")
        time.sleep(5)
        return fetch_games_chunk(since, until)
    if response.status_code == 429:
        logging.warning("Rate limit exceeded. Sleeping for 60 seconds...")
        time.sleep(60)
        return fetch_games_chunk(since, until)
    if response.status_code != 200:
        logging.error(f"Error: received status code {response.status_code}. Retrying in 10 seconds...")
        time.sleep(10)
        return fetch_games_chunk(since, until)
    games = []
    try:
        for line in response.iter_lines():
            if line:
                try:
                    games.append(json.loads(line))
                except json.JSONDecodeError:
                    logging.warning("Failed to decode a line; skipping it.")
    except Exception as e:
        logging.error(f"Error processing response: {e}.")
    return games

def fetch_all_games_range(lower_bound, upper_bound):
    """
    Fetches all games between lower_bound and upper_bound (both in ms).
    This loop handles chunking using CHUNK_SIZE.
    """
    logging.info(f"Fetching games from {datetime.utcfromtimestamp(lower_bound/1000)} to {datetime.utcfromtimestamp(upper_bound/1000)}")
    all_games = []
    pointer = lower_bound
    while pointer < upper_bound:
        next_until = pointer + CHUNK_SIZE
        if next_until > upper_bound:
            next_until = upper_bound
        logging.info(f"Fetching chunk from {datetime.utcfromtimestamp(pointer/1000)} to {datetime.utcfromtimestamp(next_until/1000)}")
        chunk_games = fetch_games_chunk(pointer, next_until)
        if chunk_games:
            logging.info(f"Retrieved {len(chunk_games)} games in this chunk.")
            all_games.extend(chunk_games)
            # If max limit (300) returned, there might be more games at the same timestamp.
            if len(chunk_games) == 300:
                last_created = chunk_games[-1]["createdAt"]
                pointer = last_created if last_created > pointer else next_until
                time.sleep(1)
                continue
        pointer = next_until
        time.sleep(1)
    logging.info(f"Fetched a total of {len(all_games)} games between the given bounds.")
    return all_games

def load_json(path, default):
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                data = json.load(f)
                logging.info(f"Successfully loaded {path}.")
                return data
        except json.JSONDecodeError:
            logging.error(f"Failed to load JSON from {path}. Using default data.")
    else:
        logging.info(f"{path} not found. Using default data.")
    return default

def atomic_save_json(path, data, owner='ubuntu', group='www-data', mode=0o664):
    try:
        dir_name = os.path.dirname(path) or '.'
        with tempfile.NamedTemporaryFile("w", delete=False, dir=dir_name) as tmp:
            json.dump(data, tmp, indent=2)
            tmp.flush()
            os.fsync(tmp.fileno())
        os.replace(tmp.name, path)
        shutil.chown(path, user=owner, group=group)
        os.chmod(path, mode)
        logging.info(f"Successfully saved JSON to {path}.")
    except Exception as e:
        logging.error(f"Error saving JSON to {path}: {e}")

def escore(elo):
    return 1 / (1 + 10 ** (elo / 400))

def model1(timecontrol):
    """Compute adjustment based on the time control string ('<seconds>+<increment>')."""
    try:
        t, inc = timecontrol.split('+')
        t = int(t)
        inc = int(inc)
        b1 = 158
        b2 = 251
        normalized_32 = math.log(180 + math.log(1 + 2) * b1) * b2
        return math.log(t + math.log(1 + inc) * b1) * b2 - normalized_32
    except Exception as e:
        logging.error(f"Error in model1 with timecontrol '{timecontrol}': {e}")
        return 0

adjust_func = model1

def k_thresh(games):
    if games < 30:
        return 40
    elif games < 150:
        return 20
    else:
        return 10

def inactivity_malus(lead):
    for player in lead.keys():
        if lead[player].get('BOT', False):  # Skip bot records.
            continue
        lead[player]["rating"] -= 10
        if lead[player]["rating"] < 1600:
            lead[player]["rating"] = 1600
    return lead

# ────── Main Update Routine ──────

def update():
    logging.info("Starting update routine.")
    archive = load_json(ARCHIVE_FILE, {"games": []})
    # The leaderboard JSON key names remain unchanged so that the HTML stays the same.
    leaderboard = load_json(LEADERBOARD_FILE, {"metadata": {"last_update_timestamp": RATING_START_TIMESTAMP,
                                                              "last_game_timestamp": RATING_START_TIMESTAMP}})
    
    old_archive_count = len(archive.get("games", []))
    logging.info(f"Old archive game count: {old_archive_count}")
    
    # Use the stored last_game_timestamp as our starting point.
    last_fetch = leaderboard["metadata"].get("last_game_timestamp", RATING_START_TIMESTAMP)
    current_time = int(time.time() * 1000)
    # Define cutoff: any game older than this (current_time - REFETCH_DELAY) is considered stable.
    safe_cutoff = current_time - REFETCH_DELAY

    # Fetch two ranges:
    # 1. Stable range: games between last_fetch and safe_cutoff.
    new_games_stable = fetch_all_games_range(last_fetch, safe_cutoff)
    # 2. Recent range: games between safe_cutoff and now (this range is repeatedly re-fetched until the games become stable).
    new_games_recent = fetch_all_games_range(safe_cutoff, current_time)
    
    # Combine both sets.
    new_games = new_games_stable + new_games_recent
    logging.info(f"Found {len(new_games)} new games since last fetch (stable: {len(new_games_stable)}, recent: {len(new_games_recent)}).")
    
    # Deduplicate and add new games to the archive.
    existing_ids = set(g["id"] for g in archive.get("games", []))
    new_added = 0
    # Only update the pointer with stable games (ensuring the game is definitely finished).
    latest_stable_timestamp = last_fetch

    for g in new_games:
        if g["id"] not in existing_ids:
            archive["games"].append(g)
            new_added += 1
        # Only use stable games (those with createdAt <= safe_cutoff) for moving the pointer forward.
        if g["createdAt"] <= safe_cutoff and g["createdAt"] > latest_stable_timestamp:
            latest_stable_timestamp = g["createdAt"]

    logging.info(f"Added {new_added} new games to the archive.")
    atomic_save_json(ARCHIVE_FILE, archive)
    
    new_archive_count = len(archive.get("games", []))
    logging.info(f"New archive game count: {new_archive_count} (was {old_archive_count})")
    
    # Update metadata:
    fetch_time = int(time.time() * 1000)
    # Advance pointer only to the stable cutoff
    leaderboard["metadata"]["last_game_timestamp"] = latest_stable_timestamp
    leaderboard["metadata"]["last_update_timestamp"] = fetch_time
    leaderboard["metadata"]["update_interval"] = 600000  # 10 minutes in ms
    
    # Now update the leaderboard calculations.
    players = {}
    malus_date = RATING_START_TIMESTAMP  # Start malus tracking from the rating start time.
    # Process games in the archive in chronological order.
    for g in sorted(archive.get("games", []), key=lambda x: x["createdAt"]):
        if g["createdAt"] < RATING_START_TIMESTAMP:
            continue
        # Every MALUS_INTERVAL, apply an inactivity malus.
        if g["createdAt"] - malus_date >= MALUS_INTERVAL:
            players = inactivity_malus(players)
            malus_date = g["createdAt"]
            logging.info(f"Applied inactivity malus at {datetime.utcfromtimestamp(g['createdAt']/1000)}.")
        
        tc = g.get("clock", {"initial": 180, "increment": 2})
        base = tc.get("initial", 180)
        inc = tc.get("increment", 2)
        tc_str = f"{base}+{inc}"
        
        # Determine which color BOT (LeelaQueenOdds) played.
        bot_color = "white" if g["players"]["white"]["user"]["name"].lower() == LICHESS_USERNAME.lower() else "black"
        human_color = "black" if bot_color == "white" else "white"
        player = g["players"][human_color]["user"]["name"]
        
        # Effective bot rating calculation.
        effective_bot_rating = BOT_BASE_RATING
        if bot_color == "black":
            effective_bot_rating -= BLACK_PENALTY
        effective_bot_rating -= adjust_func(tc_str)
        
        # Initialize player's record if missing.
        if player not in players:
            players[player] = {"rating": INITIAL_PLAYER_RATING, "W": 0, "D": 0, "L": 0, "last_game": "", "tc_bases": [], "tc_incs": []}
        
        # Compute result.
        if "winner" not in g:
            result = 0.5
        elif g["winner"] == human_color:
            result = 1
        else:
            result = 0
        
        # Determine K factor (halved for draws).
        total_games = players[player]["W"] + players[player]["D"] + players[player]["L"]
        Kfactor = k_thresh(total_games)
        if result == 0.5:
            Kfactor = Kfactor / 2
        
        delta = (result - escore(effective_bot_rating - players[player]["rating"])) * Kfactor
        players[player]["rating"] += delta
        players[player]["rating"] = max(1600, players[player]["rating"])
        
        # Update win/draw/loss record.
        if result == 1:
            players[player]["W"] += 1
        elif result == 0.5:
            players[player]["D"] += 1
        else:
            players[player]["L"] += 1
        
        players[player]["last_game"] = datetime.utcfromtimestamp(g["createdAt"]/1000).strftime("%Y-%m-%d")
        players[player]["tc_bases"].append(base)
        players[player]["tc_incs"].append(inc)
    
    # Compute average time control for each player.
    for p in players:
        if players[p]["tc_bases"]:
            avg_base = round(sum(players[p]["tc_bases"]) / len(players[p]["tc_bases"]))
            avg_inc = round(sum(players[p]["tc_incs"]) / len(players[p]["tc_incs"]))
        else:
            avg_base, avg_inc = 0, 0
        players[p]["Average_TC"] = f"{avg_base // 60}+{avg_inc}"
        del players[p]["tc_bases"], players[p]["tc_incs"]
    
    leaderboard.update(players)
    atomic_save_json(LEADERBOARD_FILE, leaderboard)
    logging.info("Update routine completed successfully.")

if __name__ == "__main__":
    try:
        update()
    except Exception as e:
        logging.exception(f"Unhandled exception during update: {e}")
