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

RATING_START_TIMESTAMP = int(datetime(2025, 2, 24).timestamp() * 1000)
BOT_BASE_RATING = 2650
BLACK_PENALTY = 200            # Penalize black by 200 Elo (instead of 100)
INITIAL_PLAYER_RATING = 1800
MALUS_INTERVAL = 30 * 86400 * 1000  # 7 days in milliseconds
CHUNK_SIZE = 1800000           # 30 minutes in ms (for API chunking)

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

def fetch_all_games(since):
    logging.info(f"Starting fetch_all_games() from {datetime.utcfromtimestamp(since/1000)}")
    all_games = []
    current_time = int(time.time() * 1000)
    pointer = since
    while pointer < current_time:
        until = pointer + CHUNK_SIZE
        if until > current_time:
            until = current_time
        logging.info(f"Fetching games from {datetime.utcfromtimestamp(pointer/1000)} to {datetime.utcfromtimestamp(until/1000)}")
        chunk_games = fetch_games_chunk(pointer, until)
        if chunk_games:
            logging.info(f"Retrieved {len(chunk_games)} games in this chunk.")
            all_games.extend(chunk_games)
            if len(chunk_games) == 300:
                last_created = chunk_games[-1]["createdAt"]
                pointer = last_created + 1 if last_created > pointer else until
                time.sleep(1)
                continue
        pointer = until
        time.sleep(1)
    logging.info(f"Fetched a total of {len(all_games)} games.")
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
        
        # Explicitly set permissions and ownership
        shutil.chown(path, user=owner, group=group)
        os.chmod(path, mode)
        
        logging.info(f"Successfully saved JSON to {path}.")
    except Exception as e:
        logging.error(f"Error saving JSON to {path}: {e}")

def escore(elo):
    return 1 / (1 + 10 ** (elo / 400))

def model1(timecontrol):
    """Computes an adjustment based on the given time control string ('<seconds>+<increment>')."""
    try:
        t, inc = timecontrol.split('+')
        t = int(t)
        inc = int(inc)
        b1 = 158
        b2 = 251
        normalized_32 = math.log(180 + math.log(1 + 2)*b1) * b2
        return math.log(t + math.log(1 + inc)*b1) * b2 - normalized_32
    except Exception as e:
        logging.error(f"Error in model1 with timecontrol '{timecontrol}': {e}")
        return 0

# Use model1 as our adjustment function.
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
    leaderboard = load_json(LEADERBOARD_FILE, {"metadata": {"last_fetch": RATING_START_TIMESTAMP}})
    
    old_archive_count = len(archive.get("games", []))
    logging.info(f"Old archive game count: {old_archive_count}")
    
    last_fetch = leaderboard["metadata"].get("last_fetch", RATING_START_TIMESTAMP)
    new_games = fetch_all_games(last_fetch)
    logging.info(f"Found {len(new_games)} new games since last fetch.")
    
    existing_ids = set(g["id"] for g in archive.get("games", []))
    new_added = 0
    for g in new_games:
        if g["id"] not in existing_ids:
            archive["games"].append(g)
            new_added += 1
    logging.info(f"Added {new_added} new games to the archive.")
    atomic_save_json(ARCHIVE_FILE, archive)
    
    new_archive_count = len(archive.get("games", []))
    logging.info(f"New archive game count: {new_archive_count} (was {old_archive_count})")

    # Use this:
    if new_games:
        latest_game_timestamp = max(game["createdAt"] for game in new_games)
        leaderboard["metadata"]["last_fetch"] = latest_game_timestamp
        logging.info(f"Metadata last_fetch (new games, using latest game time): {latest_game_timestamp}")
    else:
        lf_utc = int(time.time() * 1000)
        leaderboard["metadata"]["last_fetch"] = lf_utc
        logging.info(f"Metadata last_fetch (no new games, using utc): {lf_utc}")

    leaderboard["metadata"]["update_interval"] = 600000  # 10 minutes
        
    players = {}
    malus_date = RATING_START_TIMESTAMP  # start malus tracking from the rating start time

    # Process games in chronological order.
    for g in sorted(archive.get("games", []), key=lambda x: x["createdAt"]):
        if g["createdAt"] < RATING_START_TIMESTAMP:
            continue

        # Apply inactivity malus if at least 7 days have elapsed.
        if g["createdAt"] - malus_date >= MALUS_INTERVAL:
            players = inactivity_malus(players)
            malus_date = g["createdAt"]
            logging.info(f"Applied inactivity malus at {datetime.utcfromtimestamp(g['createdAt']/1000)}.")
        
        tc = g.get("clock", {"initial": 180, "increment": 2})
        base = tc.get("initial", 180)
        inc = tc.get("increment", 2)
        tc_str = f"{base}+{inc}"
        
        # Determine which color the BOT (LeelaQueenOdds) played.
        bot_color = "white" if g["players"]["white"]["user"]["name"].lower() == LICHESS_USERNAME.lower() else "black"
        human_color = "black" if bot_color == "white" else "white"
        player = g["players"][human_color]["user"]["name"]
        
        # Effective bot rating calculation.
        effective_bot_rating = BOT_BASE_RATING
        if bot_color == "black":
            effective_bot_rating -= BLACK_PENALTY
        effective_bot_rating -= adjust_func(tc_str)
        
        # Initialize player record if missing.
        if player not in players:
            players[player] = {"rating": INITIAL_PLAYER_RATING, "W": 0, "D": 0, "L": 0, "last_game": "", "tc_bases": [], "tc_incs": []}
        
        # Compute game result.
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
    
    # Compute average time control (in "minutes+seconds" format without extra symbols).
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
