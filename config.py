import os
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# API
# ============================================================
API_KEY  = os.getenv("ODDS_API_KEY", "")
BASE_URL = "https://api.the-odds-api.com/v4/historical/sports"

# ============================================================
# SPORTS - Ajouter / retirer librement
# ============================================================
SPORTS = [
    "soccer_fifa_world_cup_qualifiers_europe",
    "soccer_fifa_world_cup_qualifiers_africa",
    "soccer_fifa_world_cup_qualifiers_asia",
    "soccer_fifa_world_cup_qualifiers_south_america",
    "soccer_uefa_european_championship",
    "soccer_uefa_nations_league",
    "soccer_copa_america",
    "soccer_africa_cup_of_nations",
    "soccer_fifa_world_cup",
    "soccer_international_friendlies",
]

# ============================================================
# BOOKMAKERS
# ============================================================
REGIONS           = "eu,uk"
TARGET_BOOKMAKERS = [
    "pinnacle",
    "betfair_ex_eu",
    "betfair_ex_uk",
]

# ============================================================
# PARAMÈTRES COTES
# ============================================================
MARKETS     = "h2h"
ODDS_FORMAT = "decimal"
DATE_FORMAT = "iso"

# ============================================================
# FENÊTRE TEMPORELLE
# ============================================================
DATE_START     = "2020-01-01T00:00:00Z"
DATE_END       = "2026-12-31T00:00:00Z"
INTERVAL_HOURS = 24          # 1 snapshot par jour

# ============================================================
# GOOGLE SHEETS
# ============================================================
SPREADSHEET_ID       = os.getenv("GOOGLE_SHEETS_ID", "")
CREDENTIALS_PATH     = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")

# Nom des onglets dans le Google Sheet
SHEET_ODDS      = "odds"
SHEET_SUMMARY   = "summary"

# ============================================================
# STOCKAGE LOCAL
# ============================================================
OUTPUT_DIR = "data/raw"
DB_PATH    = "data/odds.db"
