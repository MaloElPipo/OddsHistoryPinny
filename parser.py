from datetime import datetime
from config import TARGET_BOOKMAKERS


def parse_events(events: list, sport: str, snapshot_date: str) -> list[dict]:
    """
    Transforme les événements bruts en lignes plates prêtes pour
    l'export CSV / Google Sheets.

    Une ligne = un événement x un bookmaker.
    """
    rows = []

    for event in events:
        base = {
            "snapshot_date":  snapshot_date,
            "sport":          sport,
            "event_id":       event.get("id", ""),
            "commence_time":  event.get("commence_time", ""),
            "home_team":      event.get("home_team", ""),
            "away_team":      event.get("away_team", ""),
        }

        # Filtrer les bookmakers ciblés
        bookmakers = [
            bm for bm in event.get("bookmakers", [])
            if bm["key"] in TARGET_BOOKMAKERS
        ]

        for bm in bookmakers:
            for market in bm.get("markets", []):
                if market["key"] != "h2h":
                    continue

                outcomes = {o["name"]: o["price"] for o in market.get("outcomes", [])}

                row = {
                    **base,
                    "bookmaker":       bm["key"],
                    "bookmaker_title": bm.get("title", bm["key"]),
                    "last_update":     market.get("last_update", ""),
                    "odds_home":       outcomes.get(event.get("home_team", ""), ""),
                    "odds_away":       outcomes.get(event.get("away_team", ""), ""),
                    "odds_draw":       outcomes.get("Draw", ""),
                }
                rows.append(row)

    return rows


# Colonnes dans l'ordre pour le Sheet
COLUMNS = [
    "snapshot_date",
    "sport",
    "event_id",
    "commence_time",
    "home_team",
    "away_team",
    "bookmaker",
    "bookmaker_title",
    "last_update",
    "odds_home",
    "odds_draw",
    "odds_away",
]
