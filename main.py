import argparse
import time
from datetime import datetime, timedelta
from loguru import logger

from config import (
    SPORTS, DATE_START, DATE_END, INTERVAL_HOURS,
    OUTPUT_DIR, DB_PATH
)
from fetcher  import OddsFetcher
from parser   import parse_events
from storage  import DataStorage
from sheets   import SheetsExporter


# ============================================================
# SETUP LOGS
# ============================================================
logger.add("logs/run_{time}.log", rotation="50 MB", level="INFO")


# ============================================================
# HELPERS
# ============================================================
def generate_timestamps(start: str, end: str, hours: int) -> list[str]:
    fmt     = "%Y-%m-%dT%H:%M:%SZ"
    current = datetime.strptime(start, fmt)
    end_dt  = datetime.strptime(end, fmt)
    delta   = timedelta(hours=hours)
    ts      = []
    while current <= end_dt:
        ts.append(current.strftime(fmt))
        current += delta
    return ts


# ============================================================
# PIPELINE PRINCIPAL
# ============================================================
def run_fetch(push_to_sheets: bool = False):

    fetcher    = OddsFetcher()
    storage    = DataStorage(DB_PATH, OUTPUT_DIR)
    timestamps = generate_timestamps(DATE_START, DATE_END, INTERVAL_HOURS)

    total_rows = 0

    logger.info(f"Démarrage : {len(SPORTS)} sports x {len(timestamps)} timestamps")
    logger.info(f"Total appels estimés : {len(SPORTS) * len(timestamps)}")

    for sport in SPORTS:
        logger.info(f"\n=== {sport} ===")

        for date in timestamps:
            events, meta = fetcher.fetch_snapshot(sport, date)

            if events is None:
                continue

            # Sauvegarde JSON brut
            storage.save_raw(events, sport, date)

            # Parse → lignes plates
            rows = parse_events(events, sport, date)

            # Sauvegarde SQLite
            if rows:
                storage.save_rows(rows)
                total_rows += len(rows)

            time.sleep(0.3)   # pause légère

    fetcher.close()
    logger.success(f"Collecte terminée : {total_rows} lignes au total")

    # Export CSV local dans tous les cas
    df = storage.export_csv()

    # Export Google Sheets si demandé
    if push_to_sheets and not df.empty:
        exporter = SheetsExporter()
        exporter.export_odds(df)
        exporter.export_summary(df)


# ============================================================
# CLI
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="Odds fetcher - Football International")

    parser.add_argument(
        "--fetch",
        action="store_true",
        help="Lance la collecte des données"
    )
    parser.add_argument(
        "--sheets",
        action="store_true",
        help="Pousse les données vers Google Sheets"
    )
    parser.add_argument(
        "--export-only",
        action="store_true",
        help="Exporte uniquement depuis la DB locale (sans refetch)"
    )

    args = parser.parse_args()

    if args.fetch:
        run_fetch(push_to_sheets=args.sheets)

    elif args.export_only:
        storage  = DataStorage(DB_PATH, OUTPUT_DIR)
        df       = storage.export_csv()
        if args.sheets and not df.empty:
            exporter = SheetsExporter()
            exporter.export_odds(df)
            exporter.export_summary(df)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
