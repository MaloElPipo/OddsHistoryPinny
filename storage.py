import json
import sqlite3
import pandas as pd
from pathlib import Path
from loguru import logger
from parser import COLUMNS


class DataStorage:

    def __init__(self, db_path: str, output_dir: str):
        self.db_path    = db_path
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._init_db()

    # ----------------------------------------------------------
    # SQLite
    # ----------------------------------------------------------
    def _init_db(self):
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS odds (
                    id               INTEGER PRIMARY KEY AUTOINCREMENT,
                    snapshot_date    TEXT,
                    sport            TEXT,
                    event_id         TEXT,
                    commence_time    TEXT,
                    home_team        TEXT,
                    away_team        TEXT,
                    bookmaker        TEXT,
                    bookmaker_title  TEXT,
                    last_update      TEXT,
                    odds_home        REAL,
                    odds_draw        REAL,
                    odds_away        REAL,
                    UNIQUE(snapshot_date, event_id, bookmaker)
                )
            """)
            conn.commit()
        logger.info(f"Base SQLite initialisée : {self.db_path}")

    def save_rows(self, rows: list[dict]):
        if not rows:
            return
        df = pd.DataFrame(rows, columns=COLUMNS)
        with sqlite3.connect(self.db_path) as conn:
            df.to_sql("odds", conn, if_exists="append", index=False,
                      method="multi")
        logger.info(f"{len(rows)} lignes sauvegardées en base")

    # ----------------------------------------------------------
    # JSON brut
    # ----------------------------------------------------------
    def save_raw(self, data: list, sport: str, date: str):
        sport_dir = self.output_dir / sport
        sport_dir.mkdir(parents=True, exist_ok=True)
        filename  = date.replace(":", "-").replace("Z", "") + ".json"
        with open(sport_dir / filename, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    # ----------------------------------------------------------
    # Export CSV complet depuis la base
    # ----------------------------------------------------------
    def export_csv(self, output_path: str = "data/odds_export.csv"):
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql("SELECT * FROM odds ORDER BY commence_time", conn)
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        logger.info(f"Export CSV : {output_path} ({len(df)} lignes)")
        return df

    # ----------------------------------------------------------
    # Lecture pour Google Sheets
    # ----------------------------------------------------------
    def read_all(self) -> pd.DataFrame:
        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql(
                "SELECT * FROM odds ORDER BY commence_time, sport, bookmaker",
                conn
            )
