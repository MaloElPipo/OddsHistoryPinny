import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from loguru import logger
from config import CREDENTIALS_PATH, SPREADSHEET_ID, SHEET_ODDS, SHEET_SUMMARY


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Taille max par batch pour éviter les timeouts
BATCH_SIZE = 5000


class SheetsExporter:

    def __init__(self):
        creds  = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=SCOPES)
        self.client = gspread.authorize(creds)
        self.sheet  = self.client.open_by_key(SPREADSHEET_ID)

    # ----------------------------------------------------------
    def _get_or_create_tab(self, name: str) -> gspread.Worksheet:
        try:
            return self.sheet.worksheet(name)
        except gspread.WorksheetNotFound:
            return self.sheet.add_worksheet(title=name, rows=100000, cols=30)

    # ----------------------------------------------------------
    def export_odds(self, df: pd.DataFrame):
        """Envoie toutes les cotes dans l'onglet SHEET_ODDS."""
        ws = self._get_or_create_tab(SHEET_ODDS)
        ws.clear()

        if df.empty:
            logger.warning("DataFrame vide - rien à exporter")
            return

        # Header
        headers = df.columns.tolist()
        ws.append_row(headers, value_input_option="RAW")

        # Données par batch
        rows = df.fillna("").values.tolist()
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i:i + BATCH_SIZE]
            ws.append_rows(batch, value_input_option="USER_ENTERED")
            logger.info(f"Sheets : batch {i // BATCH_SIZE + 1} envoyé ({len(batch)} lignes)")

        logger.success(f"Export Sheets terminé : {len(rows)} lignes dans '{SHEET_ODDS}'")

    # ----------------------------------------------------------
    def export_summary(self, df: pd.DataFrame):
        """Génère un onglet récapitulatif par sport x bookmaker."""
        ws = self._get_or_create_tab(SHEET_SUMMARY)
        ws.clear()

        summary = (
            df.groupby(["sport", "bookmaker"])
            .agg(
                nb_events    =("event_id",     "nunique"),
                nb_snapshots =("snapshot_date","nunique"),
                first_date   =("commence_time","min"),
                last_date    =("commence_time","max"),
            )
            .reset_index()
        )

        headers = summary.columns.tolist()
        ws.append_row(headers, value_input_option="RAW")
        ws.append_rows(summary.fillna("").values.tolist(), value_input_option="USER_ENTERED")

        logger.success(f"Résumé exporté dans '{SHEET_SUMMARY}'")
