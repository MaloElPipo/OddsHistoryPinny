import time
import requests
from loguru import logger
from config import (
    API_KEY, BASE_URL,
    REGIONS, MARKETS, ODDS_FORMAT, DATE_FORMAT
)


class OddsFetcher:

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    # ----------------------------------------------------------
    def fetch_snapshot(self, sport: str, date: str) -> tuple[list, dict] | tuple[None, None]:
        """
        Récupère un snapshot de cotes pour un sport et une date.
        Retourne (events, meta) ou (None, None).
        """
        url = f"{BASE_URL}/{sport}/odds"

        try:
            response = self.session.get(url, params={
                "api_key":    API_KEY,
                "regions":    REGIONS,
                "markets":    MARKETS,
                "oddsFormat": ODDS_FORMAT,
                "dateFormat": DATE_FORMAT,
                "date":       date,
            }, timeout=30)

        except requests.exceptions.RequestException as e:
            logger.error(f"Erreur réseau [{sport}] [{date}] : {e}")
            return None, None

        # --- Gestion des status codes ---
        if response.status_code == 200:
            data = response.json()
            meta = {
                "timestamp":          data.get("timestamp"),
                "previous_timestamp": data.get("previous_timestamp"),
                "next_timestamp":     data.get("next_timestamp"),
                "credits_used":       response.headers.get("x-requests-used", "?"),
                "credits_remaining":  response.headers.get("x-requests-remaining", "?"),
            }
            logger.info(
                f"OK [{sport}] [{date}] "
                f"| {len(data.get('data', []))} events "
                f"| crédits restants: {meta['credits_remaining']}"
            )
            return data.get("data", []), meta

        elif response.status_code == 422:
            logger.warning(f"SKIP [{sport}] [{date}] : pas de données")
            return [], None

        elif response.status_code == 401:
            logger.error("Clé API invalide - arrêt")
            raise SystemExit("API key invalide")

        elif response.status_code == 429:
            logger.warning("Rate limit - pause 60s")
            time.sleep(60)
            return self.fetch_snapshot(sport, date)    # retry

        else:
            logger.error(f"HTTP {response.status_code} [{sport}] [{date}] : {response.text}")
            return None, None

    # ----------------------------------------------------------
    def close(self):
        self.session.close()
