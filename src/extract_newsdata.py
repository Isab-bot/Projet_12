"""Extraction d'articles NewsData.io (texte + image) vers JSON Lines."""
import logging
import os
import requests
from pathlib import Path

from dotenv import load_dotenv

# --- Configuration ---
load_dotenv()
API_KEY = os.getenv("NEWSDATA_API_KEY")
BASE_URL = "https://newsdata.io/api/1/latest"
LANGUAGE = "fr"
MAX_PAGES = 1
OUTPUT_DIR = Path("data/raw")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

def fetch_page(page_token=None):
    """Appelle l'API et renvoie la réponse JSON d'une page, ou None en cas d'échec."""
    headers = {"X-ACCESS-KEY": API_KEY}
    params = {"language": LANGUAGE}
    if page_token:
        params["page"] = page_token
    try:
        response = requests.get(BASE_URL, headers=headers, params=params, timeout=30)
        response.raise_for_status()
    except requests.RequestException as error:
        logger.error("Échec de l'appel à l'API : %s", error)
        return None
    data = response.json()
    if data.get("status") != "success":
        logger.error("Réponse en erreur : %s", data.get("results"))
        return None
    logger.info("%d articles reçus", len(data.get("results", [])))
    return data

if __name__ == "__main__":
    data = fetch_page()
    if data:
        first = data["results"][0]
        logger.info("Champs du premier article : %s", list(first.keys()))