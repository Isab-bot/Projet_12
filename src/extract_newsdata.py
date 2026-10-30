"""Extraction d'articles NewsData.io (texte + image) vers JSON Lines."""
import argparse
import logging
import os
import requests
import time


from dotenv import load_dotenv
from datetime import datetime, timezone

from common import clean_records, save_jsonl

# --- Configuration ---

MAX_PAGES = 2
PAUSE_SECONDS = 2
load_dotenv()
API_KEY = os.getenv("NEWSDATA_API_KEY")
BASE_URL = "https://newsdata.io/api/1/latest"
LANGUAGE = "fr"


FIELDS = [
    "article_id", "link", "title", "description", "image_url",
    "pubDate", "pubDateTZ", "language", "country", "category",
    "keywords", "creator", "source_id", "source_name",
    "source_url", "source_priority", "duplicate",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

def fetch_page(page_token=None, language=LANGUAGE):
    """Appelle l'API et renvoie la réponse JSON d'une page, ou None en cas d'échec."""
    headers = {"X-ACCESS-KEY": API_KEY}
    params = {"language": language}
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

def parse_articles(data):
    """Extrait les champs utiles de chaque article de la réponse."""
    collected_at = datetime.now(timezone.utc).isoformat()
    records = []
    for article in data.get("results", []):
        record = {field: article.get(field) for field in FIELDS}
        record["source"] = "newsdata"
        record["collected_at"] = collected_at
        records.append(record)
    logger.info("%d articles parsés", len(records))
    return records

def run(language=LANGUAGE, max_pages=MAX_PAGES):
    """Enchaîne connexion, parsing, nettoyage et sauvegarde sur plusieurs pages."""
    page_token = None
    total = 0
    for page_number in range(1, max_pages + 1):
        logger.info("Page %d", page_number)
        data = fetch_page(page_token, language)
        if data is None:
            raise RuntimeError(f"Échec de l'appel à NewsData.io (page {page_number})")
        records = clean_records(parse_articles(data))
        if records and not save_jsonl(records, "newsdata"):
            raise RuntimeError("Échec de la sauvegarde des articles NewsData.io")
        total += len(records)
            
        page_token = data.get("nextPage")
        if not page_token:
            logger.info("Plus de page disponible")
            break
        if page_number < max_pages:
            time.sleep(PAUSE_SECONDS)
    logger.info("Extraction terminée : %d articles sauvegardés", total)
    return total

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extraction d'articles NewsData.io")
    parser.add_argument("--language", default=LANGUAGE, help="Code langue (ex. fr, en)")
    parser.add_argument("--max-pages", type=int, default=MAX_PAGES, help="Nombre maximal de pages")
    args = parser.parse_args()
    run(language=args.language, max_pages=args.max_pages)