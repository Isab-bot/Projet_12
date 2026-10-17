"""Extraction d'articles NewsData.io (texte + image) vers JSON Lines."""
import argparse
import json
import logging
import os
import requests
import time
from pathlib import Path

from dotenv import load_dotenv
from datetime import datetime, timezone



# --- Configuration ---

MAX_PAGES = 2
PAUSE_SECONDS = 2
load_dotenv()
API_KEY = os.getenv("NEWSDATA_API_KEY")
BASE_URL = "https://newsdata.io/api/1/latest"
LANGUAGE = "fr"
OUTPUT_DIR = Path("data/raw")

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

def clean_records(records):
    """Écarte les articles sans titre ou sans image (non multimodaux)."""
    kept = [r for r in records if r.get("title") and r.get("image_url")]
    logger.info(
        "%d articles conservés, %d écartés (titre ou image manquant)",
        len(kept), len(records) - len(kept),
    )
    return kept

def save_jsonl(records, source_name="newsdata"):
    """Ajoute les enregistrements au fichier JSON Lines du jour."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    date = datetime.now(timezone.utc).strftime("%Y%m%d")
    path = OUTPUT_DIR / f"{source_name}_{date}.jsonl"
    try:
        with path.open("a", encoding="utf-8") as file:
            for record in records:
                file.write(json.dumps(record, ensure_ascii=False) + "\n")
    except OSError as error:
        logger.error("Échec de l'écriture dans %s : %s", path, error)
        return None
    logger.info("%d articles sauvegardés dans %s", len(records), path)
    return path

def run(language=LANGUAGE, max_pages=MAX_PAGES):
    """Enchaîne connexion, parsing, nettoyage et sauvegarde sur plusieurs pages."""
    page_token = None
    total = 0
    for page_number in range(1, max_pages + 1):
        logger.info("Page %d", page_number)
        data = fetch_page(page_token, language)
        if data is None:
            break
        records = clean_records(parse_articles(data))
        if records:
            save_jsonl(records)
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