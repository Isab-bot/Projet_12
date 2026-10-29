"""Extraction d'articles du flux RSS Les Décodeurs (texte + image) vers JSON Lines."""
import argparse
import logging
from datetime import datetime, timezone

import feedparser
import requests

from common import clean_records, save_jsonl

# --- Configuration ---
FEED_URL = "https://www.lemonde.fr/les-decodeurs/rss_full.xml"
FIELDS = ["id", "link", "title", "summary", "published", "updated"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def fetch_feed(url=FEED_URL):
    """Télécharge le flux RSS et renvoie ses entrées, ou None en cas d'échec."""
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as error:
        logger.error("Échec du téléchargement du flux : %s", error)
        return None
    feed = feedparser.parse(response.content)
    if feed.bozo:
        logger.warning("Flux mal formé : %s", feed.bozo_exception)
    logger.info("%d entrées reçues", len(feed.entries))
    return feed.entries


def parse_entries(entries):
    """Extrait les champs utiles de chaque entrée du flux."""
    collected_at = datetime.now(timezone.utc).isoformat()
    records = []
    for entry in entries:
        record = {field: entry.get(field) for field in FIELDS}
        media = (entry.get("media_content") or [{}])[0]
        record["image_url"] = media.get("url")
        record["image_width"] = media.get("width")
        record["image_height"] = media.get("height")
        # Légende de l'image (nom d'origine conservé, renommage à l'étape 3)
        record["content"] = (entry.get("content") or [{}])[0].get("value")
        record["media_credit"] = (entry.get("media_credit") or [{}])[0].get("content")
        record["source"] = "decodeurs"
        record["collected_at"] = collected_at
        records.append(record)
    logger.info("%d entrées parsées", len(records))
    return records


def run(url=FEED_URL):
    """Enchaîne connexion, parsing, nettoyage et sauvegarde."""
    entries = fetch_feed(url)
    if entries is None:
        return 0
    records = clean_records(parse_entries(entries))
    saved = len(records) if records and save_jsonl(records, "decodeurs") else 0
    logger.info("Extraction terminée : %d entrées sauvegardées", saved)
    return saved


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extraction du flux RSS Les Décodeurs")
    parser.add_argument("--url", default=FEED_URL, help="Adresse du flux RSS")
    args = parser.parse_args()
    run(url=args.url)