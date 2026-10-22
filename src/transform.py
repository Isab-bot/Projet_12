"""Pipeline de transformation des données brutes vers le schéma commun."""
import json
import logging
from pathlib import Path
import html
import re
import unicodedata
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

# --- Configuration ---
RAW_DIR = Path("data/raw")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def read_jsonl(raw_dir=RAW_DIR):
    """Lit tous les fichiers JSON Lines bruts et renvoie la liste des enregistrements."""
    records = []
    invalid = 0
    for path in sorted(raw_dir.glob("*.jsonl")):
        with path.open(encoding="utf-8") as file:
            for line_number, line in enumerate(file, start=1):
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    invalid += 1
                    logger.warning("Ligne illisible ignorée : %s, ligne %d", path.name, line_number)
    logger.info("%d enregistrements lus, %d lignes illisibles", len(records), invalid)
    return records

def clean_text(text):
    """Nettoie un texte sans en changer le sens ; renvoie None s'il est vide."""
    if not isinstance(text, str):
        return None
    text = html.unescape(text)                    # &amp; -> &
    text = re.sub(r"<[^>]+>", " ", text)          # supprime les balises HTML
    text = unicodedata.normalize("NFC", text)     # forme Unicode unique
    text = re.sub(r"\s+", " ", text).strip()      # blancs multiples -> un espace
    return text or None

def parse_date(value, source):
    """Convertit une date brute en chaîne ISO 8601 UTC ; renvoie None si illisible."""
    try:
        if source == "newsdata":
            # pubDate sans fuseau ; pubDateTZ vaut "UTC" dans les données observées
            date = datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        elif source == "decodeurs":
            # format des flux RSS : "Fri, 16 Oct 2026 17:59:32 +0200"
            date = parsedate_to_datetime(value)
        elif source == "fakeddit":
            # timestamp Unix : secondes depuis le 1er janvier 1970 UTC
            date = datetime.fromtimestamp(float(value), tz=timezone.utc)
        else:
            return None
        return date.astimezone(timezone.utc).isoformat()
    except (TypeError, ValueError):
        return None

if __name__ == "__main__":
    print(parse_date("2026-10-16 21:38:51", "newsdata"))
    print(parse_date("Fri, 16 Oct 2026 17:59:32 +0200", "decodeurs"))
    print(parse_date(1551398400.0, "fakeddit"))
    print(parse_date("pas une date", "decodeurs"))
    read_jsonl()