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

# Codes ISO 639-1 (NewsData.io fournit le nom complet de la langue)
LANGUAGE_CODES = {"french": "fr", "english": "en"}

# Correspondance de 6_way_label vérifiée par croisement avec subreddit (cf. Table 7 de l'article)
FAKEDDIT_LABELS = {
    0: "true",
    1: "satire",
    2: "false_connection",
    3: "imposter_content",
    4: "manipulated_content",
    5: "misleading_content",
}

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
def to_int(value):
    """Convertit en entier ; renvoie None si absent ou illisible."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def transform_fakeddit(record):
    """Transforme un enregistrement Fakeddit brut vers le schéma commun."""
    original_id = record.get("id")
    return {
        # Champs communs
        "id": f"fakeddit:{original_id}" if original_id else None,
        "original_id": original_id,
        "source": "fakeddit",
        "url": None,  # absent du TSV
        "title": clean_text(record.get("title")),
        "description": None,
        "image_url": record.get("image_url"),
        "published_at": parse_date(record.get("created_utc"), "fakeddit"),
        "collected_at": record.get("collected_at"),
        "language": "en",
        "label": FAKEDDIT_LABELS.get(record.get("6_way_label")),
        "label_source": "distant_supervision",
        "label_scope": "post",
        # Champs secondaires
        "subreddit": record.get("subreddit"),  # jamais en entrée du modèle (fuite de données)
        "domain": record.get("domain"),
        "author": record.get("author"),
        "score": to_int(record.get("score")),
        "num_comments": to_int(record.get("num_comments")),
        "upvote_ratio": record.get("upvote_ratio"),
    }

if __name__ == "__main__":
    records = read_jsonl()
    fakeddit = [r for r in records if r.get("source") == "fakeddit"]
    print(transform_fakeddit(fakeddit[0]))
   
    #read_jsonl()