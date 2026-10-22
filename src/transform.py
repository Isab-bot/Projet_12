"""Pipeline de transformation des données brutes vers le schéma commun."""
import json
import logging
from pathlib import Path
import html
import re
import unicodedata
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from collections import Counter

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

# Champs toujours renseignés dans le schéma commun
REQUIRED_FIELDS = ["id", "source", "title", "image_url", "published_at", "collected_at", "language"]

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

def transform_newsdata(record):
    """Transforme un enregistrement NewsData.io brut vers le schéma commun."""
    original_id = record.get("article_id")
    return {
        # Champs communs
        "id": f"newsdata:{original_id}" if original_id else None,
        "original_id": original_id,
        "source": "newsdata",
        "url": record.get("link"),
        "title": clean_text(record.get("title")),
        "description": clean_text(record.get("description")),
        "image_url": record.get("image_url"),
        "published_at": parse_date(record.get("pubDate"), "newsdata"),
        "collected_at": record.get("collected_at"),
        "language": LANGUAGE_CODES.get(record.get("language")),
        "label": None,
        "label_source": None,
        "label_scope": None,
        # Champs secondaires
        "source_name": record.get("source_name"),
        "source_url": record.get("source_url"),
        "source_priority": record.get("source_priority"),  # métadonnée, jamais un label
        "country": record.get("country"),
        "category": record.get("category"),
    }
def transform_decodeurs(record):
    """Transforme un enregistrement Les Décodeurs brut vers le schéma commun."""
    original_id = record.get("id")
    return {
        # Champs communs
        "id": f"decodeurs:{original_id}" if original_id else None,
        "original_id": original_id,
        "source": "decodeurs",
        "url": record.get("link"),
        "title": clean_text(record.get("title")),
        "description": clean_text(record.get("summary")),
        "image_url": record.get("image_url"),
        "published_at": parse_date(record.get("published"), "decodeurs"),
        "collected_at": record.get("collected_at"),
        "language": "fr",
        "label": None,
        "label_source": None,
        "label_scope": None,
        # Champs secondaires
        "image_caption": clean_text(record.get("content")),  # légende de la photo
        "image_credit": clean_text(record.get("media_credit")),
    }

# Fonction de transformation de chaque source
TRANSFORMERS = {
    "newsdata": transform_newsdata,
    "decodeurs": transform_decodeurs,
    "fakeddit": transform_fakeddit,
}


def transform_records(records):
    """Applique à chaque enregistrement la transformation de sa source."""
    transformed = []
    unknown = 0
    for record in records:
        transformer = TRANSFORMERS.get(record.get("source"))
        if transformer is None:
            unknown += 1
            continue
        transformed.append(transformer(record))
    logger.info("%d enregistrements transformés, %d de source inconnue", len(transformed), unknown)
    return transformed

def check_required(records):
    """Écarte les enregistrements auxquels il manque un champ obligatoire."""
    valid = []
    rejected = Counter()
    for record in records:
        missing = next((field for field in REQUIRED_FIELDS if not record.get(field)), None)
        if missing:
            rejected[missing] += 1
            continue
        valid.append(record)
    logger.info("%d enregistrements complets, %d rejetés", len(valid), sum(rejected.values()))
    for field, count in rejected.items():
        logger.info("  rejet – champ manquant %s : %d", field, count)
    return valid

if __name__ == "__main__":
    records = read_jsonl()
    records = check_required(transform_records(records))
   
    