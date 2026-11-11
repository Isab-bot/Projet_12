"""Pipeline de transformation des données brutes vers le schéma commun."""
import json
import logging
from pathlib import Path
import hmac
import html
import re
import unicodedata
import os
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from collections import Counter
import hashlib
from io import BytesIO
from dotenv import load_dotenv

import requests
from PIL import Image


from common import DATA_DIR, PROJECT_ROOT

# --- Configuration ---


RAW_DIR = DATA_DIR / "raw"
IMAGES_DIR = DATA_DIR / "images"
PROCESSED_PATH = DATA_DIR / "processed" / "publications.jsonl"

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


TIMEOUT = 10  # secondes
IMAGE_EXTENSIONS = {"JPEG": "jpg"}  # sinon : format en minuscules (png, webp, gif)

# Identification du client auprès des serveurs (évite le User-Agent par défaut, fortement limité)
HEADERS = {"User-Agent": "CheckIt-AI-pipeline/0.1 (projet de formation OpenClassrooms)"}

MIN_IMAGE_SIDE = 64  # px : sous ce seuil, icônes (favicons) ou images inexploitables



logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

load_dotenv()
PSEUDONYMIZATION_KEY = os.getenv("PSEUDONYMIZATION_KEY")

def read_jsonl(raw_dir=RAW_DIR):
    """Lit tous les fichiers JSON Lines bruts ; renvoie les enregistrements et les noms des fichiers lus."""
    records = []
    files = []
    invalid = 0
    for path in sorted(raw_dir.glob("*.jsonl")):
        files.append(path.name)
        with path.open(encoding="utf-8") as file:
            for line_number, line in enumerate(file, start=1):
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    invalid += 1
                    logger.warning("Ligne illisible ignorée : %s, ligne %d", path.name, line_number)
    logger.info("%d enregistrements lus dans %d fichiers, %d lignes illisibles", len(records), len(files), invalid)
    return records, files

def clean_text(text):
    """Nettoie un texte sans en changer le sens ; renvoie None s'il est vide."""
    if not isinstance(text, str):
        return None
    text = html.unescape(text)                    # &amp; -> &
    text = re.sub(r"<[^>]+>", " ", text)          # supprime les balises HTML
    text = unicodedata.normalize("NFC", text)     # forme Unicode unique
    text = re.sub(r"\s+", " ", text).strip()      # blancs multiples -> un espace
    return text or None

def parse_date(value, source, tz=None):
    """Convertit une date brute en chaîne ISO 8601 UTC ; renvoie None si illisible."""
    try:
        if source == "newsdata":
            # pubDate est sans fuseau : on contrôle pubDateTZ au lieu de supposer l'UTC
            if tz != "UTC":
                logger.warning("Fuseau NewsData.io non géré : %s", tz)
                return None
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

def pseudonymize(value):
    """Remplace une valeur personnelle par son empreinte HMAC-SHA-256 ; None reste None."""
    if value is None:
        return None
    if not PSEUDONYMIZATION_KEY:
        raise RuntimeError("PSEUDONYMIZATION_KEY absente de .env")
    return hmac.new(PSEUDONYMIZATION_KEY.encode(), value.encode(), hashlib.sha256).hexdigest()

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
        "author_hash": pseudonymize(record.get("author")),
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
        "published_at": parse_date(record.get("pubDate"), "newsdata", record.get("pubDateTZ")),
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


def deduplicate(records):
    """Garde, pour chaque id, l'enregistrement collecté le plus récemment."""
    latest = {}
    duplicates = Counter()
    for record in records:
        current = latest.get(record["id"])
        if current is not None:
            duplicates[record["source"]] += 1
        if current is None or record["collected_at"] > current["collected_at"]:
            latest[record["id"]] = record
    logger.info(
        "%d enregistrements uniques, %d doublons écartés",
        len(latest), sum(duplicates.values()),
    )
    for source, count in duplicates.items():
        logger.info("  doublons – %s : %d", source, count)
    return list(latest.values())

def image_filename(record_id):
    """Renvoie un nom de fichier sûr et déterministe dérivé de l'id (sans extension)."""
    return hashlib.sha256(record_id.encode("utf-8")).hexdigest()

def validate_image(record, session):
    """Vérifie que l'image est accessible et lisible, et complète l'enregistrement.

    Renvoie (enregistrement complété, None) si l'image est valide, (None, motif) sinon.
    """
    name = image_filename(record["id"])
    existing = next(IMAGES_DIR.glob(f"{name}.*"), None)

    # 1. Accessibilité (ou relecture si déjà téléchargée)
    try:
        if existing:
            content = existing.read_bytes()
        else:
            response = session.get(record["image_url"], timeout=TIMEOUT)
            response.raise_for_status()
            content = response.content
    except requests.HTTPError as error:
        return None, f"erreur HTTP {error.response.status_code}"
    except requests.RequestException as error:
        return None, f"image inaccessible ({type(error).__name__})"

    # 2. Nature du fichier et 3. dimensions
    try:
        with Image.open(BytesIO(content)) as image:
            image.load()  # décode toute l'image : détecte aussi les fichiers tronqués
            width, height = image.size
            extension = IMAGE_EXTENSIONS.get(image.format, image.format.lower())
    except (OSError, Image.DecompressionBombError):
        return None, "image illisible"

    if min(width, height) < MIN_IMAGE_SIDE:
        return None, "image trop petite"

    # Enregistrement local
    path = existing or IMAGES_DIR / f"{name}.{extension}"
    if not existing:
        try:
            path.write_bytes(content)
        except OSError as error:
            logger.warning("Écriture impossible : %s (%s)", path, error)
            return None, "écriture impossible"

    return {**record, "image_path": path.relative_to(PROJECT_ROOT).as_posix(), "image_width": width, "image_height": height}, None


def validate_images(records):
    """Ne garde que les enregistrements dont l'image est valide."""
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    valid = []
    rejected = Counter()
    with requests.Session() as session:
        session.headers.update(HEADERS)
        for count, record in enumerate(records, start=1):
            if count % 200 == 0:
                logger.info("  images traitées : %d / %d", count, len(records))
            checked, reason = validate_image(record, session)
            if reason:
                rejected[(record["source"], reason)] += 1
            else:
                valid.append(checked)
    logger.info("%d images valides, %d rejetées", len(valid), sum(rejected.values()))
    for (source, reason), nb in sorted(rejected.items()):
        logger.info("  rejet – %s – %s : %d", source, reason, nb)

    if valid:
        smallest = min(valid, key=lambda r: r["image_width"] * r["image_height"])
        logger.info(
            "  plus petite image : %d x %d px (%s)",
            smallest["image_width"], smallest["image_height"], smallest["source"],
        )
    return valid

def export_jsonl(records, path=PROCESSED_PATH):
    """Écrit les enregistrements propres en JSON Lines ; renvoie le chemin, ou None en cas d'échec.

    L'écriture se fait dans un fichier temporaire, renommé en fichier final une fois complet :
    une interruption ne laisse jamais un fichier final partiellement écrit.
    """
    temp_path = path.with_suffix(".jsonl.tmp")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with temp_path.open("w", encoding="utf-8") as file:
            for record in records:
                file.write(json.dumps(record, ensure_ascii=False) + "\n")
        temp_path.replace(path)  # renommage atomique
    except OSError as error:
        logger.error("Écriture impossible : %s (%s)", path, error)
        temp_path.unlink(missing_ok=True)  # supprime le fichier temporaire s'il existe
        return None
    logger.info("%d enregistrements exportés : %s", len(records), path)
    return path

def run(raw_dir=RAW_DIR):
    """Transforme les fichiers bruts présents ; renvoie le nombre exporté et les fichiers lus."""
    records, files = read_jsonl(raw_dir)
    if not records:
        logger.info("Aucun nouvel enregistrement brut dans %s", raw_dir)
        if not export_jsonl([]):
            raise RuntimeError("Échec de l'export de publications.jsonl")
        return {"exported": 0, "raw_files": files}
    records = deduplicate(check_required(transform_records(records)))
    records = validate_images(records)
    if not records:
        raise RuntimeError("Aucun enregistrement valide à exporter")
    if not export_jsonl(records):
        raise RuntimeError("Échec de l'export de publications.jsonl")
    return {"exported": len(records), "raw_files": files}

if __name__ == "__main__":
    run()
  