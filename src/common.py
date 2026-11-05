"""Fonctions communes aux scripts d'extraction."""
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = DATA_DIR / "raw"
logger = logging.getLogger(__name__)


def clean_records(records):
    """Écarte les entrées sans titre ou sans image (non multimodales)."""
    kept = [r for r in records if r.get("title") and r.get("image_url")]
    logger.info(
        "%d entrées conservées, %d écartées (titre ou image manquant)",
        len(kept), len(records) - len(kept),
    )
    return kept


def save_jsonl(records, source_name):
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
    logger.info("%d entrées sauvegardées dans %s", len(records), path)
    return path