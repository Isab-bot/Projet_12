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
    """Écrit les enregistrements dans un nouveau fichier JSON Lines, au nom unique.

    Un fichier écrit n'est plus jamais modifié : il peut être archivé sans risque
    une fois transformé et chargé.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%f")
    path = OUTPUT_DIR / f"{source_name}_{timestamp}.jsonl"
    try:
        with path.open("x", encoding="utf-8") as file:
            for record in records:
                file.write(json.dumps(record, ensure_ascii=False) + "\n")
    except OSError as error:
        logger.error("Échec de l'écriture dans %s : %s", path, error)
        return None
    logger.info("%d entrées sauvegardées dans %s", len(records), path)
    return path

def archive_raw_files(file_names, raw_dir=OUTPUT_DIR):
    """Déplace les fichiers bruts lus et chargés vers raw/archive/.

    Idempotente : un fichier déjà archivé lors d'un essai précédent est ignoré.
    Une archive existante n'est jamais écrasée.
    """
    archive_dir = raw_dir / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)
    moved = 0
    for name in file_names:
        source = raw_dir / name
        target = archive_dir / name
        if not source.exists() and target.exists():
            continue  # déjà archivé (relance après un échec à mi-chemin)
        if target.exists():
            raise FileExistsError(f"Archive déjà présente, déplacement refusé : {target}")
        source.rename(target)
        moved += 1
    logger.info("%d fichiers bruts archivés, %d déjà archivés", moved, len(file_names) - moved)
    return moved