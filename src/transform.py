"""Pipeline de transformation des données brutes vers le schéma commun."""
import json
import logging
from pathlib import Path

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


if __name__ == "__main__":
    read_jsonl()