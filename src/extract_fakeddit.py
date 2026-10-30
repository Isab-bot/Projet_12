"""Extraction d'un échantillon de Fakeddit (texte + image) vers JSON Lines."""
import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from common import clean_records, save_jsonl

# --- Configuration ---
INPUT_FILE = Path("data/fakeddit/multimodal_train.tsv")
SAMPLE_SIZE = 2000
SEED = 42
FIELDS = [
    "id", "title", "clean_title", "image_url", "created_utc", "domain",
    "subreddit", "author", "score", "num_comments", "upvote_ratio",
    "2_way_label", "3_way_label", "6_way_label",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def load_dataset(path=INPUT_FILE):
    """Lit le fichier TSV et renvoie un DataFrame, ou None en cas d'échec."""
    try:
        df = pd.read_csv(path, sep="\t")
    except FileNotFoundError:
        logger.error("Fichier introuvable : %s (à télécharger depuis le dépôt Fakeddit)", path)
        return None
    except pd.errors.ParserError as error:
        logger.error("Fichier illisible : %s", error)
        return None
    logger.info("%d lignes lues", len(df))
    return df


def sample_dataset(df, sample_size=SAMPLE_SIZE, seed=SEED):
    """Tire un échantillon stratifié qui conserve la répartition de 6_way_label."""
    frac = min(sample_size / len(df), 1)
    sample = df.groupby("6_way_label").sample(frac=frac, random_state=seed)
    logger.info("%d lignes échantillonnées (graine %d)", len(sample), seed)
    logger.info(
        "Répartition 2_way_label de l'échantillon :\n%s",
        sample["2_way_label"].value_counts(normalize=True),
    )
    return sample


def parse_rows(sample):
    """Convertit les lignes utiles en dictionnaires, avec None pour les valeurs manquantes."""
    collected_at = datetime.now(timezone.utc).isoformat()
    subset = sample[FIELDS].astype(object)
    subset = subset.where(subset.notna(), None)
    records = subset.to_dict(orient="records")
    for record in records:
        record["source"] = "fakeddit"
        record["collected_at"] = collected_at
    logger.info("%d lignes parsées", len(records))
    return records


def run(path=INPUT_FILE, sample_size=SAMPLE_SIZE, seed=SEED):
    """Enchaîne lecture, échantillonnage, parsing, nettoyage et sauvegarde ; lève une exception en cas d'échec."""
    df = load_dataset(path)
    if df is None:
        raise RuntimeError(f"Lecture impossible du fichier Fakeddit : {path}")
    records = clean_records(parse_rows(sample_dataset(df, sample_size, seed)))
    if records and not save_jsonl(records, "fakeddit"):
        raise RuntimeError("Échec de la sauvegarde des entrées Fakeddit")
    logger.info("Extraction terminée : %d entrées sauvegardées", len(records))
    return len(records)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extraction d'un échantillon de Fakeddit")
    parser.add_argument("--path", default=INPUT_FILE, type=Path, help="Chemin du fichier TSV")
    parser.add_argument("--sample-size", type=int, default=SAMPLE_SIZE, help="Nombre de lignes à tirer")
    parser.add_argument("--seed", type=int, default=SEED, help="Graine aléatoire")
    args = parser.parse_args()
    run(path=args.path, sample_size=args.sample_size, seed=args.seed)