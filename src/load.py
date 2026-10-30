"""Chargement des publications transformées dans PostgreSQL."""
import json
import logging
import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv

# --- Configuration ---
load_dotenv()
DB_PARAMS = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

PROCESSED_PATH = Path("data/processed/publications.jsonl")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def get_connection():
    """Ouvre une connexion à PostgreSQL ; lève une exception en cas d'échec."""
    try:
        connection = psycopg.connect(**DB_PARAMS, connect_timeout=10)
    except psycopg.OperationalError as error:
        logger.error("Connexion impossible à PostgreSQL : %s", error)
        raise
    logger.info("Connecté à %s:%s/%s", DB_PARAMS["host"], DB_PARAMS["port"], DB_PARAMS["dbname"])
    return connection

def read_publications(path=PROCESSED_PATH):
    """Lit le JSONL produit par transform.py ; lève une exception si le fichier est absent ou une ligne illisible."""
    records = []
    try:
        with path.open(encoding="utf-8") as file:
            for line_number, line in enumerate(file, start=1):
                records.append(json.loads(line))
    except FileNotFoundError:
        logger.error("Fichier introuvable : %s", path)
        raise
    except json.JSONDecodeError as error:
        logger.error("Ligne %d illisible dans %s : %s", line_number, path, error)
        raise
    logger.info("%d publications lues : %s", len(records), path)
    return records

if __name__ == "__main__":
    # Test provisoire, remplacé plus tard par run()
    records = read_publications()