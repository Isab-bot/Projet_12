"""Chargement des publications transformées dans PostgreSQL."""
import logging
import os

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


if __name__ == "__main__":
    # Test provisoire de la connexion, remplacé plus tard par run()
    with get_connection() as connection:
        version = connection.execute("SELECT version()").fetchone()[0]
        logger.info("Version du serveur : %s", version)