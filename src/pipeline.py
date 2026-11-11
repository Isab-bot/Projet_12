"""Exécute la chaîne complète hors Airflow : extraction → transformation → chargement → archivage."""
import logging
import time

import extract_decodeurs
import extract_fakeddit
import extract_newsdata
import load
import transform
from common import archive_raw_files

logger = logging.getLogger(__name__)

EXTRACT_STEPS = [
    ("extraction Les Décodeurs", extract_decodeurs.run),
    ("extraction Fakeddit", extract_fakeddit.run),
    ("extraction NewsData.io", extract_newsdata.run),
]


def run_step(name, step):
    """Exécute une étape en journalisant son début, sa fin et son résultat."""
    logger.info("=== Début : %s ===", name)
    result = step()
    logger.info("=== Fin : %s → %s ===", name, result)
    return result


def run():
    """Enchaîne les étapes ; s'arrête à la première qui lève une exception."""
    start = time.perf_counter()
    for name, step in EXTRACT_STEPS:
        run_step(name, step)
    transform_result = run_step("transformation", transform.run)
    run_step("chargement", load.run)
    run_step("archivage", lambda: archive_raw_files(transform_result["raw_files"]))
    logger.info("Chaîne terminée en %.1f s", time.perf_counter() - start)


if __name__ == "__main__":
    run()