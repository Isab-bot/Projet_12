"""Exécute la chaîne complète : extraction (3 sources) → transformation → chargement."""
import logging
import time

import extract_decodeurs
import extract_fakeddit
import extract_newsdata
import load
import transform

logger = logging.getLogger(__name__)

# Étapes dans l'ordre d'exécution : (nom pour les logs, fonction à appeler)
STEPS = [
    ("extraction Les Décodeurs", extract_decodeurs.run),
    ("extraction Fakeddit", extract_fakeddit.run),
    ("extraction NewsData.io", extract_newsdata.run),
    ("transformation", transform.run),
    ("chargement", load.run),
]


def run():
    """Enchaîne les étapes ; s'arrête à la première qui lève une exception."""
    start = time.perf_counter()
    for name, step in STEPS:
        logger.info("=== Début : %s ===", name)
        result = step()
        logger.info("=== Fin : %s → %s ===", name, result)
    logger.info("Chaîne terminée en %.1f s", time.perf_counter() - start)


if __name__ == "__main__":
    run()