"""Vérifie que chaque image_path de la base correspond à un fichier existant."""
import sys
from pathlib import Path

sys.path.insert(0, "src")  # pour importer load.py depuis la racine du projet
from load import get_connection

with get_connection() as connection:
    paths = [row[0] for row in connection.execute("SELECT image_path FROM publications")]

missing = [path for path in paths if not Path(path).is_file()]
print(f"{len(paths)} chemins vérifiés, {len(missing)} fichiers manquants")
for path in missing[:10]:
    print("  manquant :", path)