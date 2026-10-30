"""Teste la règle de mise à jour dans une transaction annulée : la base n'est pas modifiée."""
import sys

sys.path.insert(0, "src")
from load import get_connection, load_rows, read_publications, to_row

record = read_publications()[0]
older = {**record, "title": "TITRE ANCIEN (test)", "collected_at": "2000-01-01T00:00:00+00:00"}
newer = {**record, "title": "TITRE RÉCENT (test)", "collected_at": "2099-01-01T00:00:00+00:00"}
query = "SELECT title FROM publications WHERE id = %s"

with get_connection() as connection:
    with connection.transaction(force_rollback=True):
        print("Version plus ancienne :", load_rows(connection, [to_row(older)]))
        print("Version plus récente  :", load_rows(connection, [to_row(newer)]))
        print("Titre pendant le test :", connection.execute(query, (record["id"],)).fetchone()[0])
    print("Titre après annulation :", connection.execute(query, (record["id"],)).fetchone()[0])