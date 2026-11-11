"""DAG manuel, à lancer une seule fois : échantillon Fakeddit → transformation → chargement.

Fakeddit est un jeu de données statique : le réextraire chaque jour produirait les mêmes
entrées et réécrirait les mêmes lignes en base. Ce DAG sert au chargement initial,
par exemple sur une machine neuve.
"""
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task

POOL = "checkit_publications"


@dag(
    dag_id="checkit_fakeddit_init",
    description="Chargement initial de l'échantillon Fakeddit (déclenchement manuel)",
    schedule=None,         # aucun déclenchement automatique
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["checkit"],
)
def checkit_fakeddit_init():

    # Lecture d'un fichier local : erreur déterministe, pas de relance
    @task(task_id="extract_fakeddit", retries=0)
    def run_extract_fakeddit():
        import extract_fakeddit
        return extract_fakeddit.run()

    @task(task_id="transform", pool=POOL, retries=0)
    def run_transform():
        import transform
        return transform.run()

    @task(task_id="load", pool=POOL, retries=1, retry_delay=timedelta(minutes=1))
    def run_load():
        import load
        return load.run()

    run_extract_fakeddit() >> run_transform() >> run_load()


checkit_fakeddit_init()