"""DAG quotidien : Les Décodeurs + NewsData.io → transformation → chargement.

Les tâches appellent les fonctions run() existantes, sans les recoder.
Chaque run() lève une exception en cas d'échec : Airflow marque alors la tâche en échec.
"""
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task

# Pool d'une place : une seule transformation ou un seul chargement à la fois, tous DAG confondus
POOL = "checkit_publications"

# Extractions : erreurs surtout transitoires (réseau, API) → relances
EXTRACT_RETRIES = {"retries": 3, "retry_delay": timedelta(minutes=5)}


@dag(
    dag_id="checkit_daily",
    description="Extraction quotidienne des flux d'actualité, transformation et chargement",
    schedule="0 6 * * *",  # chaque jour à 6 h UTC
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,         # les sources renvoient l'état actuel : rattraper n'apporterait rien
    max_active_runs=1,     # jamais deux exécutions de ce DAG en même temps
    tags=["checkit"],
)
def checkit_daily():

    @task(task_id="extract_decodeurs", **EXTRACT_RETRIES)
    def run_extract_decodeurs():
        import extract_decodeurs
        return extract_decodeurs.run()

    @task(task_id="extract_newsdata", **EXTRACT_RETRIES)
    def run_extract_newsdata():
        import extract_newsdata
        return extract_newsdata.run()

    # Attend la fin de toutes les extractions ; s'exécute si au moins une a réussi
    @task(task_id="transform", trigger_rule="all_done_min_one_success", pool=POOL, retries=0)
    def run_transform():
        import transform
        return transform.run()

    # Une relance : seule cause transitoire possible, la connexion à la base
    @task(task_id="load", pool=POOL, retries=1, retry_delay=timedelta(minutes=1))
    def run_load():
        import load
        return load.run()

    # Règle par défaut (all_success) : en échec si une extraction a échoué.
    # Tâche finale : l'exécution entière apparaît alors en rouge, même si le chargement a réussi.
    @task(task_id="check_extractions")
    def check_extractions():
        """Rend visible l'échec d'une extraction au niveau de l'exécution du DAG."""

    extractions = [run_extract_decodeurs(), run_extract_newsdata()]
    extractions >> run_transform() >> run_load()
    extractions >> check_extractions()


checkit_daily()