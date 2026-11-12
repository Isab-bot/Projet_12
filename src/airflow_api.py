"""Accès en lecture à l'API REST d'Airflow 3 (jeton JWT, compte Viewer)."""
import os

import requests
from dotenv import load_dotenv

from common import PROJECT_ROOT
from urllib.parse import quote

load_dotenv(PROJECT_ROOT / ".env")

API_URL = os.getenv("AIRFLOW_API_URL", "http://localhost:8080")
TIMEOUT = 10


def get_token():
    """Échange l'identifiant et le mot de passe contre un jeton d'accès."""
    response = requests.post(
        f"{API_URL}/auth/token",
        json={
            "username": os.environ["AIRFLOW_API_USER"],
            "password": os.environ["AIRFLOW_API_PASSWORD"],
        },
        timeout=TIMEOUT,
    )
    response.raise_for_status()
    return response.json()["access_token"]


def api_get(path, token, params=None):
    """Envoie une requête GET à l'API v2 et renvoie la réponse JSON."""
    response = requests.get(
        f"{API_URL}/api/v2/{path}",
        headers={"Authorization": f"Bearer {token}"},
        params=params,
        timeout=TIMEOUT,
    )
    response.raise_for_status()
    return response.json()

def unwrap(value):
    """Déballe un tuple sérialisé par Airflow ; renvoie les autres valeurs telles quelles."""
    if isinstance(value, dict) and value.get("__classname__") == "builtins.tuple":
        return value["__data__"]
    return value


def get_runs(token, dag_id="checkit_daily", limit=30):
    """Renvoie les dernières exécutions d'un DAG, de la plus récente à la plus ancienne."""
    params = {"limit": limit, "order_by": "-start_date"}
    return api_get(f"dags/{dag_id}/dagRuns", token, params)["dag_runs"]


def get_task_instances(token, run_id, dag_id="checkit_daily"):
    """Renvoie les tâches d'une exécution (état, numéro d'essai, durée)."""
    path = f"dags/{dag_id}/dagRuns/{quote(run_id, safe='')}/taskInstances"
    return api_get(path, token)["task_instances"]


def get_return_value(token, run_id, task_id, dag_id="checkit_daily"):
    """Renvoie la valeur renvoyée par une tâche, ou None si elle n'existe pas."""
    path = (f"dags/{dag_id}/dagRuns/{quote(run_id, safe='')}"
            f"/taskInstances/{task_id}/xcomEntries/return_value")
    try:
        xcom = api_get(path, token, {"stringify": "false"})
    except requests.HTTPError as error:
        if error.response.status_code == 404:
            return None  # tâche en échec ou non exécutée : pas de valeur
        raise
    return unwrap(xcom["value"])

if __name__ == "__main__":
    token = get_token()
    for run in get_runs(token, limit=3):
        run_id = run["dag_run_id"]
        print(run_id, run["state"])
        for ti in get_task_instances(token, run_id):
            print(f"   {ti['task_id']:<18} essai {ti['try_number']}  {ti['duration']} s")
        print("   load            →", get_return_value(token, run_id, "load"))
        print("   extract_newsdata →", get_return_value(token, run_id, "extract_newsdata"))