"""Tableau de bord KPI de l'ETL CheckIt.AI (Streamlit).

Données → PostgreSQL (rôle checkit_reader) ; exécutions → API REST d'Airflow (compte Viewer).
"""
import os
from collections import Counter

import pandas as pd
import psycopg
import requests
import streamlit as st
import altair as alt
from dotenv import load_dotenv

import airflow_api
from common import PROJECT_ROOT

load_dotenv(PROJECT_ROOT / ".env")
KPI_SQL = (PROJECT_ROOT / "queries" / "kpi_donnees.sql").read_text(encoding="utf-8")
TASKS = ["extract_decodeurs", "extract_newsdata", "transform", "load", "archive"]
CREDIT_LIMIT = 200
STATES = {"success": "réussie", "failed": "échouée", "running": "en cours", "queued": "en attente"}


def hours_since(timestamp):
    """Nombre d'heures écoulées depuis une date (UTC)."""
    return (pd.Timestamp.now(tz="UTC") - pd.Timestamp(timestamp)).total_seconds() / 3600


@st.cache_data(ttl=300)
def load_data_kpis():
    """KPI de données (B1, C1, C3, C4), une ligne par source, lus avec le rôle de lecture."""
    with psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
        port=os.getenv("POSTGRES_PORT", "5433"),
        dbname=os.environ["POSTGRES_DB"],
        user="checkit_reader",
        password=os.environ["POSTGRES_READER_PASSWORD"],
    ) as conn, conn.cursor() as cursor:
        cursor.execute(KPI_SQL)
        columns = [column.name for column in cursor.description]
        return pd.DataFrame(cursor.fetchall(), columns=columns)


@st.cache_data(ttl=300)
def load_runs(limit=30):
    """Dernières exécutions de checkit_daily : durées, essais, tâches en échec et valeurs renvoyées."""
    token = airflow_api.get_token()
    rows = []
    for run in airflow_api.get_runs(token, limit=limit):
        run_id = run["dag_run_id"]
        tasks = {ti["task_id"]: ti for ti in airflow_api.get_task_instances(token, run_id)}
        row = {
            "type": "planifiée" if run["run_type"] == "scheduled" else "manuelle",
            "état": STATES.get(run["state"], run["state"]),
            "début": run["start_date"],
            "fin": run["end_date"],
            "tâches en échec": ", ".join(
                t for t, ti in tasks.items() if ti["state"] in ("failed", "upstream_failed")),
            "tâches relancées": ", ".join(
                t for t, ti in tasks.items() if (ti["try_number"] or 1) > 1),
        }
        for task in TASKS:
            row[f"durée {task} (s)"] = tasks.get(task, {}).get("duration")
            row[f"essais {task}"] = tasks.get(task, {}).get("try_number")
        counts = airflow_api.get_return_value(token, run_id, "load")
        row["insérées"], row["mises à jour"], row["inchangées"] = counts or (None, None, None)
        newsdata = airflow_api.get_return_value(token, run_id, "extract_newsdata")
        # Borne basse : requêtes de l'essai réussi + 1 par essai raté ; ancien format → non disponible
        row["crédits NewsData.io"] = (
            newsdata["requests"] + (row["essais extract_newsdata"] or 1) - 1
            if isinstance(newsdata, dict) else None
        )
        transform = airflow_api.get_return_value(token, run_id, "transform")
        row["métriques"] = transform.get("metrics") if isinstance(transform, dict) else None
        rows.append(row)
    runs = pd.DataFrame(rows)
    runs["début"] = pd.to_datetime(runs["début"], utc=True)
    runs["fin"] = pd.to_datetime(runs["fin"], utc=True)
    runs["durée totale (min)"] = (runs["fin"] - runs["début"]).dt.total_seconds() / 60
    runs["exécution du"] = runs["début"].dt.strftime("%Y-%m-%d %H:%M")
    return runs.sort_values("début").reset_index(drop=True)


def rejection_table(runs):
    """C2 : publications uniques, rejetées et motifs par source, cumulés sur les exécutions."""
    unique, rejected, reasons = Counter(), Counter(), {}
    for metrics in runs["métriques"].dropna():
        unique.update(metrics["unique_by_source"])
        for source, by_reason in metrics["rejected_by_source"].items():
            rejected[source] += sum(by_reason.values())
            reasons.setdefault(source, Counter()).update(by_reason)
    return pd.DataFrame([
        {
            "source": source,
            "publications uniques": unique[source],
            "rejetées": rejected[source],
            "taux de rejet (%)": round(100 * rejected[source] / unique[source], 1),
            "motifs": ", ".join(f"{r} : {n}" for r, n in reasons.get(source, Counter()).most_common()),
        }
        for source in sorted(unique)
    ])


st.set_page_config(page_title="CheckIt.AI – Suivi du pipeline", layout="wide")
st.title("CheckIt.AI – Suivi du pipeline de données")
st.caption("Données rafraîchies toutes les 5 minutes. Heures en UTC.")

data = load_data_kpis()
try:
    runs = load_runs()
except requests.RequestException:
    runs = None
    st.error("Airflow est indisponible : les indicateurs d'exécution ne peuvent pas être affichés.")

# --- 1. Le pipeline tourne-t-il ? ---
st.header("1. Le pipeline tourne-t-il ?")
if runs is not None:
    success = runs[runs["état"] == "réussie"]
    scheduled = runs[runs["type"] == "planifiée"]
    col1, col2, col3 = st.columns(3)
    col1.metric("Dernière exécution réussie",
                f"il y a {hours_since(success['fin'].max()):.1f} h" if not success.empty else "aucune")
    col2.metric("Taux de réussite (exécutions planifiées)",
                f"{100 * (scheduled['état'] == 'réussie').mean():.0f} %" if not scheduled.empty else "—")
    col3.metric("Exécutions analysées", len(runs))
    st.caption("Taux de réussite : état final. Une exécution réinitialisée puis réussie compte comme réussie.")

    st.subheader("Durée totale de chaque exécution (minutes)")
    st.caption("Repère les retards entre les tâches : mise en veille, scheduler arrêté.")
    st.scatter_chart(runs, x="début", y="durée totale (min)")

    st.subheader("Durée de chaque tâche (secondes)")
    st.caption("Repère une tâche lente : réseau, volume inhabituel. Durée du dernier essai.")
    durations = runs.melt(id_vars="début", value_vars=[f"durée {t} (s)" for t in TASKS],
                          var_name="tâche", value_name="durée (s)")
    durations["tâche"] = durations["tâche"].str.removeprefix("durée ").str.removesuffix(" (s)")
    st.scatter_chart(durations, x="début", y="durée (s)", color="tâche")

    st.subheader("Dernières exécutions")
    st.dataframe(
        runs.sort_values("début", ascending=False)[
            ["exécution du", "type", "état", "durée totale (min)", "tâches en échec", "tâches relancées"]
        ].round({"durée totale (min)": 1}),
        hide_index=True,
    )

# --- 2. Collecte-t-il du nouveau ? ---
st.header("2. Collecte-t-il du nouveau ?")
st.subheader("Dernière collecte par source")
fresh = data[["source", "derniere_collecte"]].copy()
fresh["dernière collecte (UTC)"] = fresh["derniere_collecte"].map(lambda ts: ts.strftime("%d/%m/%Y %H:%M"))
fresh["il y a (h)"] = fresh["derniere_collecte"].map(hours_since).round(1)
fresh["remarque"] = fresh["source"].map(
    {"fakeddit": "source statique : pas de collecte quotidienne"}).fillna("")
st.dataframe(fresh[["source", "dernière collecte (UTC)", "il y a (h)", "remarque"]], hide_index=True)

if runs is not None:
    st.subheader("Résultat du chargement par exécution")
    loads = runs.melt(id_vars="exécution du", value_vars=["insérées", "mises à jour"],
                      var_name="résultat", value_name="publications")
    st.altair_chart(
        alt.Chart(loads).mark_bar().encode(
            x=alt.X("exécution du:N", sort=None, title=None),
            y=alt.Y("publications:Q", scale=alt.Scale(zero=True)),
            color=alt.Color("résultat:N", title=None),
        )
    )

    st.subheader("Crédits NewsData.io")
    credits = runs.dropna(subset=["crédits NewsData.io"])
    by_day = credits.groupby(credits["début"].dt.date)["crédits NewsData.io"].sum()
    today = int(by_day.get(pd.Timestamp.now(tz="UTC").date(), 0))
    col1, col2 = st.columns(2)
    col1.metric("Aujourd'hui (UTC)", f"{today} / {CREDIT_LIMIT} ({100 * today / CREDIT_LIMIT:.0f} %)")
    col2.metric("Maximum sur la période",
                f"{int(by_day.max())} / {CREDIT_LIMIT}" if not by_day.empty else "non disponible")
    st.caption("Borne basse : requêtes de l'essai réussi + 1 par essai raté. Mesuré depuis le 12/11.")

# --- 3. Les données sont-elles exploitables ? ---
st.header("3. Les données sont-elles exploitables ?")
columns = st.columns(len(data) + 1)
columns[0].metric("Publications en base", int(data["publications"].sum()))
for column, (_, row) in zip(columns[1:], data.iterrows()):
    column.metric(row["source"], int(row["publications"]))

st.subheader("Complétude et cohérence par source")
st.dataframe(
    data[["source", "pct_descriptions", "dates_futures", "dates_manquantes"]].rename(columns={
        "pct_descriptions": "descriptions renseignées (%)",
        "dates_futures": "dates dans le futur",
        "dates_manquantes": "dates manquantes",
    }),
    hide_index=True,
)
if runs is not None:
    st.subheader("Rejets en transformation (exécutions depuis le 12/11)")
    st.dataframe(rejection_table(runs), hide_index=True)