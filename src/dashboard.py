"""Tableau de bord KPI de l'ETL CheckIt.AI (Streamlit).

Données → PostgreSQL (rôle checkit_reader) ; exécutions → API REST d'Airflow (compte Viewer).
"""
import os
from collections import Counter

import altair as alt
import pandas as pd
import psycopg
import requests
import streamlit as st
from dotenv import load_dotenv

import airflow_api
from common import PROJECT_ROOT

load_dotenv(PROJECT_ROOT / ".env")
KPI_SQL = (PROJECT_ROOT / "queries" / "kpi_donnees.sql").read_text(encoding="utf-8")
TASKS = ["extract_decodeurs", "extract_newsdata", "transform", "load", "archive"]
CREDIT_LIMIT = 200
STATES = {"success": "réussie", "failed": "échouée", "running": "en cours", "queued": "en attente"}
OK, WARN, ALERT, NA = "🟢 normal", "🟠 vigilance", "🔴 alerte", "⚪ non disponible"
COLORS = {OK: "green", WARN: "orange", ALERT: "red", NA: "gray"}
DAILY_SOURCES = ["decodeurs", "newsdata"]
REFERENCE_DESCRIPTIONS = {"decodeurs": 100.0, "newsdata": 94.1}  # mesuré le 12/11/2026
RUNNING = "1. Le pipeline tourne-t-il ?"
COLLECTING = "2. Collecte-t-il du nouveau ?"
USABLE = "3. Les données sont-elles exploitables ?"


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


def credits_by_day(runs):
    """Crédits NewsData.io par jour (UTC), sur les exécutions où ils sont mesurés."""
    credits = runs.dropna(subset=["crédits NewsData.io"])
    return credits.groupby(credits["début"].dt.date)["crédits NewsData.io"].sum()


def status_table(data, runs):
    """État de chaque KPI d'alarme par rapport à son seuil (plan de monitoring)."""
    rows = []

    def add(group, indicator, value, threshold, state):
        rows.append({"groupe": group, "indicateur": indicator, "valeur": value,
                     "seuil d'alerte": threshold, "état": state})

    # --- 1. Le pipeline tourne-t-il ? (Airflow) ---
    if runs is None:
        add(RUNNING, "Exécutions (Airflow)", "Airflow indisponible", "—", ALERT)
        week = None
    else:
        now = pd.Timestamp.now(tz="UTC")
        last_success = runs.loc[runs["état"] == "réussie", "fin"].max()
        hours = hours_since(last_success) if pd.notna(last_success) else float("inf")
        add(RUNNING, "Dernière exécution réussie", f"il y a {hours:.1f} h", "> 26 h",
            ALERT if hours > 26 else OK)

        week = runs[runs["début"] >= now - pd.Timedelta(days=7)]
        failed = week[(week["type"] == "planifiée") & (week["état"] == "échouée")]
        add(RUNNING, "Exécutions planifiées échouées (7 j)", str(len(failed)), "> 0",
            ALERT if len(failed) else OK)

        last, previous = runs.iloc[-1], runs.iloc[:-1]
        total = last["durée totale (min)"]
        add(RUNNING, "Durée de la dernière exécution", f"{total:.1f} min" if pd.notna(total) else "en cours",
            "> 10 min", NA if pd.isna(total) else ALERT if total > 10 else OK)
        slow = [task for task in TASKS
                if pd.notna(last[f"durée {task} (s)"])
                and last[f"durée {task} (s)"] > 5 * previous[f"durée {task} (s)"].median()]
        add(RUNNING, "Tâches lentes (dernière exécution)", ", ".join(slow) or "aucune",
            "> 5 × médiane", ALERT if slow else OK)

    # --- 2. Collecte-t-il du nouveau ? ---
    for source in DAILY_SOURCES:
        hours = hours_since(data.loc[data["source"] == source, "derniere_collecte"].iloc[0])
        add(COLLECTING, f"Dernière collecte – {source}", f"il y a {hours:.1f} h", "> 26 h",
            ALERT if hours > 26 else OK)
    if runs is not None:
        today = int(credits_by_day(runs).get(pd.Timestamp.now(tz="UTC").date(), 0))
        add(COLLECTING, "Crédits NewsData.io aujourd'hui", f"{today} / {CREDIT_LIMIT}",
            "> 100 vigilance, > 160 alerte", ALERT if today > 160 else WARN if today > 100 else OK)

    # --- 3. Les données sont-elles exploitables ? ---
    if week is not None:
        rejections = rejection_table(week)
        for _, row in rejections.iterrows():
            add(USABLE, f"Taux de rejet (7 j) – {row['source']}", f"{row['taux de rejet (%)']} %",
                "> 30 % (provisoire)", ALERT if row["taux de rejet (%)"] > 30 else OK)
        if rejections.empty:
            add(USABLE, "Taux de rejet (7 j)", "non mesuré", "> 30 % (provisoire)", NA)
    for source, reference in REFERENCE_DESCRIPTIONS.items():
        pct = data.loc[data["source"] == source, "pct_descriptions_7j"].iloc[0]
        threshold = f"< {reference - 10:.0f} %"
        if pd.isna(pct):
            add(USABLE, f"Descriptions renseignées (7 j) – {source}", "aucune publication récente",
                threshold, NA)
        else:
            add(USABLE, f"Descriptions renseignées (7 j) – {source}", f"{float(pct):.1f} %", threshold,
                ALERT if float(pct) < reference - 10 else OK)
    futures = int(data["dates_futures"].sum())
    add(USABLE, "Dates dans le futur", str(futures), "> 0", ALERT if futures else OK)
    return pd.DataFrame(rows)


def show_status(status):
    """Bandeau de synthèse, puis une carte par indicateur, groupées par question."""
    alerts = (status["état"] == ALERT).sum()
    warnings = (status["état"] == WARN).sum()
    if alerts:
        st.error(f"{alerts} alerte(s) : voir les indicateurs en rouge.")
    elif warnings:
        st.warning(f"{warnings} point(s) de vigilance : voir les indicateurs en orange.")
    else:
        st.success("Tout fonctionne normalement.")
    for group, rows in status.groupby("groupe", sort=False):
        st.subheader(group)
        columns = st.columns(len(rows))
        for column, (_, row) in zip(columns, rows.iterrows()):
            threshold = row["seuil d'alerte"]
            with column.container(border=True):
                st.metric(row["indicateur"], row["valeur"])
                st.markdown(f":{COLORS[row['état']]}[{row['état']}] · seuil : {threshold}")


st.set_page_config(page_title="CheckIt.AI – Suivi du pipeline", layout="wide")
st.title("CheckIt.AI – Suivi du pipeline de données")
st.caption("Données rafraîchies toutes les 5 minutes. Heures en UTC.")

data = load_data_kpis()
try:
    runs = load_runs()
except requests.RequestException:
    runs = None
    st.error("Airflow est indisponible : les indicateurs d'exécution ne peuvent pas être affichés.")

tab_state, tab_details = st.tabs(["État du pipeline", "Détails"])

with tab_state:
    show_status(status_table(data, runs))
    st.caption("Seuils définis dans le plan de monitoring. "
               "Vérification quotidienne, après l'exécution de 6 h UTC.")

with tab_details:
    # --- 1. Le pipeline tourne-t-il ? ---
    st.header(RUNNING)
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
    st.header(COLLECTING)
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
        by_day = credits_by_day(runs)
        today = int(by_day.get(pd.Timestamp.now(tz="UTC").date(), 0))
        col1, col2 = st.columns(2)
        col1.metric("Aujourd'hui (UTC)", f"{today} / {CREDIT_LIMIT} ({100 * today / CREDIT_LIMIT:.0f} %)")
        col2.metric("Maximum sur la période",
                    f"{int(by_day.max())} / {CREDIT_LIMIT}" if not by_day.empty else "non disponible")
        st.caption("Borne basse : requêtes de l'essai réussi + 1 par essai raté. Mesuré depuis le 12/11.")

    # --- 3. Les données sont-elles exploitables ? ---
    st.header(USABLE)
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