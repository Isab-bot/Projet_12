# CheckIt.AI – Pipeline de données multimodales (texte + image)

Projet de formation (AI Engineer, OpenClassrooms) : pipeline ETL qui collecte, nettoie et stocke des publications **texte + image** destinées à entraîner et surveiller un modèle de détection de désinformation. Le projet fournit les données ; il n'entraîne pas de modèle.

## Vue d'ensemble

```text
NewsData.io (API) ──┐
Les Décodeurs (RSS) ┼─→ data/raw/*.jsonl ─→ transform ─→ data/processed/publications.jsonl ─→ PostgreSQL
Fakeddit (TSV) ─────┘                         └─→ data/images/                                    │
                                                                         archivage ←──────────────┘
Orchestration : Airflow (2 DAG)      Surveillance : tableau de bord Streamlit
```

| Source | Accès | Langue | Label |
|---|---|---|---|
| NewsData.io | API REST (clé) | FR | aucun |
| Les Décodeurs – Le Monde | flux RSS | FR | aucun |
| Fakeddit | fichier TSV, échantillon stratifié de 2 000 lignes | EN | 6 catégories |

## Stack

Python (UV) · requests, feedparser, pandas, Pillow · PostgreSQL 17 · Airflow 3.3.2 (LocalExecutor) · Docker Compose · Streamlit + Altair · pytest

## Organisation du dépôt

```text
src/            scripts d'extraction, transform.py, load.py, pipeline.py, dashboard.py
dags/           checkit_daily et checkit_fakeddit_init
sql/            01_schema.sql, 02_roles.sql (exécutés au premier démarrage de la base)
queries/        requêtes de contrôle et de KPI
tests/          tests pytest
docs/           modèle de données, plan de monitoring
data/           raw/, raw/archive/, processed/, images/ (non versionné)
```

## Installation

Prérequis : Windows 11, Docker Desktop, UV.

1. Installer les dépendances : `uv sync`
2. Créer un fichier `.env` à la racine (jamais versionné) avec :
   - la clé NewsData.io (`NEWSDATA_API_KEY`) ;
   - les identifiants PostgreSQL (administrateur, `checkit_loader`, `checkit_reader`) ;
   - la clé de pseudonymisation HMAC ;
   - les secrets Airflow (`FERNET_KEY`, `AIRFLOW__API_AUTH__JWT_SECRET`) et le compte Viewer du tableau de bord.
3. Télécharger le fichier TSV de Fakeddit (non fourni).
4. Démarrer les services :

```powershell
docker compose build
docker compose up airflow-init
docker compose up -d
```

PostgreSQL est accessible sur `127.0.0.1:5433`, l'interface Airflow sur `http://127.0.0.1:8080`.

## Utilisation

| Action | Commande / lieu |
|---|---|
| Chargement initial Fakeddit | DAG `checkit_fakeddit_init`, déclenchement manuel, une fois |
| Collecte quotidienne | DAG `checkit_daily`, chaque jour à 6 h UTC (à activer dans l'interface) |
| Exécution hors Airflow | `uv run python src/pipeline.py` |
| Tableau de bord | `uv run streamlit run src/dashboard.py` |
| Tests | `uv run pytest` |

En cas d'échec d'une tâche : la réinitialiser dans Airflow (« en aval ») plutôt que relancer toute l'exécution, pour ne pas consommer de crédits NewsData.io.

## Points clés

- **Idempotence** : identifiant déterministe, `INSERT … ON CONFLICT`, export atomique ; une relance ne crée pas de doublon.
- **Traçabilité** : chaque fichier brut est écrit une fois, puis archivé après chargement réussi ; les rejets sont comptés par source et par motif.
- **Sécurité** : rôles PostgreSQL à moindre privilège, authentification SCRAM-SHA-256, auteurs Fakeddit pseudonymisés par HMAC-SHA-256.
- **Monitoring** : 10 KPI (exécution, fraîcheur de la collecte, qualité des données) ; seuils et actions décrits dans `docs/plan_monitoring.md`.

## Limites

- Labels Fakeddit bruités (attribués par subreddit) et en anglais ; flux français sans label.
- Licence des Décodeurs limitée à l'usage personnel.
- Pas de notification : les alertes ne sont visibles qu'à l'ouverture du tableau de bord.
- Déploiement local : la mise en veille de l'ordinateur peut perturber les exécutions.