# Plan de monitoring – Pipeline ETL CheckIt.AI

> Stratégie de surveillance du pipeline d'acquisition de données multimodales (texte + image).
> Version du 12/11/2026. Les seuils de ce document sont exactement ceux du tableau de bord (`src/dashboard.py`, fonction `status_table()`).

## 1. Objet et principes

Le monitoring répond à trois questions :

1. **Le pipeline tourne-t-il ?**
2. **Collecte-t-il du nouveau ?**
3. **Les données sont-elles exploitables ?**

Chaque indicateur d'alarme suit la chaîne **question → indicateur → seuil → action**. Un indicateur sans action possible n'est pas surveillé ; il sert seulement de contexte.

Les seuils ont trois origines :

| Origine | Exemple |
|---|---|
| **Contrainte** extérieure | 200 crédits NewsData.io par jour ; cadence quotidienne |
| **Valeur observée** dans les données | durée habituelle d'une exécution ; taux de descriptions renseignées |
| **Principe** | aucune date de publication dans le futur |

## 2. Dispositif de surveillance

### 2.1 Ce qui est automatisé

| Élément | Comportement |
|---|---|
| **Planification** | `checkit_daily` s'exécute chaque jour à 6 h UTC (`0 6 * * *`) ; `catchup=False` : au redémarrage, seule la dernière exécution manquée est lancée |
| **Relances** | extractions : 3 relances à 5 min d'intervalle (erreurs réseau transitoires) ; `transform`, `load`, `archive` : 1 relance après 1 min |
| **Échec partiel** | `transform` s'exécute si au moins une extraction a réussi (`all_done_min_one_success`) ; la tâche finale `check_extractions` rend l'exécution **rouge** si une extraction a échoué |
| **Simultanéité** | `max_active_runs=1` + pool `checkit_publications` (1 place) : jamais deux transformations ou chargements en même temps |
| **Démarrage des services** | `checkit_postgres` : `restart: always` + healthcheck `pg_isready` ; les services Airflow attendent que les deux bases soient `healthy` |
| **Contrôles internes** | export atomique ; arrêt si **toutes** les données sont rejetées ; archivage refusé s'il écraserait une archive existante ; chargement en transaction (tout ou rien) |
| **Tableau de bord** | relit PostgreSQL (rôle `checkit_reader`) et l'API REST d'Airflow (compte `dashboard`, rôle Viewer) ; données mises en cache 5 min ; état de chaque indicateur calculé automatiquement (🟢 normal, 🟠 vigilance, 🔴 alerte, ⚪ non disponible) |

### 2.2 Ce qui ne l'est pas

- **Aucune notification n'est envoyée** (courriel, messagerie). Les alertes sont **visuelles** : elles ne sont vues que si quelqu'un ouvre le tableau de bord. La vérification quotidienne (section 6) est donc indispensable.
- Les **actions correctives** (relancer une tâche, corriger une configuration) sont manuelles.

## 3. Journaux

| Journal | Emplacement | Contenu utile |
|---|---|---|
| Tâches Airflow | interface Airflow (exécution → tâche → **Journaux**, un onglet par essai) ; fichiers dans `logs/` | début et fin de chaque étape, comptes, erreurs avec trace complète |
| Scripts hors Airflow | console (`pipeline.py`, scripts lancés à la main) | mêmes messages |

**Messages à connaître :**

| Étape | Messages clés |
|---|---|
| Extraction | `N entrées conservées, M écartées (titre ou image manquant)` ; `Page n` (une requête NewsData.io = un crédit) |
| Transformation | `N enregistrements lus dans F fichiers` ; `N uniques, M doublons écartés` ; `N images valides, M rejetées` puis **une ligne par source et par motif** (`rejet – newsdata – erreur HTTP 403 : 2`) |
| Chargement | `N insérées, M mises à jour, K inchangées` |
| Archivage | `N fichiers bruts archivés, M déjà archivés` |

**Niveaux :** `INFO` pour le déroulement normal, `WARNING` pour une ligne ignorée (ligne JSON illisible), `ERROR` pour un échec qui arrête la tâche.

Les métriques utiles au suivi sont aussi **renvoyées par les tâches** (XCom) et lues par le tableau de bord : comptes de `load`, requêtes de `extract_newsdata`, métriques de `transform` (lus, doublons, rejets par source, par motif et par classe Fakeddit).

## 4. Indicateurs et seuils

### 4.1 Indicateurs d'alarme

| # | Indicateur | Source | Seuil | Action en cas d'alerte |
|---|---|---|---|---|
| A1 | Dernière exécution réussie | Airflow | **> 26 h** (cadence + 2 h) | Ouvrir la dernière exécution, identifier la tâche en échec ; vérifier que Docker tourne et que l'ordinateur n'était pas en veille |
| A2 | Exécutions planifiées échouées (7 jours) | Airflow | **> 0** | Lire le journal de la tâche en échec ; si la cause est déjà connue et corrigée, aucune action (l'alerte disparaît après 7 jours) |
| A3a | Durée de la dernière exécution | Airflow | **> 10 min** (habituel : < 3 min) | Retard entre les tâches : vérifier la mise en veille ou l'arrêt du scheduler |
| A3b | Tâches lentes (dernière exécution) | Airflow | **> 5 × la médiane** de la tâche | Vérifier le réseau (téléchargement d'images, API) ou un volume inhabituel |
| B1 | Dernière collecte – Les Décodeurs, NewsData.io | PostgreSQL | **> 26 h** | Lire le journal de l'extraction ; vérifier la clé d'API, les crédits, le format du flux. Fakeddit est exclu (source statique) |
| B3 | Crédits NewsData.io consommés aujourd'hui | Airflow | **> 100 vigilance ; > 160 alerte** (limite : 200) | Vérifier les relances en série et les exécutions manuelles ; éviter tout déclenchement manuel jusqu'au lendemain |
| C2 | Taux de rejet en transformation, par source (7 jours) | Airflow | **> 30 %** (provisoire) | Lire les motifs : 403 (site qui bloque), 404 (liens morts), image trop petite, erreur réseau |
| C3 | Descriptions renseignées, par source (7 jours) | PostgreSQL | **baisse de plus de 10 points** par rapport à la référence (Décodeurs : < 90 % ; NewsData.io : < 84 %) | Vérifier un changement de format de la source |
| C4 | Dates de publication dans le futur | PostgreSQL | **> 0** | Vérifier le fuseau horaire et le format des dates de la source concernée |
| — | Airflow indisponible | API Airflow | inaccessible | Vérifier les conteneurs (`docker compose ps`) ; les indicateurs de données restent affichés |

### 4.2 Indicateurs de contexte (sans seuil)

- Publications en base, au total et par source.
- Insertions, mises à jour et inchangées par exécution (distingue « présentes en base » et « réellement ajoutées »).
- Taux de réussite des exécutions planifiées, sur la période affichée.
- Durées de chaque exécution et de chaque tâche, dans le temps.
- Détail des rejets par motif ; crédits maximum sur la période.

### 4.3 Choix de mesure

| Choix | Raison |
|---|---|
| Rejet calculé sur les publications **uniques** | un doublon n'est pas une donnée invalide |
| Rejet et descriptions **par source** | un taux global mesure le mélange des sources, pas leur qualité |
| Descriptions sur **7 jours** pour l'alarme | un pourcentage cumulé sur toute la base réagirait trop lentement |
| Durée **totale** et durée **par tâche** | la mise en veille retarde le passage d'une tâche à l'autre sans allonger les tâches |
| Seuil **relatif** pour les tâches | la durée varie fortement avec le réseau, sans problème réel |
| Vitesse (publications / durée) **écartée** | le temps dépend surtout des coûts fixes et des images nouvelles, pas du volume |

## 5. Gestion des erreurs

| Situation | Comportement automatique | Action manuelle |
|---|---|---|
| Source momentanément indisponible | 3 relances de l'extraction | Aucune si une relance réussit |
| Source en panne durable | les autres sources sont traitées ; exécution rouge (`check_extractions`) ; la date de dernière collecte se fige (B1) | Lire le journal ; corriger clé, URL ou format |
| Clé NewsData.io invalide ou crédits épuisés | échec de l'extraction après les relances | Vérifier la clé dans `.env` ; attendre le lendemain si les crédits sont épuisés |
| Aucun nouvel article | **pas un échec** : fichier vide exporté, chargement `(0, 0, 0)` | Aucune, sauf si la situation dure (B1) |
| Toutes les données reçues rejetées | `transform` lève une exception | Lire les motifs de rejet |
| Base du projet indisponible | 1 relance de `load` ; au démarrage de Docker, la base redémarre seule | Vérifier `docker compose ps` |
| Mise en veille pendant une exécution | suivi des tâches perdu ou jeton expiré (`403 Invalid auth token`) ; relance possible | Si l'exécution reste en échec : **Réinitialiser** la tâche avec l'option **En aval** (pas de nouvelle extraction, aucun crédit consommé) |
| Erreur dans le code (bug) | échec identique à la relance | Corriger, tester (pytest), puis réinitialiser la tâche |
| Fichiers bruts non archivés après un échec | ils restent dans `data/raw/` et sont repris par l'exécution suivante | Aucune |

**Règle de reprise :** une tâche échouée est **réinitialisée** (↻, « En aval ») plutôt que l'exécution relancée entièrement, pour ne pas réextraire ni consommer de crédits. Toutes les étapes sont **idempotentes** : les refaire ne crée ni doublon ni régression.

## 6. Fréquence des vérifications

| Fréquence | Quoi | Où |
|---|---|---|
| **Quotidienne**, après l'exécution de 6 h UTC | onglet **État du pipeline** : bandeau de synthèse, cartes rouges ou orange | tableau de bord |
| **Hebdomadaire** | onglet **Détails** : tendance des durées, chargements par exécution, motifs de rejet, crédits maximum | tableau de bord |
| **À chaque alerte** | journal de la tâche concernée, onglet de l'essai en échec | interface Airflow |
| **Révision ponctuelle** | seuil provisoire de C2 et références de C3, après une semaine de mesures | ce document et `dashboard.py` |

## 7. Limites connues

- **Pas de notification** : une alerte n'est vue qu'à l'ouverture du tableau de bord.
- **Déploiement sur un ordinateur personnel** : la mise en veille a perturbé 3 exécutions (07/11, 10/11, 11/11). En production, un serveur toujours actif supprimerait ce risque.
- **Taux de réussite = état final** : une exécution réinitialisée puis réussie compte comme réussie.
- **Durée par tâche = dernier essai** : la durée d'un essai raté n'apparaît que dans Airflow.
- **Crédits NewsData.io = borne basse** : requêtes de l'essai réussi + 1 par essai raté.
- **Historique partiel** : crédits et rejets mesurés depuis le 12/11/2026 (format des valeurs renvoyées).
- **Fenêtre de 7 jours** : une alerte reste visible jusqu'à 7 jours après sa correction.
- **Dépendance à Airflow** : si Airflow est arrêté, les indicateurs d'exécution ne sont pas affichés (signalé en alerte).
- **Seuil de rejet provisoire** : fixé sur un petit échantillon (18 publications NewsData.io).

## 8. Évolutions possibles

- Notification par courriel en cas d'échec (`on_failure_callback` ou réglage d'Airflow).
- Table de suivi des exécutions dans PostgreSQL, si un historique plus long que celui d'Airflow devient nécessaire.
- Suivi de l'espace disque (images et archives), qui grandit chaque jour.