\echo '1. Nombre de publications par source'
SELECT source, COUNT(*) AS nb
FROM publications
GROUP BY source
ORDER BY source;

\echo '2. Unicité des identifiants'
SELECT COUNT(*) AS total, COUNT(DISTINCT id) AS ids_distincts
FROM publications;

\echo '3. Champs optionnels renseignés, par source'
SELECT source,
       COUNT(*)            AS total,
       COUNT(url)          AS avec_url,
       COUNT(description)  AS avec_description,
       COUNT(label)        AS avec_label,
       COUNT(label_source) AS avec_label_source
FROM publications
GROUP BY source
ORDER BY source;

\echo '4a. Période couverte, par source'
SELECT source, MIN(published_at) AS plus_ancienne, MAX(published_at) AS plus_recente
FROM publications
GROUP BY source
ORDER BY source;

\echo '4b. Publications datées après leur collecte (anomalie attendue : 0)'
SELECT COUNT(*) AS anomalies
FROM publications
WHERE published_at > collected_at;

\echo '5. Publications sans métadonnées (attendu : 0)'
SELECT COUNT(*) AS sans_metadata
FROM publications
WHERE metadata = '{}';