SELECT
    source,
    COUNT(*) AS publications,                                         -- C1
    MAX(collected_at) AS derniere_collecte,                           -- B1
    ROUND(100.0 * COUNT(*) FILTER (WHERE NULLIF(TRIM(description), '') IS NOT NULL)
          / COUNT(*), 1) AS pct_descriptions,                         -- C3
    COUNT(*) FILTER (WHERE published_at > collected_at) AS dates_futures,   -- C4
    COUNT(*) FILTER (WHERE published_at IS NULL) AS dates_manquantes        -- C4
FROM publications
GROUP BY source
ORDER BY source;