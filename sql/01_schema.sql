CREATE TABLE publications (
    -- Identification
    id            TEXT PRIMARY KEY,               -- préfixe de source + id d'origine
    original_id   TEXT NOT NULL,
    source        TEXT NOT NULL
                  CHECK (source IN ('fakeddit', 'newsdata', 'decodeurs')),
    url           TEXT,                           -- NULL pour Fakeddit

    -- Contenu texte
    title         TEXT NOT NULL,
    description   TEXT,
    language      TEXT NOT NULL,                  -- ISO 639-1

    -- Dates (UTC)
    published_at  TIMESTAMPTZ NOT NULL,
    collected_at  TIMESTAMPTZ NOT NULL,

    -- Label : NULL ne veut pas dire « vrai »
    label         TEXT,
    label_source  TEXT,
    label_scope   TEXT,

    -- Image (relation 1-1)
    image_url     TEXT NOT NULL,
    image_path    TEXT NOT NULL,                  -- relatif à la racine du projet
    image_width   INTEGER NOT NULL CHECK (image_width >= 64),
    image_height  INTEGER NOT NULL CHECK (image_height >= 64),

    -- Champs propres à chaque source (Média, subreddit, score…)
    metadata      JSONB NOT NULL DEFAULT '{}'
);