"""Tests du chargement : chaque test s'exécute dans une transaction annulée."""
import psycopg
import pytest

from load import get_connection, load_rows, to_row

# Publication fictive : indépendante des fichiers de données et de l'état de la base
BASE = {
    "id": "decodeurs:pytest-0001",
    "original_id": "pytest-0001",
    "source": "decodeurs",
    "url": "https://exemple.org/article",
    "title": "Titre initial",
    "description": None,
    "language": "fr",
    "published_at": "2026-01-01T08:00:00+00:00",
    "collected_at": "2026-01-02T08:00:00+00:00",
    "label": None,
    "label_source": None,
    "label_scope": None,
    "image_url": "https://exemple.org/image.jpg",
    "image_path": "data/images/pytest.jpg",
    "image_width": 800,
    "image_height": 600,
    "image_caption": "Légende de test",
}


@pytest.fixture
def connection():
    """Connexion dont tout ce qui est écrit est annulé à la fin du test."""
    with get_connection() as conn:
        with conn.transaction(force_rollback=True):
            yield conn


def title_in_db(connection):
    return connection.execute(
        "SELECT title FROM publications WHERE id = %s", (BASE["id"],)
    ).fetchone()[0]


def test_new_record_is_inserted(connection):
    assert load_rows(connection, [to_row(BASE)]) == (1, 0, 0)


def test_identical_record_is_unchanged(connection):
    load_rows(connection, [to_row(BASE)])
    assert load_rows(connection, [to_row(BASE)]) == (0, 0, 1)


def test_older_record_is_ignored(connection):
    load_rows(connection, [to_row(BASE)])
    older = {**BASE, "title": "Titre ancien", "collected_at": "2026-01-01T08:00:00+00:00"}
    assert load_rows(connection, [to_row(older)]) == (0, 0, 1)
    assert title_in_db(connection) == "Titre initial"


def test_newer_record_is_updated(connection):
    load_rows(connection, [to_row(BASE)])
    newer = {**BASE, "title": "Titre mis à jour", "collected_at": "2026-01-03T08:00:00+00:00"}
    assert load_rows(connection, [to_row(newer)]) == (0, 1, 0)
    assert title_in_db(connection) == "Titre mis à jour"


def test_missing_title_is_rejected(connection):
    with pytest.raises(psycopg.errors.NotNullViolation):
        load_rows(connection, [to_row({**BASE, "title": None})])


def test_unknown_source_is_rejected(connection):
    with pytest.raises(psycopg.errors.CheckViolation):
        load_rows(connection, [to_row({**BASE, "source": "inconnue"})])