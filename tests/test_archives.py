"""Tests de l'archivage des fichiers bruts et du cas « aucune nouvelle donnée »."""
import pytest

import load
import transform
from common import archive_raw_files
from collections import Counter


def make_raw_files(folder, names):
    """Crée de faux fichiers bruts dans un dossier temporaire."""
    for name in names:
        (folder / name).write_text('{"id": "x"}\n', encoding="utf-8")


def test_archive_moves_only_listed_files(tmp_path):
    make_raw_files(tmp_path, ["a.jsonl", "b.jsonl", "c.jsonl"])
    assert archive_raw_files(["a.jsonl", "b.jsonl"], raw_dir=tmp_path) == 2
    assert sorted(p.name for p in (tmp_path / "archive").iterdir()) == ["a.jsonl", "b.jsonl"]
    assert (tmp_path / "c.jsonl").exists()  # non listé : reste à traiter


def test_archive_is_idempotent(tmp_path):
    make_raw_files(tmp_path, ["a.jsonl"])
    archive_raw_files(["a.jsonl"], raw_dir=tmp_path)
    assert archive_raw_files(["a.jsonl"], raw_dir=tmp_path) == 0  # relance : rien à refaire, pas d'erreur


def test_archive_refuses_to_overwrite(tmp_path):
    make_raw_files(tmp_path, ["a.jsonl"])
    (tmp_path / "archive").mkdir()
    make_raw_files(tmp_path / "archive", ["a.jsonl"])
    with pytest.raises(FileExistsError):
        archive_raw_files(["a.jsonl"], raw_dir=tmp_path)
    assert (tmp_path / "a.jsonl").exists()  # le fichier n'a pas bougé


def test_transform_with_no_new_file(tmp_path, monkeypatch):
    exported = []

    def fake_export(records):
        exported.append(records)
        return True

    monkeypatch.setattr(transform, "export_jsonl", fake_export)
    result = transform.run(raw_dir=tmp_path)
    assert result["exported"] == 0
    assert result["raw_files"] == []
    assert result["metrics"]["read"] == 0
    assert exported == [[]]

def test_build_metrics():
    unique = [
        {"source": "fakeddit", "label": "satire"},
        {"source": "fakeddit", "label": "true"},
        {"source": "newsdata", "label": None},
    ]
    valid = [unique[1], unique[2]]
    rejected = Counter({("fakeddit", "erreur HTTP 404"): 1})
    metrics = transform.build_metrics(unique + [unique[0]], unique, unique, unique, valid, rejected)
    assert metrics["read"] == 4
    assert metrics["duplicates"] == 0
    assert metrics["unique_by_source"] == {"fakeddit": 2, "newsdata": 1}
    assert metrics["rejected_by_source"] == {"fakeddit": {"erreur HTTP 404": 1}}
    assert metrics["rejected_by_label"] == {"satire": 1}

def test_load_with_empty_file(tmp_path):
    path = tmp_path / "publications.jsonl"
    path.write_text("", encoding="utf-8")
    assert load.run(path) == (0, 0, 0)  # rien à charger, sans connexion à la base
    