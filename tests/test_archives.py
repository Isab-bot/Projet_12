"""Tests de l'archivage des fichiers bruts et du cas « aucune nouvelle donnée »."""
import pytest

import load
import transform
from common import archive_raw_files


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
    assert transform.run(raw_dir=tmp_path) == {"exported": 0, "raw_files": []}
    assert exported == [[]]  # un fichier vide est exporté


def test_load_with_empty_file(tmp_path):
    path = tmp_path / "publications.jsonl"
    path.write_text("", encoding="utf-8")
    assert load.run(path) == (0, 0, 0)  # rien à charger, sans connexion à la base
    