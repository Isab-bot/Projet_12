"""Vérifie que chaque module du pipeline s'importe sans erreur (import oublié, faute de frappe…)."""
import importlib

import pytest

MODULES = ["common", "extract_decodeurs", "extract_fakeddit", "extract_newsdata", "transform", "load", "pipeline"]


@pytest.mark.parametrize("module", MODULES)
def test_module_imports(module):
    importlib.import_module(module)