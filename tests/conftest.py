import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

# Ensure project root is importable so `udf_service` can be resolved in tests.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
project_root_str = str(PROJECT_ROOT)
if project_root_str not in sys.path:
    sys.path.insert(0, project_root_str)

from udf_service import server


@pytest.fixture
def client():
    return TestClient(server.app)


@pytest.fixture
def set_symbols(monkeypatch):
    def _set(symbols):
        monkeypatch.setattr(server._client, "symbols", lambda: symbols)

    return _set
