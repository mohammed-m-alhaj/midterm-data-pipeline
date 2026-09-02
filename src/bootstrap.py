from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"


def ensure_project_root() -> Path:
    """Allow running project files directly, for example python src/main.py."""
    for path in (PROJECT_ROOT, SRC_DIR):
        path_text = str(path)
        if path_text not in sys.path:
            sys.path.insert(0, path_text)
    return PROJECT_ROOT
