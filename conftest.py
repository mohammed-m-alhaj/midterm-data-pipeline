"""Root conftest — ensures project root AND src/ are on sys.path.

The ``common.py`` module in ``src/`` adds the project root, but when pytest
discovers modules inside ``src/``, it needs ``src/`` itself on sys.path first
so that ``from common import ...`` resolves.
"""
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"

for p in (str(PROJECT_ROOT), str(SRC_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)
