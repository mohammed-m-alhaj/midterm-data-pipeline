from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from config.settings import REPORTS_DIR

RESULTS_FILE = REPORTS_DIR / "results.json"


def append_run_metrics(metrics: dict[str, Any]) -> None:
    """Create or merge a metrics record keyed by run_id."""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    try:
        payload = json.loads(RESULTS_FILE.read_text(encoding="utf-8")) if RESULTS_FILE.exists() else []
        if not isinstance(payload, list):
            payload = []
    except json.JSONDecodeError:
        payload = []

    run_id = metrics.get("run_id")
    merged = None
    new_payload = []
    for item in payload:
        if item.get("run_id") == run_id and merged is None:
            merged = {**item, **metrics}
            new_payload.append(merged)
        elif item.get("run_id") != run_id:
            new_payload.append(item)
    if merged is None:
        new_payload.append(metrics)

    RESULTS_FILE.write_text(
        json.dumps(new_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def read_metrics() -> list[dict[str, Any]]:
    """Read all metrics records from results.json."""
    if not RESULTS_FILE.exists():
        return []
    try:
        data = json.loads(RESULTS_FILE.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        return []

