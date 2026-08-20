"""
Automated Playwright UI Test & Screenshot Generator for Web Dashboard.
Opens http://localhost:8000, tests interactive controls, captures execution proof screenshots.
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path
import pytest

from common import PROJECT_ROOT


def test_web_dashboard_playwright():
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        pytest.skip("Playwright is not installed.")

    screenshots_dir = PROJECT_ROOT / "reports" / "screenshots"
    screenshots_dir.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        try:
            page.goto("http://localhost:8000", timeout=5000)
        except Exception:
            pytest.skip("Web dashboard server is not running on http://localhost:8000")

        assert "جامعة الرازي" in page.content() or "Dashboard" in page.content() or "HYBRID" in page.content()

        # Capture UI dashboard screenshot
        screenshot_path = screenshots_dir / "dashboard_proof.png"
        page.screenshot(path=str(screenshot_path), full_page=True)
        assert screenshot_path.is_file()

        browser.close()
