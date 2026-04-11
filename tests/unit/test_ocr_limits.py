from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "api"))

from services.ocr import OCRService  # noqa: E402


class FakePool:
    def __init__(self, total_pages: int):
        self.total_pages = total_pages
        self.fetchval_calls = 0

    async def fetchval(self, sql, *args):
        self.fetchval_calls += 1
        return self.total_pages


@pytest.mark.asyncio
async def test_global_page_limits_are_skipped_when_flag_disabled(monkeypatch):
    pool = FakePool(total_pages=999999)
    service = OCRService(None, pool)

    monkeypatch.setattr("services.ocr.settings.GLOBAL_OCR_ENABLED", True)
    monkeypatch.setattr("services.ocr.settings.OCR_PAGE_LIMITS_ENABLED", False)

    await service._check_global_limits("doc-1")

    assert pool.fetchval_calls == 0


@pytest.mark.asyncio
async def test_global_page_limits_still_apply_when_flag_enabled(monkeypatch):
    pool = FakePool(total_pages=50000)
    service = OCRService(None, pool)

    monkeypatch.setattr("services.ocr.settings.GLOBAL_OCR_ENABLED", True)
    monkeypatch.setattr("services.ocr.settings.OCR_PAGE_LIMITS_ENABLED", True)
    monkeypatch.setattr("services.ocr.settings.GLOBAL_MAX_PAGES", 50000)

    with pytest.raises(ValueError, match="Platform page limit reached"):
        await service._check_global_limits("doc-1")

    assert pool.fetchval_calls == 1
