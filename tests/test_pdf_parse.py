"""Test PDF parse: sample PDF -> text + table (mocked)."""
import tempfile
from unittest.mock import MagicMock, patch

import pytest


def test_parse_pdf_extract_text_and_table():
    """Parse PDF returns (full_text, first_table, num_pages)."""
    from scraper.parse import parse_pdf

    # Create a minimal PDF with pdfplumber by mocking
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
        # Write minimal valid PDF content (header + minimal body)
        f.write(b"%PDF-1.4\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n3 0 obj<</Type/Page/MediaBox[0 0 612 792]/Parent 2 0 R>>endobj\nxref\n0 4\ntrailer<</Size 4/Root 1 0 R>>\nstartxref\n0\n%%EOF\n")
        path = f.name
    try:
        with patch("pdfplumber.open") as mock_open:
            mock_page = MagicMock()
            mock_page.extract_text.return_value = "MoSPI Report 2024. GDP growth."
            mock_page.extract_tables.return_value = [["Head1", "Head2"], ["Val1", "Val2"]]
            mock_pdf = MagicMock()
            mock_pdf.pages = [mock_page]
            mock_open.return_value.__enter__.return_value = mock_pdf

            text, table, pages = parse_pdf(path)
            assert "MoSPI" in text
            assert pages == 1
            assert table is not None
            assert len(table) == 2
            assert table[0] == ["Head1", "Head2"]
    finally:
        import os
        try:
            os.unlink(path)
        except Exception:
            pass
