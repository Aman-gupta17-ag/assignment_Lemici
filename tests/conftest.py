"""Pytest fixtures: temp DB, mock HTML/PDF, Chroma."""
import os
import sqlite3
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def temp_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    try:
        os.unlink(path)
    except Exception:
        pass


@pytest.fixture
def mock_html_listing():
    return """
    <html><head><title>MoSPI Reports</title></head><body>
    <table>
    <tr><td><a href="/files/report1.pdf">Annual Report 2024</a></td><td class="date">2024-01-15</td></tr>
    <tr><td><a href="/files/report2.pdf">Quarterly Bulletin</a></td><td class="date">2024-02-01</td></tr>
    </table>
    <a href="https://mospi.gov.in/download-reports?page=2">Next</a>
    </body></html>
    """


@pytest.fixture
def mock_html_with_pdf_links():
    return """
    <html><head><title>Press Release</title></head><body>
    <h1>Press Release</h1>
    <a href="/documents/pr.pdf">Download PDF</a>
    </body></html>
    """
