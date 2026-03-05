"""Test crawl: mock HTML -> document extraction."""
import os
import sqlite3

import pytest
from bs4 import BeautifulSoup

# Import crawl module (init_db, extract_listing-style logic)
from scraper.crawl import (
    doc_id_from_url,
    extract_listing,
    file_id_from_url,
    init_db,
    is_doc_url_crawled,
)


def test_doc_id_from_url():
    id1 = doc_id_from_url("https://example.com/a")
    id2 = doc_id_from_url("https://example.com/a")
    assert id1 == id2
    assert len(id1) == 32


def test_file_id_from_url():
    id1 = file_id_from_url("doc1", "https://example.com/x.pdf")
    id2 = file_id_from_url("doc1", "https://example.com/x.pdf")
    assert id1 == id2


def test_init_db(temp_db):
    conn = sqlite3.connect(temp_db)
    init_db(conn)
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [r[0] for r in cur.fetchall()]
    conn.close()
    assert "documents" in tables
    assert "files" in tables
    assert "tables" in tables


def test_is_doc_url_crawled(temp_db):
    conn = sqlite3.connect(temp_db)
    init_db(conn)
    assert is_doc_url_crawled(conn, "http://x") is False
    conn.execute(
        "INSERT INTO documents (id, title, url, date_published, summary, category, hash, created_at) VALUES (?,?,?,?,?,?,?,?)",
        ("id1", "T", "http://x", "2024-01-01", "", "cat", "h", "2024-01-01"),
    )
    conn.commit()
    assert is_doc_url_crawled(conn, "http://x") is True
    conn.close()


def test_extract_listing(mock_html_listing):
    soup = BeautifulSoup(mock_html_listing, "html.parser")
    base = "https://mospi.gov.in"
    items = extract_listing(soup, base)
    # Should find PDF links and optionally detail links
    pdf_items = [i for i in items if i.get("pdf_url")]
    assert len(pdf_items) >= 2
    titles = [i["title"] for i in pdf_items]
    assert "Annual Report 2024" in titles or any("Annual" in t for t in titles)
