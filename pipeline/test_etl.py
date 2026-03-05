"""Test ETL chunking and DB read."""
import os
import sqlite3
import tempfile
from unittest.mock import patch

import pytest

from pipeline.etl import build_document_text, chunk_text


def test_chunk_text():
    text = "a" * 5000
    chunks = chunk_text(text, size=1000, overlap=200)
    assert len(chunks) >= 4
    assert sum(len(c) for c in chunks) >= len(text) - 1000


def test_build_document_text():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        conn = sqlite3.connect(path)
        conn.executescript("""
            CREATE TABLE documents (id TEXT, title TEXT, url TEXT, summary TEXT);
            CREATE TABLE files (id TEXT, document_id TEXT, content_text TEXT);
            INSERT INTO documents VALUES ('d1', 'T1', 'https://u1', 'Summary one');
            INSERT INTO files VALUES ('f1', 'd1', 'Content one from PDF.');
        """)
        conn.commit()
        docs = build_document_text(conn)
        conn.close()
        assert len(docs) == 1
        assert docs[0][0] == "d1"
        assert "Summary one" in docs[0][3]
        assert "Content one" in docs[0][3]
    finally:
        try:
            os.unlink(path)
        except Exception:
            pass
