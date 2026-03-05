"""End-to-end mock: HTML + PDF -> DB -> ETL -> Chroma query."""
import json
import os
import sqlite3
import tempfile
from unittest.mock import patch

import pytest

# Ensure we use temp paths
@pytest.fixture
def temp_dirs():
    t = tempfile.mkdtemp()
    db_path = os.path.join(t, "raw", "mospi.db")
    pdf_dir = os.path.join(t, "raw", "pdf")
    chroma_path = os.path.join(t, "processed", "chroma")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    os.makedirs(pdf_dir, exist_ok=True)
    os.makedirs(chroma_path, exist_ok=True)
    return {"db": db_path, "pdf": pdf_dir, "chroma": chroma_path}


def test_integration_mock_db_to_etl(temp_dirs):
    """Create mock DB with one document + file content -> run ETL -> Chroma has chunks."""
    db_path = temp_dirs["db"]
    chroma_path = temp_dirs["chroma"]
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE documents (id TEXT PRIMARY KEY, title TEXT, url TEXT, date_published TEXT, summary TEXT, category TEXT, hash TEXT, created_at TEXT);
        CREATE TABLE files (id TEXT PRIMARY KEY, document_id TEXT, file_url TEXT, file_path TEXT, file_type TEXT, pages INTEGER, content_text TEXT);
        CREATE TABLE tables (id TEXT PRIMARY KEY, document_id TEXT, source_file_id TEXT, table_json TEXT, n_rows INTEGER, n_cols INTEGER);
        INSERT INTO documents VALUES ('doc1', 'Test Report', 'https://example.com/1', '2024-01-01', 'Summary here.', 'publications', 'h1', '2024-01-01');
        INSERT INTO files VALUES ('f1', 'doc1', 'https://example.com/1.pdf', '/tmp/1.pdf', 'pdf', 2, 'This is the full text content of the MoSPI report about GDP growth in India.');
    """)
    conn.commit()
    conn.close()

    with patch.dict(os.environ, {"DB_PATH": db_path, "VECTOR_PATH": chroma_path}):
        from pipeline.etl import run_etl

        run_etl()

    import chromadb
    from chromadb.utils import embedding_functions

    ef = embedding_functions.SentenceTransformerEmbeddingFunction(model_name="all-MiniLM-L6-v2")
    client = chromadb.PersistentClient(path=chroma_path)
    coll = client.get_collection("mospi", embedding_function=ef)
    n = coll.count()
    assert n >= 1
    result = coll.query(query_texts=["GDP growth"], n_results=2, include=["documents", "metadatas"])
    assert result["ids"] and result["ids"][0]
    assert any("GDP" in (d or "") for d in result["documents"][0])
