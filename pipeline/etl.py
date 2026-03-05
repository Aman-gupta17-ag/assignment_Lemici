"""
ETL: SQLite -> chunk (1000 tokens, 20% overlap) -> embed -> Chroma.
CLI: python -m pipeline.etl
"""
import os
import sqlite3

import chromadb
from chromadb.utils import embedding_functions
DB_PATH = os.environ.get("DB_PATH", "data/raw/mospi.db")
VECTOR_PATH = os.environ.get("VECTOR_PATH", "data/processed/chroma")
EMBED_MODEL = os.environ.get("EMBED_MODEL", "all-MiniLM-L6-v2")
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", "1000"))  # token approx
CHUNK_OVERLAP = int(os.environ.get("CHUNK_OVERLAP", "200"))

# Approx 4 chars per token for English
CHAR_CHUNK = CHUNK_SIZE * 4
CHAR_OVERLAP = CHUNK_OVERLAP * 4


def chunk_text(text: str, size: int = CHAR_CHUNK, overlap: int = CHAR_OVERLAP) -> list[str]:
    if not text or not text.strip():
        return []
    chunks = []
    start = 0
    while start < len(text):
        end = start + size
        chunk = text[start:end]
        if chunk.strip():
            chunks.append(chunk.strip())
        start = end - overlap
        if start >= len(text):
            break
    return chunks


def build_document_text(conn: sqlite3.Connection) -> list[tuple[str, str, str, str]]:
    """Returns list of (doc_id, title, url, full_text)."""
    cur = conn.execute(
        """SELECT d.id, d.title, d.url,
                  COALESCE(d.summary, '') || ' ' ||
                  (SELECT GROUP_CONCAT(COALESCE(f.content_text, ''), ' ')
                   FROM files f WHERE f.document_id = d.id)
           FROM documents d"""
    )
    rows = cur.fetchall()
    return [(r[0], r[1], r[2], (r[3] or "").strip()) for r in rows]


def run_etl() -> None:
    os.makedirs(VECTOR_PATH, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    docs = build_document_text(conn)
    conn.close()

    all_chunks = []
    for doc_id, title, url, text in docs:
        if not text:
            continue
        for i, c in enumerate(chunk_text(text)):
            chunk_id = f"{doc_id}_{i}"
            all_chunks.append({"id": chunk_id, "text": c, "doc_id": doc_id, "title": title, "url": url})

    if not all_chunks:
        print("No text chunks; run crawl and parse first.")
        return

    # Embed and index
    ef = embedding_functions.SentenceTransformerEmbeddingFunction(model_name=EMBED_MODEL)
    client = chromadb.PersistentClient(path=VECTOR_PATH)
    try:
        client.delete_collection("mospi")
    except Exception:
        pass
    coll = client.create_collection("mospi", embedding_function=ef, metadata={"description": "MoSPI RAG"})

    ids = [c["id"] for c in all_chunks]
    documents = [c["text"] for c in all_chunks]
    metadatas = [{"doc_id": c["doc_id"], "title": c["title"], "url": c["url"]} for c in all_chunks]
    coll.add(ids=ids, documents=documents, metadatas=metadatas)
    print(f"Indexed {len(all_chunks)} chunks in {VECTOR_PATH}")


def main() -> None:
    run_etl()


if __name__ == "__main__":
    main()
