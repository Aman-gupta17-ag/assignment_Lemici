"""
Parse downloaded PDFs: extract text + first table, update files and tables.
CLI: python -m scraper.parse
"""
import hashlib
import json
import os
import sqlite3
import time
from urllib.parse import urlparse

import pdfplumber
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

DB_PATH = os.environ.get("DB_PATH", "data/raw/mospi.db")
PDF_DIR = os.path.join(os.path.dirname(DB_PATH), "pdf")
DELAY_SEC = float(os.environ.get("DELAY_SEC", "1.0"))
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) MoSPI-Scraper/1.0 (educational)"


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
def download_pdf(session: requests.Session, url: str, out_path: str) -> None:
    r = session.get(url, timeout=30)
    r.raise_for_status()
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "wb") as f:
        f.write(r.content)


def table_id_for_file(source_file_id: str) -> str:
    return hashlib.sha256(source_file_id.encode()).hexdigest()[:32]


def parse_pdf(file_path: str) -> tuple[str, list[list[str]] | None, int]:
    """Extract full text and first table from PDF. Returns (text, first_table, num_pages)."""
    text_parts = []
    first_table = None
    n_pages = 0
    with pdfplumber.open(file_path) as pdf:
        n_pages = len(pdf.pages)
        for page in pdf.pages:
            t = page.extract_text()
            if t:
                text_parts.append(t)
            if first_table is None and page.extract_tables():
                first_table = page.extract_tables()[0] or []
                # Normalize to list of list of str
                first_table = [[str(cell) if cell is not None else "" for cell in row] for row in first_table]
    full_text = "\n\n".join(text_parts)
    return full_text, first_table, n_pages


def run_parse() -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.execute(
        "SELECT id, document_id, file_url FROM files WHERE file_type = 'pdf' AND (file_path IS NULL OR file_path = '')"
    )
    rows = cur.fetchall()
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    for file_id, document_id, file_url in rows:
        try:
            name = urlparse(file_url).path.split("/")[-1] or "doc.pdf"
            if not name.lower().endswith(".pdf"):
                name += ".pdf"
            safe_name = "".join(c if c.isalnum() or c in "._-" else "_" for c in name)[:200]
            file_path = os.path.join(PDF_DIR, f"{file_id}_{safe_name}")

            download_pdf(session, file_url, file_path)
            text, table_data, pages = parse_pdf(file_path)

            conn.execute(
                "UPDATE files SET file_path = ?, pages = ?, content_text = ? WHERE id = ?",
                (file_path, pages, text[:500000] if text else None, file_id),
            )

            if table_data:
                table_id = table_id_for_file(file_id)
                n_rows, n_cols = len(table_data), max(len(r) for r in table_data) if table_data else 0
                conn.execute(
                    """INSERT OR REPLACE INTO tables (id, document_id, source_file_id, table_json, n_rows, n_cols)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (table_id, document_id, file_id, json.dumps(table_data), n_rows, n_cols),
                )
            conn.commit()
            time.sleep(DELAY_SEC)
        except Exception as e:
            print(f"Parse error for {file_url}: {e}")
    conn.close()


def main() -> None:
    run_parse()


if __name__ == "__main__":
    main()
