"""
Print a short report of crawled documents and files.
CLI: python -m scraper.report
"""
import os
import sqlite3

DB_PATH = os.environ.get("DB_PATH", "data/raw/mospi.db")


def report() -> None:
    if not os.path.isfile(DB_PATH):
        print(f"No database at {DB_PATH}. Run crawl first.")
        return
    conn = sqlite3.connect(DB_PATH)
    docs = conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
    files = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
    tables = conn.execute("SELECT COUNT(*) FROM tables").fetchone()[0]
    parsed = conn.execute("SELECT COUNT(*) FROM files WHERE file_path IS NOT NULL AND file_path != ''").fetchone()[0]
    conn.close()
    print(f"Documents: {docs}")
    print(f"Files: {files} (parsed: {parsed})")
    print(f"Tables: {tables}")


def main() -> None:
    report()


if __name__ == "__main__":
    main()
