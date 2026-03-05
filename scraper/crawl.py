"""
MoSPI listing crawler. Discovers documents and PDF links; stores in SQLite.
CLI: python -m scraper.crawl --seed-url <url> --max-pages 5
"""
import argparse
import hashlib
import json
import os
import sqlite3
import time
from datetime import datetime
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential

from scraper.models import Document, File

# Defaults; override via env or CLI
DB_PATH = os.environ.get("DB_PATH", "data/raw/mospi.db")
SEED_URLS_DEFAULT = "https://mospi.gov.in/download-reports?main_cat=NzI2,https://mospi.gov.in/press-releases"
DELAY_SEC = float(os.environ.get("DELAY_SEC", "1.0"))

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) MoSPI-Scraper/1.0 (educational)"


def get_seed_urls() -> list[str]:
    raw = os.environ.get("SEED_URLS", SEED_URLS_DEFAULT)
    return [u.strip() for u in raw.split(",") if u.strip()]


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS documents (
            id TEXT PRIMARY KEY,
            title TEXT,
            url TEXT,
            date_published TEXT,
            summary TEXT,
            category TEXT,
            hash TEXT,
            created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS files (
            id TEXT PRIMARY KEY,
            document_id TEXT,
            file_url TEXT,
            file_path TEXT,
            file_type TEXT,
            pages INTEGER DEFAULT 0,
            content_text TEXT,
            FOREIGN KEY (document_id) REFERENCES documents(id)
        );
        CREATE TABLE IF NOT EXISTS tables (
            id TEXT PRIMARY KEY,
            document_id TEXT,
            source_file_id TEXT,
            table_json TEXT,
            n_rows INTEGER,
            n_cols INTEGER,
            FOREIGN KEY (document_id) REFERENCES documents(id),
            FOREIGN KEY (source_file_id) REFERENCES files(id)
        );
    """)
    conn.commit()


def content_hash(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def doc_id_from_url(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:32]


def file_id_from_url(document_id: str, file_url: str) -> str:
    return hashlib.sha256((document_id + file_url).encode()).hexdigest()[:32]


def is_doc_url_crawled(conn: sqlite3.Connection, url: str) -> bool:
    cur = conn.execute("SELECT 1 FROM documents WHERE url = ?", (url,))
    return cur.fetchone() is not None


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
def fetch(session: requests.Session, url: str) -> requests.Response:
    r = session.get(url, timeout=15)
    r.raise_for_status()
    return r


def extract_listing(soup: BeautifulSoup, base_url: str) -> list[dict]:
    """Extract listing items: title, date, PDF links from MoSPI-style pages."""
    items = []
    # MoSPI often uses tables or divs for listings; look for links to PDFs and parent rows/cells for title/date
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        full_url = urljoin(base_url, href)
        if ".pdf" in href.lower() or "download" in href.lower():
            # Try to get title from link text or parent row
            title = (a.get_text(strip=True) or "Untitled").strip() or "Untitled"
            parent = a.find_parent("tr") or a.find_parent("div") or a.find_parent("li")
            if parent:
                # Prefer date from sibling or parent text
                date_el = parent.find(class_=lambda c: c and "date" in str(c).lower()) or parent
                date_text = date_el.get_text(strip=True) if date_el else ""
            else:
                date_text = ""
            items.append({
                "title": title[:500],
                "date_published": date_text[:100] or datetime.utcnow().strftime("%Y-%m-%d"),
                "url": full_url,
                "pdf_url": full_url if ".pdf" in href.lower() else None,
            })
        # Also capture detail page links that might lead to PDFs (e.g. /download-reports/...)
        if "/download-reports" in href or "/press-releases" in href or "/report" in href.lower():
            if not any(x["url"] == full_url for x in items):
                items.append({
                    "title": (a.get_text(strip=True) or "Untitled")[:500],
                    "date_published": datetime.utcnow().strftime("%Y-%m-%d"),
                    "url": full_url,
                    "pdf_url": None,
                })
    return items


def crawl(seed_urls: list[str], max_pages: int = 5) -> None:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    visited: set[str] = set()
    queue: list[str] = list(seed_urls)

    while queue and max_pages > 0:
        url = queue.pop(0)
        if url in visited or is_doc_url_crawled(conn, url):
            continue
        visited.add(url)
        try:
            resp = fetch(session, url)
            body = resp.content
            h = content_hash(body)
            soup = BeautifulSoup(body, "html.parser")
            page_title = (soup.title.string if soup.title else "" or "Unknown").strip()
            meta_desc = soup.find("meta", {"name": "description"})
            summary = (meta_desc.get("content", "") if meta_desc else "")[:2000]
            category = "press-releases" if "press-releases" in url else "publications"

            # If this page is a listing, extract items and add as documents + queue detail links
            items = extract_listing(soup, url)
            if items:
                for item in items:
                    doc_url = item["url"]
                    if is_doc_url_crawled(conn, doc_url):
                        continue
                    doc_id = doc_id_from_url(doc_url)
                    now = datetime.utcnow().isoformat()
                    conn.execute(
                        """INSERT OR REPLACE INTO documents
                           (id, title, url, date_published, summary, category, hash, created_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            doc_id,
                            item["title"],
                            doc_url,
                            item["date_published"],
                            summary or item["title"],
                            category,
                            h,
                            now,
                        ),
                    )
                    if item.get("pdf_url"):
                        file_id = file_id_from_url(doc_id, item["pdf_url"])
                        conn.execute(
                            """INSERT OR REPLACE INTO files (id, document_id, file_url, file_path, file_type, pages)
                               VALUES (?, ?, ?, '', 'pdf', 0)""",
                            (file_id, doc_id, item["pdf_url"]),
                        )
                    # Queue non-PDF links for further crawling
                    if not item.get("pdf_url") and doc_url not in visited:
                        queue.append(doc_url)
            else:
                # Single page as document
                doc_id = doc_id_from_url(url)
                now = datetime.utcnow().isoformat()
                conn.execute(
                    """INSERT OR REPLACE INTO documents
                       (id, title, url, date_published, summary, category, hash, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (doc_id, page_title, url, now[:19], summary, category, h, now),
                )
                # Find PDF links on this page
                for a in soup.find_all("a", href=True):
                    href = a.get("href", "")
                    if ".pdf" in href.lower():
                        pdf_url = urljoin(url, href)
                        file_id = file_id_from_url(doc_id, pdf_url)
                        conn.execute(
                            """INSERT OR REPLACE INTO files (id, document_id, file_url, file_path, file_type, pages)
                               VALUES (?, ?, ?, '', 'pdf', 0)""",
                            (file_id, doc_id, pdf_url),
                        )

            conn.commit()
            max_pages -= 1
            time.sleep(DELAY_SEC)
        except Exception as e:
            print(f"Error crawling {url}: {e}")
    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="MoSPI listing crawler")
    parser.add_argument("--seed-url", action="append", dest="seed_urls", help="Seed URL (repeatable)")
    parser.add_argument("--max-pages", type=int, default=None, help="Max pages (default from env MAX_PAGES or 5)")
    args = parser.parse_args()
    seeds = args.seed_urls if args.seed_urls else get_seed_urls()
    max_pages = args.max_pages if args.max_pages is not None else int(os.environ.get("MAX_PAGES", "5"))
    crawl(seeds, max_pages=max_pages)


if __name__ == "__main__":
    main()
