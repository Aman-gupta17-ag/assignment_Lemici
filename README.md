# MoSPI Scraper + LLaMA RAG

Incremental MoSPI scraper → ETL → LLaMA-3 RAG chatbot. Dockerized, SQLite + Chroma, Streamlit UI with citations.

## Tech stack

- **Python 3.11**
- **Core:** requests, beautifulsoup4, pdfplumber, pandas, chromadb, sentence-transformers
- **API/UI:** FastAPI, uvicorn, streamlit, ollama
- **Dev:** pytest, mypy, black, python-dotenv, tenacity
- **Storage:** sqlite3, pyarrow (parquet)

## Repo structure

```
.
├── README.md
├── requirements.txt
├── .env.example
├── Makefile
├── docker-compose.yml
├── data/
│   ├── raw/ (pdf/, mospi.db)
│   └── processed/ (chroma)
├── scraper/ (crawl, parse, models, report)
├── pipeline/ (etl, validate)
├── rag/ (api, retriever, prompts, ui)
├── tests/
└── infra/ (api/, ui/ Dockerfiles)
```

## Quick start

1. **Copy env and install**
   ```bash
   copy .env.example .env
   pip install -r requirements.txt
   ```

2. **Crawl and ETL (local)**
   ```bash
   make crawl
   python -m scraper.parse
   make report
   make etl
   ```

3. **Run with Docker**
   ```bash
   docker compose up
   ```
   - Chat UI: **http://localhost:8501**
   - API: http://localhost:8000 (docs at /docs)

4. **Ollama**
   - Start Ollama locally and run `ollama pull llama3`, or use the `ollama` service in docker-compose (then pull inside the container).

## CLI commands

| Command | Description |
|--------|-------------|
| `python -m scraper.crawl --seed-url <url> --max-pages 5` | Crawl MoSPI listing pages |
| `python -m scraper.parse` | Download PDFs and extract text + first table |
| `python -m scraper.report` | Print document/file/table counts |
| `python -m pipeline.etl` | Chunk, embed, and build Chroma index |

## API

- **POST /ask** — `{ "question": "…", "k": 3 }` → `{ "answer": "…", "citations": [{ "title", "url" }] }`
- **POST /ingest** — Rebuild Chroma index from SQLite
- **GET /health** — `{ "status": "ok" }`

## Tests

```bash
make test
# or
pytest tests/ scraper/test_crawl.py pipeline/test_etl.py -v
```

- **test_crawl.py:** Mock HTML → document extraction
- **test_pdf_parse.py:** Mock PDF → text + table
- **test_integration.py:** Mock DB → ETL → Chroma query
- **test_etl.py:** Chunking and `build_document_text`

## Success criteria

- `docker compose up` → Chat UI at **localhost:8501**
- Scrape 10+ MoSPI PDFs with tables (run crawl/parse with enough pages/links)
- RAG answers with clickable source links (citations in UI)
- `make crawl && make etl && pytest` passes
- README explains trade-offs (below)

## Trade-offs

1. **Crawling**
   - MoSPI may block or throttle non-browser requests; we use a polite User-Agent and `DELAY_SEC`. For stricter sites, consider Playwright (JS rendering) or official APIs if available.
   - Listing selectors are best-effort (links with `.pdf`, `/download-reports`, `/press-releases`). If the site HTML changes, adjust `extract_listing()` in `scraper/crawl.py`.

2. **Chunking**
   - Chunk size is approximated as 4 chars ≈ 1 token (no tokenizer). For strict 1000-token chunks, integrate `tiktoken` or the embedding model’s tokenizer and chunk by token count.

3. **Retrieval**
   - We use Chroma’s top-k by similarity. Full MMR (maximal marginal relevance) would require fetching more candidates and reranking for diversity (e.g. in `rag/retriever.py`).

4. **LLM**
   - LLaMA via Ollama runs locally. For production, swap to a hosted API and set `OLLAMA_HOST` or equivalent in the API.

5. **Storage**
   - SQLite is single-writer. For concurrent writes, use a proper DB and consider moving file content to object storage with references in the DB.

6. **Docker**
   - API and UI images embed the full repo. For smaller images, use multi-stage builds and copy only needed modules.
