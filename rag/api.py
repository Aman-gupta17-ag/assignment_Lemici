"""
FastAPI RAG: /ask, /ingest, /health.
"""
import os

import ollama
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from rag.retriever import get_collection, retrieve

app = FastAPI(title="MoSPI RAG API")
OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
LLM_MODEL = os.environ.get("LLM_MODEL", "llama3")


class AskRequest(BaseModel):
    question: str
    k: int = 3


class AskResponse(BaseModel):
    answer: str
    citations: list[dict]  # [{title, url}]


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/ask", response_model=AskResponse)
def ask(req: AskRequest) -> AskResponse:
    chunks = retrieve(req.question, k=req.k)
    if not chunks:
        return AskResponse(answer="No relevant documents found. Run /ingest after crawling and ETL.", citations=[])

    context_blocks = [c["text"] for c in chunks]
    from rag.prompts import build_rag_prompt

    prompt = build_rag_prompt(req.question, context_blocks)
    try:
        client = ollama.Client(host=OLLAMA_HOST)
        response = client.chat(model=LLM_MODEL, messages=[{"role": "user", "content": prompt}])
        answer = response["message"]["content"]
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"LLM error: {e}")

    seen = set()
    citations = []
    for c in chunks:
        m = c.get("metadata") or {}
        url = m.get("url", "")
        title = m.get("title", "")
        key = (url, title)
        if key not in seen and (url or title):
            seen.add(key)
            citations.append({"title": title or "Source", "url": url})

    return AskResponse(answer=answer, citations=citations)


@app.post("/ingest")
def ingest() -> dict:
    """Rebuild Chroma index from SQLite (runs ETL)."""
    from pipeline.etl import run_etl

    try:
        run_etl()
        return {"status": "ok", "message": "Chroma index rebuilt"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
