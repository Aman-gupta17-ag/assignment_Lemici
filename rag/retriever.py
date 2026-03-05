"""
Chroma retriever with MMR-style diversity (fetch more, then take top-k by relevance).
"""
import os
from typing import Any

import chromadb
from chromadb.utils import embedding_functions

VECTOR_PATH = os.environ.get("VECTOR_PATH", "data/processed/chroma")
EMBED_MODEL = os.environ.get("EMBED_MODEL", "all-MiniLM-L6-v2")
TOP_K_DEFAULT = int(os.environ.get("TOP_K", "5"))


def get_collection() -> Any:
    ef = embedding_functions.SentenceTransformerEmbeddingFunction(model_name=EMBED_MODEL)
    client = chromadb.PersistentClient(path=VECTOR_PATH)
    return client.get_collection("mospi", embedding_function=ef)


def retrieve(question: str, k: int = TOP_K_DEFAULT, fetch_k_mult: int = 3) -> list[dict]:
    """
    Retrieve top-k chunks for question. Uses Chroma query; optionally fetch_k_mult * k
    candidates for diversity (MMR-style: we take top-k by score).
    """
    coll = get_collection()
    fetch_k = max(k * fetch_k_mult, k)
    result = coll.query(
        query_texts=[question],
        n_results=min(fetch_k, 100),
        include=["documents", "metadatas", "distances"],
    )
    if not result["ids"] or not result["ids"][0]:
        return []
    ids = result["ids"][0]
    docs = result["documents"][0]
    metadatas = result["metadatas"][0]
    distances = result["distances"][0] if result.get("distances") else [0.0] * len(ids)
    # Chroma returns sorted by distance (lower = more similar). Take first k.
    out = []
    for i in range(min(k, len(ids))):
        out.append({
            "id": ids[i],
            "text": docs[i] if i < len(docs) else "",
            "metadata": metadatas[i] if i < len(metadatas) else {},
            "distance": distances[i] if i < len(distances) else None,
        })
    return out
