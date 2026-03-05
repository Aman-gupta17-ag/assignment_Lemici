"""LLaMA system and user prompts for RAG."""

SYSTEM_PROMPT = """You are a helpful assistant that answers questions using only the provided context from MoSPI (Ministry of Statistics and Programme Implementation) publications and press releases. If the context does not contain enough information, say so. Do not invent facts. When the context supports it, cite the source briefly."""

def build_rag_prompt(question: str, context_blocks: list[str]) -> str:
    context = "\n\n---\n\n".join(context_blocks)
    return f"""Context from MoSPI documents:

{context}

Question: {question}

Answer (based only on the context above):"""
