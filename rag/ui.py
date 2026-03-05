"""
Streamlit chat UI with citations (clickable source links).
"""
import os

import requests
import streamlit as st

API_URL = os.environ.get("API_URL", "http://localhost:8000")

st.set_page_config(page_title="MoSPI RAG Chat", layout="wide")
st.title("MoSPI RAG Chat")
st.caption("Ask questions about MoSPI publications and press releases. Answers include source links.")

if "messages" not in st.session_state:
    st.session_state.messages = []

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        if msg.get("citations"):
            with st.expander("Sources"):
                for c in msg["citations"]:
                    st.markdown(f"- [{c.get('title', 'Source')}]({c.get('url', '#')})")

if prompt := st.chat_input("Ask a question about MoSPI data..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        try:
            r = requests.post(
                f"{API_URL}/ask",
                json={"question": prompt, "k": 5},
                timeout=60,
            )
            r.raise_for_status()
            data = r.json()
            answer = data.get("answer", "")
            citations = data.get("citations", [])
            st.markdown(answer)
            if citations:
                with st.expander("Sources"):
                    for c in citations:
                        st.markdown(f"- [{c.get('title', 'Source')}]({c.get('url', '#')})")
            st.session_state.messages.append({
                "role": "assistant",
                "content": answer,
                "citations": citations,
            })
        except requests.exceptions.RequestException as e:
            st.error(f"API error: {e}. Is the API running at {API_URL}?")
            st.session_state.messages.append({"role": "assistant", "content": str(e), "citations": []})
