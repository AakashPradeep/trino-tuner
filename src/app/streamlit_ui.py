from __future__ import annotations

import requests
import streamlit as st

from core.settings import Settings

st.set_page_config(page_title="Trino SQL Optimizer", layout="wide")
st.title("Trino SQL Optimizer (LLM + EXPLAIN validation)")

s = Settings()
default_service_url = "http://localhost:8080"

service_url = st.text_input("Optimizer service URL", value=default_service_url)

sql = st.text_area(
    "Paste Trino SQL",
    height=220,
    placeholder="SELECT ... FROM ... WHERE ...",
)

col1, col2 = st.columns([1, 2])
with col1:
    run = st.button("Optimize", type="primary")

if run:
    if not sql.strip():
        st.error("Please paste a SQL query.")
    else:
        with st.spinner("Optimizing (EXPLAIN → metadata → LLM → EXPLAIN validate)..."):
            resp = requests.post(f"{service_url}/optimize", json={"sql": sql}, timeout=300)
            data = resp.json()

        if not data.get("ok"):
            st.error(f"Failed: {data.get('error')}")
        else:
            st.success(f"Optimized in {data.get('attempts')} attempt(s). Risk: {data.get('llm', {}).get('risk')}")

        st.subheader("Diff")
        st.code(data.get("diff", ""), language="diff")

        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Original SQL")
            st.code(data.get("original_sql", ""), language="sql")
        with c2:
            st.subheader("Optimized SQL")
            st.code(data.get("optimized_sql", "") or "", language="sql")

        with st.expander("EXPLAIN (before)", expanded=False):
            st.code(data.get("explain_before", {}).get("text", ""), language="text")

        with st.expander("EXPLAIN (after)", expanded=False):
            after = data.get("explain_after") or {}
            st.code(after.get("text", ""), language="text")

        with st.expander("Tables + Metadata", expanded=False):
            st.json({"tables": data.get("tables", []), "metadata": data.get("metadata", [])})

        with st.expander("LLM changes/assumptions", expanded=False):
            st.json(data.get("llm", {}))
