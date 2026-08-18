"""
Dashboard dos indicadores econômicos do Banco Central.
Lê direto da camada marts do data warehouse (alimentada pela pipeline
Airflow + dbt) e exibe a evolução de SELIC, IPCA e dólar.
"""
import os

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

st.set_page_config(page_title="Indicadores BCB", page_icon="📊", layout="wide")


@st.cache_resource
def get_engine():
    host = os.environ.get("DW_HOST", "dw-postgres")
    port = os.environ.get("DW_PORT", "5432")
    db = os.environ.get("DW_DB", "datawarehouse")
    user = os.environ.get("DW_USER", "dw_user")
    password = os.environ.get("DW_PASSWORD", "dw_password")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)


@st.cache_data(ttl=300)
def load_data() -> pd.DataFrame:
    engine = get_engine()
    query = """
        select data_referencia, selic, ipca, dolar
        from staging_marts.mart_indicadores_diarios
        order by data_referencia
    """
    df = pd.read_sql(query, engine)
    df["data_referencia"] = pd.to_datetime(df["data_referencia"]).dt.date
    return df


st.title("📊 Indicadores Econômicos — Banco Central")
st.caption(
    "Dados extraídos automaticamente da API do BCB via Airflow, "
    "transformados com dbt. Atualiza diariamente."
)

try:
    df = load_data()
except Exception as e:
    st.error(f"Não foi possível conectar ao data warehouse: {e}")
    st.stop()

if df.empty:
    st.warning("Ainda não há dados carregados. Rode a DAG no Airflow primeiro.")
    st.stop()

col1, col2, col3 = st.columns(3)
ultima_linha = df.dropna(subset=["selic"]).iloc[-1] if not df["selic"].dropna().empty else None
ultima_ipca = df.dropna(subset=["ipca"]).iloc[-1] if not df["ipca"].dropna().empty else None
ultima_dolar = df.dropna(subset=["dolar"]).iloc[-1] if not df["dolar"].dropna().empty else None

with col1:
    if ultima_linha is not None:
        st.metric("SELIC Meta (anualizada)", f"{ultima_linha['selic']:.2f}%")
with col2:
    if ultima_ipca is not None:
        st.metric("IPCA (último mês divulgado)", f"{ultima_ipca['ipca']:.2f}%")
    else:
        st.metric("IPCA (último mês divulgado)", "sem dado ainda")
with col3:
    if ultima_dolar is not None:
        st.metric("Dólar comercial (venda)", f"R$ {ultima_dolar['dolar']:.4f}")

st.divider()

st.subheader("Selic — evolução diária")
st.line_chart(df.set_index("data_referencia")["selic"])

st.subheader("Dólar comercial (venda) — evolução diária")
st.line_chart(df.set_index("data_referencia")["dolar"])

if df["ipca"].notna().any():
    st.subheader("IPCA — evolução mensal")
    st.line_chart(df.dropna(subset=["ipca"]).set_index("data_referencia")["ipca"])

st.divider()
st.subheader("Dados brutos")
st.dataframe(df.sort_values("data_referencia", ascending=False), use_container_width=True)