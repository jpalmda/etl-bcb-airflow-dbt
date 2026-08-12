"""
DAG: extract_bcb_series
Extrai diariamente séries temporais públicas do Banco Central (API SGS)
e carrega de forma incremental na camada raw do data warehouse.

Fonte: https://dadosabertos.bcb.gov.br/dataset/20542-selic
API sem necessidade de chave: https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados
"""
from __future__ import annotations

import os
from datetime import datetime

import pendulum
import psycopg2
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

# Séries do SGS que queremos acompanhar: código -> nome amigável
SERIES = {
    11: "selic_diaria",
    433: "ipca_mensal",
    1: "dolar_comercial_venda",
}

DW_CONN_PARAMS = {
    "host": os.environ.get("DW_HOST", "dw-postgres"),
    "port": os.environ.get("DW_PORT", "5432"),
    "dbname": os.environ.get("DW_DB", "datawarehouse"),
    "user": os.environ.get("DW_USER", "dw_user"),
    "password": os.environ.get("DW_PASSWORD", "dw_password"),
}

CREATE_TABLE_SQL_PATH = "/opt/airflow/include/sql/create_raw_bcb_series.sql"

UPSERT_SQL = """
    INSERT INTO raw.bcb_series (serie_codigo, serie_nome, data_referencia, valor)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (serie_codigo, data_referencia)
    DO UPDATE SET valor = EXCLUDED.valor, carregado_em = now();
"""


@dag(
    dag_id="extract_bcb_series",
    description="Extração incremental de séries do Banco Central (SGS)",
    schedule="0 9 * * *",  # todo dia às 9h
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["bcb", "extracao", "raw"],
)
def extract_bcb_series():

    @task
    def ensure_raw_table_exists() -> None:
        """Garante que o schema/tabela raw existam antes de carregar."""
        with open(CREATE_TABLE_SQL_PATH, "r") as f:
            ddl = f.read()
        with psycopg2.connect(**DW_CONN_PARAMS) as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
            conn.commit()

    @task
    def extract_series(serie_codigo: int, data_inicial: str, data_final: str) -> list[dict]:
        """Busca uma série no período [data_inicial, data_final] (formato dd/mm/yyyy)."""
        url = (
            f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{serie_codigo}/dados"
            f"?formato=json&dataInicial={data_inicial}&dataFinal={data_final}"
        )
        response = requests.get(url, timeout=30)

        # A API do BCB retorna 404 (em vez de lista vazia) quando não há
        # nenhum dado no período pedido. Isso é comum em séries de baixa
        # frequência (ex: IPCA é mensal) numa janela incremental de 1-2 dias.
        if response.status_code == 404:
            return []

        response.raise_for_status()
        registros = response.json()

        return [
            {
                "serie_codigo": serie_codigo,
                "serie_nome": SERIES[serie_codigo],
                "data_referencia": datetime.strptime(r["data"], "%d/%m/%Y").date().isoformat(),
                "valor": float(r["valor"]),
            }
            for r in registros
        ]

    @task
    def load_series(registros: list[dict]) -> int:
        """Faz upsert dos registros extraídos na tabela raw.bcb_series."""
        if not registros:
            return 0

        with psycopg2.connect(**DW_CONN_PARAMS) as conn:
            with conn.cursor() as cur:
                for r in registros:
                    cur.executemany(
                        UPSERT_SQL,
                        [(r["serie_codigo"], r["serie_nome"], r["data_referencia"], r["valor"])],
                    )
            conn.commit()
        return len(registros)

    @task
    def check_data_quality(linhas_carregadas: list[int]) -> None:
        """Falha a DAG se nenhuma linha foi carregada em nenhuma série (sinal de API fora do ar ou schema mudou)."""
        total = sum(linhas_carregadas)
        if total == 0:
            raise AirflowFailException(
                "Nenhum registro foi carregado em nenhuma série. "
                "Verifique se a API do BCB está disponível ou se o período consultado está correto."
            )

    # Janela incremental: usa o intervalo de execução do Airflow (data_interval)
    data_inicial = "{{ data_interval_start.strftime('%d/%m/%Y') }}"
    data_final = "{{ data_interval_end.strftime('%d/%m/%Y') }}"

    tabela_pronta = ensure_raw_table_exists()

    linhas_carregadas = []
    for codigo in SERIES:
        registros = extract_series.override(task_id=f"extract_{SERIES[codigo]}")(
            serie_codigo=codigo, data_inicial=data_inicial, data_final=data_final
        )
        linhas = load_series.override(task_id=f"load_{SERIES[codigo]}")(registros)
        tabela_pronta >> registros
        linhas_carregadas.append(linhas)

    check_data_quality(linhas_carregadas)


extract_bcb_series()