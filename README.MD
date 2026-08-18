# ETL BCB — Airflow + dbt

Pipeline de dados ETL/ELT que extrai indicadores econômicos públicos do Banco Central do Brasil, orquestra a extração incremental com Apache Airflow, transforma os dados em camadas testadas com dbt, e expõe o resultado final em um dashboard interativo.

## Arquitetura

```
API do Banco Central (SGS)
        │
        ▼
   Airflow (extração incremental diária)
        │
        ▼
PostgreSQL — schema raw (dados brutos)
        │
        ▼
   dbt staging (view, testada)
        │
        ▼
   dbt marts (tabela final, testada)
        │
        ▼
   Dashboard Streamlit
```

Todo o ambiente roda em containers Docker, orquestrados via Docker Compose.

## Indicadores extraídos

| Indicador | Série SGS | Frequência |
|---|---|---|
| Meta Selic (anualizada) | 432 | Varia conforme reuniões do Copom |
| IPCA | 433 | Mensal |
| Dólar comercial (venda) | 1 | Diária |

Fonte: [API pública do SGS/BCB](https://dadosabertos.bcb.gov.br/), sem necessidade de autenticação.

## Stack

- **Orquestração:** Apache Airflow (TaskFlow API)
- **Transformação:** dbt (staging → marts, com testes de qualidade de dados)
- **Armazenamento:** PostgreSQL
- **Dashboard:** Streamlit
- **Containerização:** Docker / Docker Compose

## Destaques técnicos

- **Extração incremental de verdade**: cada execução da DAG busca apenas a janela de datas correspondente ao seu `data_interval`, não a série histórica inteira.
- **Tratamento de comportamento real de API**: a API do BCB retorna erro `404` (em vez de lista vazia) quando não há dado no período consultado — comum em séries de baixa frequência como o IPCA. Isso é tratado como ausência de dado, não como falha.
- **Idempotência**: a carga usa `UPSERT` (`ON CONFLICT DO UPDATE`), então reprocessar uma mesma data não gera duplicidade.
- **Qualidade de dados automatizada**: 10 testes dbt (`not_null`, `unique`, `accepted_values`) rodam a cada execução, tanto na fonte bruta quanto nos modelos transformados.
- **Pipeline fim-a-fim automatizada**: a DAG do Airflow dispara a extração, a carga, o `dbt run` e o `dbt test` em sequência — um único disparo atualiza toda a cadeia.

## Estrutura do projeto

```
.
├── docker-compose.yml
├── Dockerfile.airflow
├── requirements.txt
├── dags/
│   └── extract_bcb_series.py       # DAG principal
├── include/
│   ├── sql/
│   │   └── create_raw_bcb_series.sql
│   └── dbt_project/
│       ├── dbt_project.yml
│       ├── profiles.yml
│       └── models/
│           ├── staging/
│           │   ├── _staging__sources.yml
│           │   ├── _staging__models.yml
│           │   └── stg_bcb_series.sql
│           └── marts/
│               ├── _marts__models.yml
│               └── mart_indicadores_diarios.sql
└── dashboard/
    ├── app.py
    ├── requirements.txt
    └── Dockerfile
```

## Como rodar localmente

Pré-requisitos: Docker Desktop instalado e rodando.

```bash
# 1. Inicializar o banco de metadados do Airflow e criar o usuário admin
docker compose up airflow-init

# 2. Subir todos os serviços
docker compose up -d
```

Acessos:

| Serviço | URL | Login |
|---|---|---|
| Airflow | http://localhost:8081 | admin / admin |
| Dashboard | http://localhost:8502 | — |
| PostgreSQL (data warehouse) | localhost:5433 | dw_user / dw_password |

Depois de subir, ative a DAG `extract_bcb_series` na interface do Airflow e dispare uma execução manual para popular os dados pela primeira vez.

## Rodando dbt manualmente (fora da DAG)

```bash
docker compose exec airflow-scheduler bash -c "cd /opt/airflow/include/dbt_project && dbt run"
docker compose exec airflow-scheduler bash -c "cd /opt/airflow/include/dbt_project && dbt test"
```

## Possíveis melhorias futuras

- Adicionar mais séries econômicas (câmbio de outras moedas, IGP-M, etc.)
- Alertas automáticos (Slack/e-mail) em caso de falha na pipeline
- Deploy em ambiente cloud (ex: Airflow gerenciado + banco gerenciado)
- Versionamento de schema com migrations