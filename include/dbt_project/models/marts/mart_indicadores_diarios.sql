-- Mart final: um indicador por coluna, uma linha por data.
-- Formato "largo" (wide), ideal para consumo direto por dashboards.
select
    data_referencia,
    max(case when serie_nome = 'selic_diaria' then valor end) as selic,
    max(case when serie_nome = 'ipca_mensal' then valor end) as ipca,
    max(case when serie_nome = 'dolar_comercial_venda' then valor end) as dolar
from {{ ref('stg_bcb_series') }}
group by data_referencia
order by data_referencia desc