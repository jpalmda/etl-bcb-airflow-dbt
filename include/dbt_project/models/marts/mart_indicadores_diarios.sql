select
    data_referencia,
    max(case when serie_nome = 'selic_meta_anual' then valor end) as selic,
    max(case when serie_nome = 'ipca_mensal' then valor end) as ipca,
    max(case when serie_nome = 'dolar_comercial_venda' then valor end) as dolar
from {{ ref('stg_bcb_series') }}
group by data_referencia
order by data_referencia desc