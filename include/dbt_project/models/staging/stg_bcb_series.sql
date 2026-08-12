-- Camada staging: renomeia/tipa os dados brutos, sem lógica de negócio ainda
select
    serie_codigo,
    serie_nome,
    data_referencia::date as data_referencia,
    valor::numeric(18, 4) as valor,
    carregado_em
from {{ source('raw', 'bcb_series') }}