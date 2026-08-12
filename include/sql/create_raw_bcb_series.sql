-- Schema e tabela raw para as séries temporais do Banco Central (SGS)
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.bcb_series (
    serie_codigo     INTEGER      NOT NULL,
    serie_nome       TEXT         NOT NULL,
    data_referencia  DATE         NOT NULL,
    valor            NUMERIC(18,4) NOT NULL,
    carregado_em     TIMESTAMP    NOT NULL DEFAULT now(),
    PRIMARY KEY (serie_codigo, data_referencia)
);

CREATE INDEX IF NOT EXISTS idx_bcb_series_data
    ON raw.bcb_series (data_referencia);