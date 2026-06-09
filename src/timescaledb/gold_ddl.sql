CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.dim_symbol (
    symbol_id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL UNIQUE,
    base_asset TEXT,
    quote_asset TEXT,
    exchange TEXT DEFAULT 'Binance',
    market_type TEXT DEFAULT 'spot',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.dim_interval (
    interval_id BIGSERIAL PRIMARY KEY,
    interval_code TEXT NOT NULL UNIQUE,
    interval_seconds INT NOT NULL
);

CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_id DATE PRIMARY KEY,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    quarter INT NOT NULL,
    day_of_week INT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS gold.dim_data_source (
    data_source_id BIGSERIAL PRIMARY KEY,
    source_code TEXT NOT NULL UNIQUE,
    description TEXT
);

CREATE TABLE IF NOT EXISTS gold.dim_pattern_type (
    pattern_type_id BIGSERIAL PRIMARY KEY,
    pattern_type TEXT NOT NULL UNIQUE,
    description TEXT
);

CREATE TABLE IF NOT EXISTS gold.fact_kline (
    symbol_id BIGINT NOT NULL REFERENCES gold.dim_symbol(symbol_id),
    interval_id BIGINT NOT NULL REFERENCES gold.dim_interval(interval_id),
    date_id DATE NOT NULL REFERENCES gold.dim_date(date_id),

    open_time BIGINT NOT NULL,
    open_ts TIMESTAMPTZ NOT NULL,
    close_time BIGINT,
    close_ts TIMESTAMPTZ,

    open_price DOUBLE PRECISION,
    high_price DOUBLE PRECISION,
    low_price DOUBLE PRECISION,
    close_price DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    quote_asset_volume DOUBLE PRECISION,
    number_of_trades BIGINT,
    taker_buy_base_volume DOUBLE PRECISION,
    taker_buy_quote_volume DOUBLE PRECISION,

    data_source_id BIGINT REFERENCES gold.dim_data_source(data_source_id),
    is_final BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (symbol_id, interval_id, open_time)
);

CREATE INDEX IF NOT EXISTS idx_fact_kline_date
ON gold.fact_kline(date_id);

CREATE INDEX IF NOT EXISTS idx_fact_kline_symbol_time
ON gold.fact_kline(symbol_id, open_time DESC);

CREATE TABLE IF NOT EXISTS gold.fact_daily_market_summary (
    date_id DATE NOT NULL REFERENCES gold.dim_date(date_id),
    symbol_id BIGINT NOT NULL REFERENCES gold.dim_symbol(symbol_id),
    interval_id BIGINT NOT NULL REFERENCES gold.dim_interval(interval_id),

    open_price DOUBLE PRECISION,
    high_price DOUBLE PRECISION,
    low_price DOUBLE PRECISION,
    close_price DOUBLE PRECISION,
    total_volume DOUBLE PRECISION,
    price_change DOUBLE PRECISION,
    price_change_pct DOUBLE PRECISION,
    daily_range DOUBLE PRECISION,
    number_of_candles BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (date_id, symbol_id, interval_id)
);

CREATE TABLE IF NOT EXISTS gold.fact_technical_indicator (
    symbol_id BIGINT NOT NULL REFERENCES gold.dim_symbol(symbol_id),
    interval_id BIGINT NOT NULL REFERENCES gold.dim_interval(interval_id),
    date_id DATE NOT NULL REFERENCES gold.dim_date(date_id),

    open_time BIGINT NOT NULL,
    open_ts TIMESTAMPTZ NOT NULL,
    close_price DOUBLE PRECISION,
    sma7 DOUBLE PRECISION,
    sma25 DOUBLE PRECISION,
    rsi14 DOUBLE PRECISION,
    macd DOUBLE PRECISION,
    macd_signal DOUBLE PRECISION,
    macd_hist DOUBLE PRECISION,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (symbol_id, interval_id, open_time)
);

CREATE TABLE IF NOT EXISTS gold.fact_pattern_alert (
    alert_id BIGSERIAL PRIMARY KEY,
    symbol_id BIGINT NOT NULL REFERENCES gold.dim_symbol(symbol_id),
    date_id DATE NOT NULL REFERENCES gold.dim_date(date_id),
    pattern_type_id BIGINT NOT NULL REFERENCES gold.dim_pattern_type(pattern_type_id),

    close_time BIGINT NOT NULL,
    close_ts TIMESTAMPTZ NOT NULL,
    close_price DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE(symbol_id, pattern_type_id, close_time, close_price)
);

INSERT INTO gold.dim_interval(interval_code, interval_seconds)
VALUES ('1m', 60)
ON CONFLICT (interval_code) DO NOTHING;

INSERT INTO gold.dim_data_source(source_code, description)
VALUES
    ('batch_silver', 'Finalized historical data from Silver Iceberg table'),
    ('speed_hot', 'Recent realtime data from Speed Layer TimescaleDB tables')
ON CONFLICT (source_code) DO NOTHING;

CREATE OR REPLACE VIEW gold.vw_kline_unified AS
SELECT
    s.symbol,
    i.interval_code,
    f.date_id AS trade_date,
    f.open_time,
    f.open_ts,
    f.close_time,
    f.close_ts,
    f.open_price,
    f.high_price,
    f.low_price,
    f.close_price,
    f.volume,
    f.quote_asset_volume,
    f.number_of_trades,
    src.source_code AS data_source,
    f.is_final,
    f.updated_at
FROM gold.fact_kline f
JOIN gold.dim_symbol s ON f.symbol_id = s.symbol_id
JOIN gold.dim_interval i ON f.interval_id = i.interval_id
LEFT JOIN gold.dim_data_source src ON f.data_source_id = src.data_source_id;

CREATE OR REPLACE VIEW gold.vw_daily_market_summary AS
SELECT
    f.date_id AS trade_date,
    s.symbol,
    i.interval_code,
    f.open_price,
    f.high_price,
    f.low_price,
    f.close_price,
    f.total_volume,
    f.price_change,
    f.price_change_pct,
    f.daily_range,
    f.number_of_candles,
    f.updated_at
FROM gold.fact_daily_market_summary f
JOIN gold.dim_symbol s ON f.symbol_id = s.symbol_id
JOIN gold.dim_interval i ON f.interval_id = i.interval_id;

CREATE OR REPLACE VIEW gold.vw_latest_market_snapshot AS
WITH latest AS (
    SELECT
        fk.*,
        ROW_NUMBER() OVER (
            PARTITION BY fk.symbol_id, fk.interval_id
            ORDER BY fk.open_time DESC
        ) AS rn
    FROM gold.fact_kline fk
)
SELECT
    s.symbol,
    i.interval_code,
    l.open_time,
    l.open_ts,
    l.close_price,
    l.volume,
    src.source_code AS data_source,
    l.is_final,
    ti.sma7,
    ti.sma25,
    ti.rsi14,
    ti.macd,
    ti.macd_signal,
    ti.macd_hist,
    l.updated_at
FROM latest l
JOIN gold.dim_symbol s ON l.symbol_id = s.symbol_id
JOIN gold.dim_interval i ON l.interval_id = i.interval_id
LEFT JOIN gold.dim_data_source src ON l.data_source_id = src.data_source_id
LEFT JOIN gold.fact_technical_indicator ti
    ON l.symbol_id = ti.symbol_id
   AND l.interval_id = ti.interval_id
   AND l.open_time = ti.open_time
WHERE l.rn = 1;

CREATE OR REPLACE VIEW gold.vw_pattern_alerts_recent AS
SELECT
    s.symbol,
    d.date_id AS trade_date,
    p.pattern_type,
    f.close_time,
    f.close_ts,
    f.close_price,
    f.created_at
FROM gold.fact_pattern_alert f
JOIN gold.dim_symbol s ON f.symbol_id = s.symbol_id
JOIN gold.dim_date d ON f.date_id = d.date_id
JOIN gold.dim_pattern_type p ON f.pattern_type_id = p.pattern_type_id
WHERE f.close_ts >= NOW() - INTERVAL '24 hours';