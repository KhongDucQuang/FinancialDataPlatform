import argparse
import os
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    expr,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
    count as spark_count,
)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--process-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--interval", default="1m")
    parser.add_argument("--hot-lookback-hours", type=int, default=48)
    return parser.parse_args()


def get_conn():
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", "5432")),
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    )


def execute_sql(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
    conn.commit()


def ensure_static_dimensions(conn, interval_code):
    execute_sql(
        conn,
        """
        INSERT INTO gold.dim_interval(interval_code, interval_seconds)
        VALUES (%s, 60)
        ON CONFLICT (interval_code) DO NOTHING;

        INSERT INTO gold.dim_data_source(source_code, description)
        VALUES
            ('batch_silver', 'Finalized historical data from Silver Iceberg table'),
            ('speed_hot', 'Recent realtime data from Speed Layer TimescaleDB tables')
        ON CONFLICT (source_code) DO NOTHING;
        """,
        (interval_code,),
    )


def upsert_symbols(conn, symbols):
    if not symbols:
        return

    rows = []
    for symbol in sorted(set(symbols)):
        if symbol.endswith("USDT"):
            base_asset = symbol.replace("USDT", "")
            quote_asset = "USDT"
        else:
            base_asset = symbol
            quote_asset = None

        rows.append((symbol, base_asset, quote_asset))

    sql = """
    INSERT INTO gold.dim_symbol(symbol, base_asset, quote_asset)
    VALUES %s
    ON CONFLICT (symbol)
    DO UPDATE SET
        base_asset = EXCLUDED.base_asset,
        quote_asset = EXCLUDED.quote_asset,
        is_active = TRUE;
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)
    conn.commit()


def upsert_dates(conn, dates):
    if not dates:
        return

    rows = []
    for d in sorted(set(dates)):
        rows.append((
            d,
            d.year,
            d.month,
            d.day,
            (d.month - 1) // 3 + 1,
            d.isoweekday(),
            d.isoweekday() in (6, 7),
        ))

    sql = """
    INSERT INTO gold.dim_date(
        date_id, year, month, day, quarter, day_of_week, is_weekend
    )
    VALUES %s
    ON CONFLICT (date_id)
    DO UPDATE SET
        year = EXCLUDED.year,
        month = EXCLUDED.month,
        day = EXCLUDED.day,
        quarter = EXCLUDED.quarter,
        day_of_week = EXCLUDED.day_of_week,
        is_weekend = EXCLUDED.is_weekend;
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)
    conn.commit()


def get_dimension_maps(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT symbol, symbol_id FROM gold.dim_symbol")
        symbol_map = dict(cur.fetchall())

        cur.execute("SELECT interval_code, interval_id FROM gold.dim_interval")
        interval_map = dict(cur.fetchall())

        cur.execute("SELECT source_code, data_source_id FROM gold.dim_data_source")
        source_map = dict(cur.fetchall())

    return symbol_map, interval_map, source_map


def upsert_batch_fact_kline(conn, rows):
    if not rows:
        return

    sql = """
    INSERT INTO gold.fact_kline (
        symbol_id, interval_id, date_id,
        open_time, open_ts, close_time, close_ts,
        open_price, high_price, low_price, close_price, volume,
        quote_asset_volume, number_of_trades,
        taker_buy_base_volume, taker_buy_quote_volume,
        data_source_id, is_final, updated_at
    )
    VALUES %s
    ON CONFLICT (symbol_id, interval_id, open_time)
    DO UPDATE SET
        date_id = EXCLUDED.date_id,
        open_ts = EXCLUDED.open_ts,
        close_time = EXCLUDED.close_time,
        close_ts = EXCLUDED.close_ts,
        open_price = EXCLUDED.open_price,
        high_price = EXCLUDED.high_price,
        low_price = EXCLUDED.low_price,
        close_price = EXCLUDED.close_price,
        volume = EXCLUDED.volume,
        quote_asset_volume = EXCLUDED.quote_asset_volume,
        number_of_trades = EXCLUDED.number_of_trades,
        taker_buy_base_volume = EXCLUDED.taker_buy_base_volume,
        taker_buy_quote_volume = EXCLUDED.taker_buy_quote_volume,
        data_source_id = EXCLUDED.data_source_id,
        is_final = TRUE,
        updated_at = NOW();
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)
    conn.commit()


def upsert_daily_summary(conn, rows):
    if not rows:
        return

    sql = """
    INSERT INTO gold.fact_daily_market_summary (
        date_id, symbol_id, interval_id,
        open_price, high_price, low_price, close_price,
        total_volume, price_change, price_change_pct,
        daily_range, number_of_candles, updated_at
    )
    VALUES %s
    ON CONFLICT (date_id, symbol_id, interval_id)
    DO UPDATE SET
        open_price = EXCLUDED.open_price,
        high_price = EXCLUDED.high_price,
        low_price = EXCLUDED.low_price,
        close_price = EXCLUDED.close_price,
        total_volume = EXCLUDED.total_volume,
        price_change = EXCLUDED.price_change,
        price_change_pct = EXCLUDED.price_change_pct,
        daily_range = EXCLUDED.daily_range,
        number_of_candles = EXCLUDED.number_of_candles,
        updated_at = NOW();
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)
    conn.commit()


def refresh_hot_speed_layer(conn, hot_lookback_hours):
    sql = """
    INSERT INTO gold.dim_symbol(symbol, base_asset, quote_asset)
    SELECT DISTINCT
        symbol,
        CASE
            WHEN symbol LIKE '%%USDT' THEN REPLACE(symbol, 'USDT', '')
            ELSE symbol
        END AS base_asset,
        CASE
            WHEN symbol LIKE '%%USDT' THEN 'USDT'
            ELSE NULL
        END AS quote_asset
    FROM public.binance_kline
    WHERE open_time >= (
        EXTRACT(EPOCH FROM NOW() - (%s::TEXT || ' hours')::INTERVAL) * 1000
    )::BIGINT
    ON CONFLICT (symbol) DO NOTHING;

    INSERT INTO gold.dim_date(date_id, year, month, day, quarter, day_of_week, is_weekend)
    SELECT DISTINCT
        d::DATE AS date_id,
        EXTRACT(YEAR FROM d)::INT AS year,
        EXTRACT(MONTH FROM d)::INT AS month,
        EXTRACT(DAY FROM d)::INT AS day,
        EXTRACT(QUARTER FROM d)::INT AS quarter,
        EXTRACT(ISODOW FROM d)::INT AS day_of_week,
        EXTRACT(ISODOW FROM d)::INT IN (6, 7) AS is_weekend
    FROM (
        SELECT (TO_TIMESTAMP(open_time / 1000.0) AT TIME ZONE 'UTC')::DATE AS d
        FROM public.binance_kline
        WHERE open_time >= (
            EXTRACT(EPOCH FROM NOW() - (%s::TEXT || ' hours')::INTERVAL) * 1000
        )::BIGINT
    ) x
    ON CONFLICT (date_id) DO NOTHING;

    INSERT INTO gold.fact_kline (
        symbol_id, interval_id, date_id,
        open_time, open_ts, close_time, close_ts,
        open_price, high_price, low_price, close_price, volume,
        quote_asset_volume, number_of_trades,
        taker_buy_base_volume, taker_buy_quote_volume,
        data_source_id, is_final, updated_at
    )
    SELECT
        ds.symbol_id,
        di.interval_id,
        (TO_TIMESTAMP(k.open_time / 1000.0) AT TIME ZONE 'UTC')::DATE AS date_id,
        k.open_time,
        TO_TIMESTAMP(k.open_time / 1000.0) AS open_ts,
        k.close_time,
        TO_TIMESTAMP(k.close_time / 1000.0) AS close_ts,
        k.open_price,
        k.high_price,
        k.low_price,
        k.close_price,
        k.volume,
        NULL::DOUBLE PRECISION AS quote_asset_volume,
        NULL::BIGINT AS number_of_trades,
        NULL::DOUBLE PRECISION AS taker_buy_base_volume,
        NULL::DOUBLE PRECISION AS taker_buy_quote_volume,
        src.data_source_id,
        FALSE AS is_final,
        NOW() AS updated_at
    FROM public.binance_kline k
    JOIN gold.dim_symbol ds ON k.symbol = ds.symbol
    JOIN gold.dim_interval di ON di.interval_code = '1m'
    JOIN gold.dim_data_source src ON src.source_code = 'speed_hot'
    WHERE k.open_time >= (
        EXTRACT(EPOCH FROM NOW() - (%s::TEXT || ' hours')::INTERVAL) * 1000
    )::BIGINT
    ON CONFLICT (symbol_id, interval_id, open_time)
    DO UPDATE SET
        date_id = EXCLUDED.date_id,
        open_ts = EXCLUDED.open_ts,
        close_time = EXCLUDED.close_time,
        close_ts = EXCLUDED.close_ts,
        open_price = EXCLUDED.open_price,
        high_price = EXCLUDED.high_price,
        low_price = EXCLUDED.low_price,
        close_price = EXCLUDED.close_price,
        volume = EXCLUDED.volume,
        data_source_id = EXCLUDED.data_source_id,
        updated_at = NOW()
    WHERE gold.fact_kline.is_final = FALSE;
    """

    execute_sql(conn, sql, (hot_lookback_hours, hot_lookback_hours, hot_lookback_hours))


def refresh_technical_indicators(conn, hot_lookback_hours):
    sql = """
    INSERT INTO gold.dim_symbol(symbol, base_asset, quote_asset)
    SELECT DISTINCT
        symbol,
        CASE
            WHEN symbol LIKE '%%USDT' THEN REPLACE(symbol, 'USDT', '')
            ELSE symbol
        END AS base_asset,
        CASE
            WHEN symbol LIKE '%%USDT' THEN 'USDT'
            ELSE NULL
        END AS quote_asset
    FROM public.technical_indicators
    WHERE open_time >= (
        EXTRACT(EPOCH FROM NOW() - (%s::TEXT || ' hours')::INTERVAL) * 1000
    )::BIGINT
    ON CONFLICT (symbol) DO NOTHING;

    INSERT INTO gold.dim_date(date_id, year, month, day, quarter, day_of_week, is_weekend)
    SELECT DISTINCT
        d::DATE AS date_id,
        EXTRACT(YEAR FROM d)::INT AS year,
        EXTRACT(MONTH FROM d)::INT AS month,
        EXTRACT(DAY FROM d)::INT AS day,
        EXTRACT(QUARTER FROM d)::INT AS quarter,
        EXTRACT(ISODOW FROM d)::INT AS day_of_week,
        EXTRACT(ISODOW FROM d)::INT IN (6, 7) AS is_weekend
    FROM (
        SELECT (TO_TIMESTAMP(open_time / 1000.0) AT TIME ZONE 'UTC')::DATE AS d
        FROM public.technical_indicators
        WHERE open_time >= (
            EXTRACT(EPOCH FROM NOW() - (%s::TEXT || ' hours')::INTERVAL) * 1000
        )::BIGINT
    ) x
    ON CONFLICT (date_id) DO NOTHING;

    INSERT INTO gold.fact_technical_indicator (
        symbol_id, interval_id, date_id,
        open_time, open_ts, close_price,
        sma7, sma25, rsi14, macd, macd_signal, macd_hist,
        updated_at
    )
    SELECT
        ds.symbol_id,
        di.interval_id,
        (TO_TIMESTAMP(t.open_time / 1000.0) AT TIME ZONE 'UTC')::DATE AS date_id,
        t.open_time,
        TO_TIMESTAMP(t.open_time / 1000.0) AS open_ts,
        t.close_price,
        t.sma7,
        t.sma25,
        t.rsi14,
        t.macd,
        t.macd_signal,
        t.macd_hist,
        NOW()
    FROM public.technical_indicators t
    JOIN gold.dim_symbol ds ON t.symbol = ds.symbol
    JOIN gold.dim_interval di ON di.interval_code = '1m'
    WHERE t.open_time >= (
        EXTRACT(EPOCH FROM NOW() - (%s::TEXT || ' hours')::INTERVAL) * 1000
    )::BIGINT
    ON CONFLICT (symbol_id, interval_id, open_time)
    DO UPDATE SET
        close_price = EXCLUDED.close_price,
        sma7 = EXCLUDED.sma7,
        sma25 = EXCLUDED.sma25,
        rsi14 = EXCLUDED.rsi14,
        macd = EXCLUDED.macd,
        macd_signal = EXCLUDED.macd_signal,
        macd_hist = EXCLUDED.macd_hist,
        updated_at = NOW();
    """

    execute_sql(conn, sql, (hot_lookback_hours, hot_lookback_hours, hot_lookback_hours))


def refresh_pattern_alerts(conn, hot_lookback_hours):
    sql = """
    INSERT INTO gold.dim_pattern_type(pattern_type)
    SELECT DISTINCT pattern_type
    FROM public.pattern_alerts
    ON CONFLICT (pattern_type) DO NOTHING;

    INSERT INTO gold.dim_symbol(symbol, base_asset, quote_asset)
    SELECT DISTINCT
        symbol,
        CASE
            WHEN symbol LIKE '%%USDT' THEN REPLACE(symbol, 'USDT', '')
            ELSE symbol
        END AS base_asset,
        CASE
            WHEN symbol LIKE '%%USDT' THEN 'USDT'
            ELSE NULL
        END AS quote_asset
    FROM public.pattern_alerts
    ON CONFLICT (symbol) DO NOTHING;

    INSERT INTO gold.dim_date(date_id, year, month, day, quarter, day_of_week, is_weekend)
    SELECT DISTINCT
        d::DATE AS date_id,
        EXTRACT(YEAR FROM d)::INT AS year,
        EXTRACT(MONTH FROM d)::INT AS month,
        EXTRACT(DAY FROM d)::INT AS day,
        EXTRACT(QUARTER FROM d)::INT AS quarter,
        EXTRACT(ISODOW FROM d)::INT AS day_of_week,
        EXTRACT(ISODOW FROM d)::INT IN (6, 7) AS is_weekend
    FROM (
        SELECT (TO_TIMESTAMP(close_time / 1000.0) AT TIME ZONE 'UTC')::DATE AS d
        FROM public.pattern_alerts
        WHERE close_time >= (
            EXTRACT(EPOCH FROM NOW() - (%s::TEXT || ' hours')::INTERVAL) * 1000
        )::BIGINT
    ) x
    ON CONFLICT (date_id) DO NOTHING;

    INSERT INTO gold.fact_pattern_alert (
        symbol_id, date_id, pattern_type_id,
        close_time, close_ts, close_price, created_at
    )
    SELECT
        ds.symbol_id,
        (TO_TIMESTAMP(p.close_time / 1000.0) AT TIME ZONE 'UTC')::DATE AS date_id,
        pt.pattern_type_id,
        p.close_time,
        TO_TIMESTAMP(p.close_time / 1000.0) AS close_ts,
        p.close_price,
        NOW()
    FROM public.pattern_alerts p
    JOIN gold.dim_symbol ds ON p.symbol = ds.symbol
    JOIN gold.dim_pattern_type pt ON p.pattern_type = pt.pattern_type
    WHERE p.close_time >= (
        EXTRACT(EPOCH FROM NOW() - (%s::TEXT || ' hours')::INTERVAL) * 1000
    )::BIGINT
    ON CONFLICT (symbol_id, pattern_type_id, close_time, close_price)
    DO NOTHING;
    """

    execute_sql(conn, sql, (hot_lookback_hours, hot_lookback_hours))


def main():
    args = parse_args()
    gcs_key = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    spark = (
        SparkSession.builder
        .appName("silver-to-gold-star")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", f"gs://{args.bucket}/silver/iceberg")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", gcs_key)
        .getOrCreate()
    )

    conn = get_conn()

    try:
        ensure_static_dimensions(conn, args.interval)

        print(f"Reading Silver Iceberg table for process_date={args.process_date}")

        silver = (
            spark.table("lakehouse.silver.binance_kline")
            .filter(col("trade_date") == lit(args.process_date))
            .filter(col("interval") == lit(args.interval))
        )

        row_count = silver.count()
        print(f"Silver rows for {args.process_date}: {row_count}")

        if row_count > 0:
            symbols = [r["symbol"] for r in silver.select("symbol").distinct().collect()]
            dates = [r["trade_date"] for r in silver.select("trade_date").distinct().collect()]

            upsert_symbols(conn, symbols)
            upsert_dates(conn, dates)

            symbol_map, interval_map, source_map = get_dimension_maps(conn)
            interval_id = interval_map[args.interval]
            batch_source_id = source_map["batch_silver"]

            kline_rows = []
            selected_cols = [
                "symbol", "interval", "trade_date",
                "open_time", "open_ts", "close_time", "close_ts",
                "open_price", "high_price", "low_price", "close_price", "volume",
                "quote_asset_volume", "number_of_trades",
                "taker_buy_base_volume", "taker_buy_quote_volume",
            ]

            for r in silver.select(*selected_cols).toLocalIterator():
                kline_rows.append((
                    symbol_map[r["symbol"]],
                    interval_id,
                    r["trade_date"],
                    int(r["open_time"]),
                    r["open_ts"],
                    int(r["close_time"]) if r["close_time"] is not None else None,
                    r["close_ts"],
                    float(r["open_price"]) if r["open_price"] is not None else None,
                    float(r["high_price"]) if r["high_price"] is not None else None,
                    float(r["low_price"]) if r["low_price"] is not None else None,
                    float(r["close_price"]) if r["close_price"] is not None else None,
                    float(r["volume"]) if r["volume"] is not None else None,
                    float(r["quote_asset_volume"]) if r["quote_asset_volume"] is not None else None,
                    int(r["number_of_trades"]) if r["number_of_trades"] is not None else None,
                    float(r["taker_buy_base_volume"]) if r["taker_buy_base_volume"] is not None else None,
                    float(r["taker_buy_quote_volume"]) if r["taker_buy_quote_volume"] is not None else None,
                    batch_source_id,
                    True,
                    datetime.now(timezone.utc),
                ))

            print(f"Upserting batch fact_kline rows: {len(kline_rows)}")
            upsert_batch_fact_kline(conn, kline_rows)

            summary = (
                silver
                .groupBy("trade_date", "symbol", "interval")
                .agg(
                    expr("min_by(open_price, open_time)").alias("open_price"),
                    spark_max("high_price").alias("high_price"),
                    spark_min("low_price").alias("low_price"),
                    expr("max_by(close_price, open_time)").alias("close_price"),
                    spark_sum("volume").alias("total_volume"),
                    spark_count("*").alias("number_of_candles"),
                )
                .withColumn("price_change", col("close_price") - col("open_price"))
                .withColumn(
                    "price_change_pct",
                    ((col("close_price") - col("open_price")) / col("open_price")) * lit(100.0),
                )
                .withColumn("daily_range", col("high_price") - col("low_price"))
            )

            summary_rows = []
            for r in summary.toLocalIterator():
                summary_rows.append((
                    r["trade_date"],
                    symbol_map[r["symbol"]],
                    interval_id,
                    float(r["open_price"]) if r["open_price"] is not None else None,
                    float(r["high_price"]) if r["high_price"] is not None else None,
                    float(r["low_price"]) if r["low_price"] is not None else None,
                    float(r["close_price"]) if r["close_price"] is not None else None,
                    float(r["total_volume"]) if r["total_volume"] is not None else None,
                    float(r["price_change"]) if r["price_change"] is not None else None,
                    float(r["price_change_pct"]) if r["price_change_pct"] is not None else None,
                    float(r["daily_range"]) if r["daily_range"] is not None else None,
                    int(r["number_of_candles"]) if r["number_of_candles"] is not None else None,
                    datetime.now(timezone.utc),
                ))

            print(f"Upserting daily summary rows: {len(summary_rows)}")
            upsert_daily_summary(conn, summary_rows)

        print(f"Refreshing Speed Layer hot data, lookback_hours={args.hot_lookback_hours}")
        refresh_hot_speed_layer(conn, args.hot_lookback_hours)
        refresh_technical_indicators(conn, args.hot_lookback_hours)
        refresh_pattern_alerts(conn, args.hot_lookback_hours)

        print("Silver-to-Gold Star Schema job completed successfully")

    finally:
        conn.close()
        spark.stop()


if __name__ == "__main__":
    main()