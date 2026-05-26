import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    from_unixtime,
    to_date,
    row_number,
    lit,
)
from pyspark.sql.window import Window
 
 
SILVER_COLUMNS = [
    "symbol",
    "interval",
    "open_time",
    "open_ts",
    "close_time",
    "close_ts",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
    "is_closed",
    "trade_date",
    "ingest_ts",
    "source",
]
 
 
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--process-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--interval", default="1m")
    parser.add_argument("--mode", default="merge", choices=["bootstrap", "merge", "overwrite-day"])
    return parser.parse_args()
 
 
def build_spark(bucket: str) -> SparkSession:
    warehouse = f"gs://{bucket}/silver/iceberg"
 
    return (
        SparkSession.builder
        .appName("bronze-to-silver-iceberg")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", warehouse)
        .config("spark.sql.defaultCatalog", "lakehouse")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/var/secrets/google/gcs-key.json")
        .getOrCreate()
    )
 
 
def main():
    args = parse_args()
    spark = build_spark(args.bucket)
    spark.sparkContext.setLogLevel("INFO")
 
    y, m, d = args.process_date[0:4], args.process_date[5:7], args.process_date[8:10]
    bronze_root = f"gs://{args.bucket}/bronze/binance/kline"
    bronze_path = f"{bronze_root}/interval={args.interval}/symbol=*/year={y}/month={m}/day={d}/*.parquet"
 
    print(f"Reading bronze_path={bronze_path}")
 
    df = (
        spark.read
        .option("basePath", bronze_root)
        .parquet(bronze_path)
    )
 
    cleaned = (
        df
        .withColumn("symbol", col("symbol").cast("string"))
        .withColumn("interval", col("interval").cast("string"))
        .withColumn("open_time", col("open_time").cast("long"))
        .withColumn("close_time", col("close_time").cast("long"))
        .withColumn("open_price", col("open_price").cast("double"))
        .withColumn("high_price", col("high_price").cast("double"))
        .withColumn("low_price", col("low_price").cast("double"))
        .withColumn("close_price", col("close_price").cast("double"))
        .withColumn("volume", col("volume").cast("double"))
        .withColumn("quote_asset_volume", col("quote_asset_volume").cast("double"))
        .withColumn("number_of_trades", col("number_of_trades").cast("long"))
        .withColumn("taker_buy_base_volume", col("taker_buy_base_volume").cast("double"))
        .withColumn("taker_buy_quote_volume", col("taker_buy_quote_volume").cast("double"))
        .withColumn("is_closed", col("is_closed").cast("boolean"))
        .withColumn("open_ts", to_timestamp(from_unixtime(col("open_time") / 1000)))
        .withColumn("close_ts", to_timestamp(from_unixtime(col("close_time") / 1000)))
        .withColumn("trade_date", to_date(col("open_ts")))
        .withColumn("ingest_ts", to_timestamp(col("ingest_ts")))
        .withColumn("source", col("source").cast("string"))
        .filter(col("symbol").isNotNull())
        .filter(col("interval").isNotNull())
        .filter(col("open_time").isNotNull())
        .filter(col("close_price").isNotNull())
    )
 
    w = Window.partitionBy("symbol", "interval", "open_time").orderBy(col("ingest_ts").desc_nulls_last())
 
    deduped = (
        cleaned
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .drop("rn")
        .select(*SILVER_COLUMNS)
    )
 
    deduped.createOrReplaceTempView("stg_binance_kline")
 
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
 
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.silver.binance_kline (
            symbol STRING,
            interval STRING,
            open_time BIGINT,
            open_ts TIMESTAMP,
            close_time BIGINT,
            close_ts TIMESTAMP,
            open_price DOUBLE,
            high_price DOUBLE,
            low_price DOUBLE,
            close_price DOUBLE,
            volume DOUBLE,
            quote_asset_volume DOUBLE,
            number_of_trades BIGINT,
            taker_buy_base_volume DOUBLE,
            taker_buy_quote_volume DOUBLE,
            is_closed BOOLEAN,
            trade_date DATE,
            ingest_ts TIMESTAMP,
            source STRING
        )
        USING iceberg
        PARTITIONED BY (trade_date, symbol)
        TBLPROPERTIES ('format-version'='2')
    """)
 
    if args.mode == "bootstrap":
        print("Mode bootstrap: append staging data into Iceberg table")
        deduped.writeTo("lakehouse.silver.binance_kline").append()
 
    elif args.mode == "overwrite-day":
        print(f"Mode overwrite-day: overwrite partitions for process_date={args.process_date}")
        spark.sql(f"""
            DELETE FROM lakehouse.silver.binance_kline
            WHERE trade_date = DATE '{args.process_date}'
        """)
        deduped.writeTo("lakehouse.silver.binance_kline").append()
 
    else:
        print("Mode merge: upsert by natural key symbol, interval, open_time")
        spark.sql("""
            MERGE INTO lakehouse.silver.binance_kline t
            USING stg_binance_kline s
            ON  t.symbol = s.symbol
            AND t.interval = s.interval
            AND t.open_time = s.open_time
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
 
    print("Silver row count for process date:")
    spark.sql(f"""
        SELECT symbol, COUNT(*) AS row_count
        FROM lakehouse.silver.binance_kline
        WHERE trade_date = DATE '{args.process_date}'
        GROUP BY symbol
        ORDER BY symbol
    """).show(100, False)
 
    spark.stop()
 
 
if __name__ == "__main__":
    main()
