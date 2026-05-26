import argparse
import io
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import List, Any
 
import pandas as pd
import requests
from google.cloud import storage
 
 
BINANCE_BASE_URL = "https://api.binance.com"
KLINE_ENDPOINT = "/api/v3/klines"
 
KLINE_COLUMNS = [
    "open_time",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "close_time",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
    "ignore",
]
 
 
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols", required=True, help="Comma-separated symbols, e.g. BTCUSDT,ETHUSDT")
    parser.add_argument("--interval", default="1m", help="Binance interval, e.g. 1m,5m,1h,1d")
    parser.add_argument("--start-date", required=True, help="UTC start date inclusive, YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="UTC end date exclusive, YYYY-MM-DD")
    parser.add_argument("--bucket", required=True, help="GCS bucket name without gs://")
    parser.add_argument("--prefix", default="bronze/binance/kline", help="GCS prefix")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
    return parser.parse_args()
 
 
def utc_date(date_str: str) -> datetime:
    return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
 
 
def to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)
 
 
def request_klines(symbol: str, interval: str, start_ms: int, end_ms: int, limit: int = 1000) -> List[List[Any]]:
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_ms,
        "endTime": end_ms - 1,
        "limit": limit,
    }
 
    for attempt in range(1, 6):
        try:
            response = requests.get(
                BINANCE_BASE_URL + KLINE_ENDPOINT,
                params=params,
                timeout=30,
            )
 
            if response.status_code in (418, 429):
                sleep_s = min(60, 5 * attempt)
                logging.warning("Rate limited for %s. Sleep %ss. Body=%s", symbol, sleep_s, response.text[:500])
                time.sleep(sleep_s)
                continue
 
            response.raise_for_status()
            return response.json()
 
        except Exception as exc:
            sleep_s = min(60, 3 * attempt)
            logging.warning("Request failed attempt=%s symbol=%s err=%s. Sleep %ss", attempt, symbol, exc, sleep_s)
            time.sleep(sleep_s)
 
    raise RuntimeError(f"Failed to fetch klines for {symbol} after retries")
 
 
def fetch_day(symbol: str, interval: str, day_start: datetime, day_end: datetime) -> pd.DataFrame:
    all_rows = []
    cursor_ms = to_ms(day_start)
    end_ms = to_ms(day_end)
 
    while cursor_ms < end_ms:
        rows = request_klines(symbol, interval, cursor_ms, end_ms, limit=1000)
        if not rows:
            break
 
        all_rows.extend(rows)
        last_open_time = int(rows[-1][0])
        next_cursor = last_open_time + 1
        if next_cursor <= cursor_ms:
            break
        cursor_ms = next_cursor
 
        if len(rows) < 1000:
            break
 
        time.sleep(0.15)
 
    if not all_rows:
        return pd.DataFrame(columns=["symbol", "interval", *KLINE_COLUMNS, "is_closed", "ingest_ts", "source"])
 
    df = pd.DataFrame(all_rows, columns=KLINE_COLUMNS)
    df.insert(0, "interval", interval)
    df.insert(0, "symbol", symbol)
 
    numeric_float_cols = [
        "open_price", "high_price", "low_price", "close_price",
        "volume", "quote_asset_volume", "taker_buy_base_volume", "taker_buy_quote_volume"
    ]
    numeric_int_cols = ["open_time", "close_time", "number_of_trades"]
 
    for col in numeric_float_cols:
        df[col] = df[col].astype(float)
    for col in numeric_int_cols:
        df[col] = df[col].astype("int64")
 
    df["is_closed"] = True
    df["ingest_ts"] = datetime.now(timezone.utc).isoformat()
    df["source"] = "binance_spot_rest_api"
    df = df.drop(columns=["ignore"])
    df = df.drop_duplicates(subset=["symbol", "interval", "open_time"])
    return df
 
 
def gcs_object_name(prefix: str, symbol: str, interval: str, day: datetime) -> str:
    return (
        f"{prefix}/"
        f"interval={interval}/"
        f"symbol={symbol}/"
        f"year={day.year:04d}/"
        f"month={day.month:02d}/"
        f"day={day.day:02d}/"
        f"part-{symbol}-{interval}-{day.strftime('%Y-%m-%d')}.parquet"
    )
 
 
def upload_df_to_gcs(df: pd.DataFrame, bucket_name: str, object_name: str, overwrite: bool):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
 
    if blob.exists() and not overwrite:
        logging.info("Skip existing gs://%s/%s", bucket_name, object_name)
        return
 
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow", compression="snappy")
    buffer.seek(0)
 
    blob.upload_from_file(buffer, content_type="application/octet-stream")
    logging.info("Uploaded rows=%s to gs://%s/%s", len(df), bucket_name, object_name)
 
 
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
 
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    start_dt = utc_date(args.start_date)
    end_dt = utc_date(args.end_date)
    if end_dt <= start_dt:
        raise ValueError("end-date must be greater than start-date")
 
    logging.info(
        "Start Binance historical backfill symbols=%s interval=%s start=%s end=%s bucket=%s prefix=%s",
        symbols, args.interval, args.start_date, args.end_date, args.bucket, args.prefix,
    )
 
    current_day = start_dt
    while current_day < end_dt:
        next_day = current_day + timedelta(days=1)
        for symbol in symbols:
            object_name = gcs_object_name(args.prefix, symbol, args.interval, current_day)
            logging.info("Fetching symbol=%s day=%s", symbol, current_day.date())
            df = fetch_day(symbol, args.interval, current_day, next_day)
            if df.empty:
                logging.warning("No data symbol=%s day=%s", symbol, current_day.date())
                continue
            upload_df_to_gcs(df, args.bucket, object_name, args.overwrite)
        current_day = next_day
 
    logging.info("Done")
 
 
if __name__ == "__main__":
    main()
