import os
import ccxt
import pandas as pd
from datetime import datetime, timezone
from google.cloud import storage

# --- CẤU HÌNH ---
BUCKET_NAME = "your-bucket" # Thay bằng tên bucket của bạn
SYMBOL = "ETH/USDT"
SYMBOL_FOLDER = "ETHUSDT"
TIMEFRAME = "1m"
BRONZE_PREFIX = "bronze/binance_kline"
CHECKPOINT_PREFIX = "checkpoints/binance_kline"

# Khởi tạo GCS Client
# Lưu ý: Cần set biến môi trường GOOGLE_APPLICATION_CREDENTIALS trỏ tới file JSON service account
storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)

def get_last_checkpoint():
    """Đọc timestamp cuối cùng từ file checkpoint trên GCS"""
    blob = bucket.blob(f"{CHECKPOINT_PREFIX}/{SYMBOL_FOLDER}/last_open_time.txt")
    if blob.exists():
        timestamp_str = blob.download_as_text()
        print(f"[INFO] Đã tìm thấy checkpoint: {timestamp_str}")
        return int(timestamp_str)
    
    # Nếu chưa có checkpoint (chạy lần đầu), lấy dữ liệu từ 24h trước
    default_start = int((datetime.now(timezone.utc).timestamp() - 86400) * 1000)
    print(f"[INFO] Không có checkpoint. Bắt đầu từ 24h trước: {default_start}")
    return default_start

def save_checkpoint(timestamp):
    """Lưu lại timestamp của nến cuối cùng lên GCS"""
    blob = bucket.blob(f"{CHECKPOINT_PREFIX}/{SYMBOL_FOLDER}/last_open_time.txt")
    blob.upload_from_string(str(timestamp))
    print(f"[INFO] Đã cập nhật checkpoint mới: {timestamp}")

def fetch_binance_data(since_timestamp):
    """Cào dữ liệu từ Binance bằng ccxt"""
    exchange = ccxt.binance({
        'enableRateLimit': True, # Tự động giãn cách gọi API để không bị sàn block
    })
    
    print(f"[INFO] Đang kéo dữ liệu {SYMBOL} từ timestamp {since_timestamp}...")
    # Lấy tối đa 1000 nến mỗi lần gọi (giới hạn của Binance)
    ohlcv = exchange.fetch_ohlcv(SYMBOL, TIMEFRAME, since=since_timestamp, limit=1000)
    
    if not ohlcv:
        print("[INFO] Không có dữ liệu mới.")
        return None

    # Chuyển đổi thành Pandas DataFrame
    df = pd.DataFrame(ohlcv, columns=['open_time', 'open', 'high', 'low', 'close', 'volume'])
    return df

def upload_to_gcs_parquet(df):
    """Lưu df thành file Parquet và đẩy lên thư mục phân vùng GCS"""
    # Lấy thời gian của nến cuối cùng để tạo phân vùng (hoặc dùng thời gian hiện tại)
    last_dt = pd.to_datetime(df['open_time'].iloc[-1], unit='ms')
    year, month, day = last_dt.year, f"{last_dt.month:02d}", f"{last_dt.day:02d}"
    
    # Tạo đường dẫn động
    file_name = f"data_{int(datetime.now().timestamp())}.parquet"
    gcs_path = f"{BRONZE_PREFIX}/symbol={SYMBOL_FOLDER}/year={year}/month={month}/day={day}/{file_name}"
    
    # Lưu file parquet tạm ở local
    local_file = f"/tmp/{file_name}"
    df.to_parquet(local_file, engine='pyarrow', index=False)
    
    # Upload lên GCS
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_file)
    print(f"[SUCCESS] Đã upload {len(df)} dòng lên: gs://{BUCKET_NAME}/{gcs_path}")
    
    # Xóa file tạm
    os.remove(local_file)

def main():
    last_open_time = get_last_checkpoint()
    
    # Cộng thêm 1 millisecond để tránh cào lại nến cuối cùng của lần trước
    df = fetch_binance_data(since_timestamp=last_open_time + 1)
    
    if df is not None and not df.empty:
        upload_to_gcs_parquet(df)
        
        # Cập nhật checkpoint bằng open_time của dòng cuối cùng vừa lấy được
        new_checkpoint = int(df['open_time'].iloc[-1])
        save_checkpoint(new_checkpoint)
    else:
        print("[INFO] Đã up-to-date, không có gì để lưu.")

if __name__ == "__main__":
    main()