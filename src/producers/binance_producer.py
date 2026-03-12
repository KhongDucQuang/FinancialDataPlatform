import json
import os
import logging
from binance.websocket.spot.websocket_client import SpotWebsocketClient
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)

KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'redpanda:9092')
TOPIC_NAME = 'binance_raw_kline'

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    acks=1 
)

def message_handler(message):
    """Callback xử lý dữ liệu khi nhận được từ Binance"""
    try:
        if 'k' in message:
            kline = message['k']
            # Chỉ lấy nến đã đóng (x: True) hoặc lấy liên tục để vẽ chart real-time
            data = {
                "symbol": message['s'],
                "event_time": message['E'],
                "open_time": kline['t'],
                "close_time": kline['T'],
                "open": kline['o'],
                "high": kline['h'],
                "low": kline['l'],
                "close": kline['c'],
                "volume": kline['v'],
                "is_closed": kline['x']
            }
            
            producer.send(TOPIC_NAME, value=data)
            logging.info(f"Đã bắn vào Redpanda: {data['symbol']} - Giá: {data['close']}")
            
    except Exception as e:
        logging.error(f"Lỗi xử lý message: {e}")

SYMBOLS = ['btcusdt', 'ethusdt', 'solusdt', 'bnbusdt', 'adausdt']
INTERVAL = '1m'

# Websocket Client
ws_client = SpotWebsocketClient(on_message=message_handler)

for symbol in SYMBOLS:
    ws_client.kline(symbol=symbol, interval=INTERVAL)
    logging.info(f"--- Đã đăng ký stream cho: {symbol.upper()} tại khung {INTERVAL} ---")

print(f"--- Đang mở đường ống WebSocket cho {len(SYMBOLS)} cặp tiền sang {KAFKA_SERVER} ---")