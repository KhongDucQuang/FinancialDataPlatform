import os
import json
import logging
import time
from binance import ThreadedWebsocketManager
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)

KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'redpanda:9092')
TOPIC_NAME = 'binance_raw_kline'
SYMBOLS = ['btcusdt', 'ethusdt', 'solusdt', 'bnbusdt', 'adausdt']

# Khởi tạo Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    acks=1
)

def handle_socket_message(msg):
    """Xử lý dữ liệu nến từ WebSocket"""
    try:
        if msg['e'] == 'kline':
            k = msg['k']
            data = {
                "symbol": msg['s'],
                "open_time": k['t'],
                "close_time": k['T'],
                "open": k['o'],
                "high": k['h'],
                "low": k['l'],
                "close": k['c'],
                "volume": k['v'],
                "is_closed": k['x']
            }
            producer.send(TOPIC_NAME, value=data)
            logging.info(f"Pushed: {data['symbol']} - Price: {data['close']}")
    except Exception as e:
        logging.error(f"Error processing message: {e}")

def main():
    twm = ThreadedWebsocketManager()
    twm.start()

    logging.info(f"--- Starting WebSocket for {len(SYMBOLS)} symbols ---")
    for symbol in SYMBOLS:
        twm.start_kline_socket(callback=handle_socket_message, symbol=symbol, interval='1m')
    
    twm.join()

if __name__ == "__main__":
    main()