import os
import json
import logging
from binance import ThreadedWebsocketManager
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)

KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'redpanda:9092')
TOPIC_NAME = 'binance_raw_kline'
SYMBOLS = ['btcusdt', 'ethusdt', 'solusdt', 'bnbusdt', 'adausdt']

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    acks=1,
    linger_ms=5,
    batch_size=16384
)

def handle_socket_message(msg):
    try:
        if msg.get('e') == 'kline':
            k = msg['k']

            data = {
                "symbol": msg['s'],
                "open_time": k['t'],
                "close_time": k['T'],
                "open_price": float(k['o']),
                "high_price": float(k['h']),
                "low_price": float(k['l']),
                "close_price": float(k['c']),
                "volume": float(k['v']),
                "is_closed": k['x']
            }

            producer.send(
                TOPIC_NAME,
                value=json.dumps(data).encode('utf-8')
            )

            logging.info(f"Pushed: {data['symbol']} - {data['close_price']}")

    except Exception as e:
        logging.error(f"Error processing message: {e}")

def main():
    twm = ThreadedWebsocketManager()
    twm.start()

    logging.info(f"--- Starting WebSocket for {len(SYMBOLS)} symbols ---")

    for symbol in SYMBOLS:
        twm.start_kline_socket(
            callback=handle_socket_message,
            symbol=symbol,
            interval='1m'
        )

    twm.join()

if __name__ == "__main__":
    main()