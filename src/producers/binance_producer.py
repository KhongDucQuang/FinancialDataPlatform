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
    """Callback function to process data received from Binance"""
    try:
        if 'k' in message:
            kline = message['k']
            # Only take closed candles (x: True) or take continuously for real-time chart
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
            logging.info(f"Sent to Redpanda: {data['symbol']} - Price: {data['close']}")
            
    except Exception as e:
        logging.error(f"Error processing message: {e}")

SYMBOLS = ['btcusdt', 'ethusdt', 'solusdt', 'bnbusdt', 'adausdt']
INTERVAL = '1m'

# WebSocket Client
ws_client = SpotWebsocketClient(on_message=message_handler)

for symbol in SYMBOLS:
    ws_client.kline(symbol=symbol, interval=INTERVAL)
    logging.info(f"--- Stream registered for: {symbol.upper()} at interval {INTERVAL} ---")

print(f"--- Opening WebSocket pipeline for {len(SYMBOLS)} trading pairs to {KAFKA_SERVER} ---")