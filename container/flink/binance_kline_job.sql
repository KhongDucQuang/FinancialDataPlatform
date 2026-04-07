CREATE TABLE binance_kline_sink (
  symbol STRING,
  open_time BIGINT,
  close_time BIGINT,
  open_price DOUBLE,
  high_price DOUBLE,
  low_price DOUBLE,
  close_price DOUBLE,
  volume DOUBLE,
  is_closed BOOLEAN,
  PRIMARY KEY (symbol, open_time) NOT ENFORCED   
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://timescaledb.default.svc.cluster.local:5432/silver_hot_data',
  'table-name' = 'binance_kline',
  'username' = 'kdquang',
  'password' = 'admin123',                   
  'driver' = 'org.postgresql.Driver',
  'sink.buffer-flush.max-rows' = '1000',
  'sink.buffer-flush.interval' = '1s',
  'sink.max-retries' = '3'
);

CREATE TABLE binance_kline_with_watermark (
  symbol STRING,
  open_time BIGINT,
  close_time BIGINT,
  open_price DOUBLE,
  high_price DOUBLE,
  low_price DOUBLE,
  close_price DOUBLE,
  volume DOUBLE,
  is_closed BOOLEAN,
  ts AS TO_TIMESTAMP(FROM_UNIXTIME(open_time / 1000)),     
  WATERMARK FOR ts AS ts - INTERVAL '10' SECOND 
) WITH (
  'connector' = 'kafka',
  'topic' = 'binance_raw_kline',
  'properties.bootstrap.servers' = 'redpanda.default.svc.cluster.local:9092',
  'properties.group.id' = 'flink-consumer-sink-v2',
  'scan.startup.mode' = 'earliest-offset',        
  'format' = 'json',
  'json.fail-on-missing-field' = 'false',
  'json.ignore-parse-errors' = 'true'
);

INSERT INTO binance_kline_sink
SELECT 
  symbol,
  open_time,
  close_time,
  open_price,
  high_price,
  low_price,
  close_price,
  volume,
  is_closed
FROM binance_kline_with_watermark;