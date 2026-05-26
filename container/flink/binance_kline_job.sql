-- 1. BẢNG NGUỒN (Đọc từ Kafka)
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
  'properties.group.id' = 'flink-consumer-sink-v4', -- Đổi version để đọc lại từ đầu nếu muốn test cảnh báo cũ
  'scan.startup.mode' = 'earliest-offset',        
  'format' = 'json',
  'json.fail-on-missing-field' = 'false',
  'json.ignore-parse-errors' = 'true'
);

-- 2. BẢNG ĐÍCH 1 (Ghi Raw Data)
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

-- 3. BẢNG ĐÍCH 2 (Ghi Cảnh báo Pattern)
CREATE TABLE pattern_alerts_sink (
  symbol STRING,
  close_time BIGINT,
  pattern_type STRING,
  close_price DOUBLE,
  PRIMARY KEY (symbol, close_time) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://timescaledb.default.svc.cluster.local:5432/silver_hot_data',
  'table-name' = 'pattern_alerts',
  'username' = 'kdquang',
  'password' = 'admin123', 
  'driver' = 'org.postgresql.Driver',
  'sink.buffer-flush.max-rows' = '1' -- Alert cần ghi ngay lập tức
);

-- 4. BẢNG ĐÍCH 3 (Ghi Technical Indicators)
CREATE TABLE technical_indicators_sink (
  symbol STRING,
  open_time BIGINT,
  close_price DOUBLE,
  sma7 DOUBLE,
  sma25 DOUBLE,
  rsi14 DOUBLE,        -- Thêm mới
  macd DOUBLE,         -- Thêm mới
  macd_signal DOUBLE,  -- Thêm mới
  macd_hist DOUBLE,    -- Thêm mới
  PRIMARY KEY (symbol, open_time) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://timescaledb.default.svc.cluster.local:5432/silver_hot_data',
  'table-name' = 'technical_indicators',
  'username' = 'kdquang',
  'password' = 'admin123',
  'driver' = 'org.postgresql.Driver',
  'sink.buffer-flush.max-rows' = '100', -- Flush nhanh hơn để Grafana hiện sớm
  'sink.buffer-flush.interval' = '1s'
);

-- 4. TẠO VIEW TẠM ĐỂ TÍNH TOÁN NỐI TIẾP (Giải quyết lỗi Over Agg)

-- Bước 4.1: Tính SMA7 trước
CREATE TEMPORARY VIEW sma7_calc_view AS
SELECT 
    symbol,
    open_time,
    close_price,
    ts, -- Giữ lại trường thời gian để truyền cho bước sau
    AVG(close_price) OVER (
        PARTITION BY symbol 
        ORDER BY ts 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS sma7
FROM binance_kline_with_watermark
WHERE is_closed = TRUE;

-- Bước 4.2: Dùng kết quả của SMA7 để tính tiếp SMA25
CREATE TEMPORARY VIEW final_indicators_view AS
SELECT 
    symbol,
    open_time,
    close_price,
    sma7,
    AVG(close_price) OVER (
        PARTITION BY symbol 
        ORDER BY ts 
        ROWS BETWEEN 24 PRECEDING AND CURRENT ROW
    ) AS sma25,
    ts
FROM sma7_calc_view;

-- BƯỚC 1: Tính chênh lệch giá (Lag - Cửa sổ 1 nến)
CREATE TEMPORARY VIEW step_lag_view AS
SELECT *,
    close_price - LAG(close_price, 1) OVER (PARTITION BY symbol ORDER BY ts) AS price_diff
FROM final_indicators_view;

-- BƯỚC 2: Tính SMA 12 (Cửa sổ 12 nến)
CREATE TEMPORARY VIEW step_sma12_view AS
SELECT *,
    AVG(close_price) OVER (PARTITION BY symbol ORDER BY ts ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS sma12
FROM step_lag_view;

-- BƯỚC 3: Tính SMA 26 (Cửa sổ 26 nến)
CREATE TEMPORARY VIEW step_sma26_view AS
SELECT *,
    AVG(close_price) OVER (PARTITION BY symbol ORDER BY ts ROWS BETWEEN 25 PRECEDING AND CURRENT ROW) AS sma26
FROM step_sma12_view;

-- BƯỚC 4: Tách Nến Tăng/Giảm và tính MACD Line (Không dùng Window)
CREATE TEMPORARY VIEW step_calc_view AS
SELECT *,
    CASE WHEN price_diff > 0 THEN price_diff ELSE 0 END AS gain,
    CASE WHEN price_diff < 0 THEN ABS(price_diff) ELSE 0 END AS loss,
    (sma12 - sma26) AS macd_line
FROM step_sma26_view;

-- BƯỚC 5: Tính trung bình Tăng/Giảm cho RSI (Cửa sổ 14 nến)
CREATE TEMPORARY VIEW step_rsi_view AS
SELECT *,
    AVG(gain) OVER (PARTITION BY symbol ORDER BY ts ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_gain,
    AVG(loss) OVER (PARTITION BY symbol ORDER BY ts ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_loss
FROM step_calc_view;

-- BƯỚC 6: Tính MACD Signal (Cửa sổ 9 nến)
CREATE TEMPORARY VIEW step_macd_signal_view AS
SELECT *,
    AVG(macd_line) OVER (PARTITION BY symbol ORDER BY ts ROWS BETWEEN 8 PRECEDING AND CURRENT ROW) AS macd_signal
FROM step_rsi_view;

-- BƯỚC 7: Ráp công thức cuối cùng ra RSI và MACD Histogram
CREATE TEMPORARY VIEW advanced_indicators_view AS
SELECT 
    symbol, open_time, close_price, sma7, sma25,
    
    -- Tính RSI 14
    CASE 
        WHEN avg_loss = 0 THEN 100.0 
        ELSE 100.0 - (100.0 / (1.0 + (avg_gain / avg_loss))) 
    END AS rsi14,
    
    -- Các thông số MACD
    macd_line AS macd,
    macd_signal,
    (macd_line - macd_signal) AS macd_hist
FROM step_macd_signal_view;

-- 4. GỘP CHUNG LUỒNG THỰC THI (Quan trọng nhất)
EXECUTE STATEMENT SET BEGIN

  -- Nhánh 1: Đẩy dữ liệu gốc vào binance_kline
  INSERT INTO binance_kline_sink
  SELECT symbol, open_time, close_time, open_price, high_price, low_price, close_price, volume, is_closed
  FROM binance_kline_with_watermark;

  -- Nhánh 2: Lọc nến đảo chiều và đẩy vào pattern_alerts
  INSERT INTO pattern_alerts_sink
  SELECT 
      symbol,
      close_time,
      CASE
          WHEN ABS(close_price - open_price) <= (high_price - low_price) * 0.1 THEN 'DOJI'
          WHEN (LEAST(open_price, close_price) - low_price) >= 2 * ABS(close_price - open_price)
           AND (high_price - GREATEST(open_price, close_price)) <= 0.1 * ABS(close_price - open_price) THEN 'HAMMER'
          ELSE 'UNKNOWN'
      END AS pattern_type,
      close_price
  FROM binance_kline_with_watermark
  WHERE is_closed = TRUE 
    AND high_price > low_price
    AND (
        ABS(close_price - open_price) <= (high_price - low_price) * 0.1
        OR 
        ((LEAST(open_price, close_price) - low_price) >= 2 * ABS(close_price - open_price)
         AND (high_price - GREATEST(open_price, close_price)) <= 0.1 * ABS(close_price - open_price))
    );

  -- Nhánh 3: Đẩy dữ liệu đã tính toán xong từ View vào Postgres
  INSERT INTO technical_indicators_sink
  SELECT 
      symbol, 
      open_time, 
      close_price, 
      sma7, 
      sma25,
      rsi14,
      macd,
      macd_signal,
      macd_hist
  FROM advanced_indicators_view;

END;
