CREATE DATABASE IF NOT EXISTS weather;

CREATE TABLE IF NOT EXISTS weather.agg_5m_by_city
(
  city String,
  window_start DateTime,
  window_end   DateTime,
  count UInt32,
  avg_temp_c Float32,
  avg_humidity Float32,
  avg_wind_kmh Float32,
  ingested_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree()
ORDER BY (city, window_start)
PARTITION BY toDate(window_start);