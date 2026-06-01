/*
Aggregates station bike availability into 5-minute tumbling windows.
Provides a rolling summary of total and average bikes available per station,
making it easy to detect utilisation trends and low-stock periods in near real-time.
*/

SELECT
  window_start,
  window_end,
  station_id,
  name                                              AS station_name,
  -- Aggregations: total, average, peak, and minimum availability per window
  COUNT(station_id)                                 AS event_count,
  SUM(num_bikes_available)                          AS total_bikes_available,
  AVG(num_bikes_available)                          AS avg_bikes_available,
  MAX(num_bikes_available)                          AS peak_bikes_available,
  MIN(num_bikes_available)                          AS min_bikes_available,
  -- Capacity utilisation ratio (bikes taken = capacity minus available)
  CAST(capacity AS INT) - MAX(num_bikes_available)  AS min_bikes_in_use
FROM TABLE(
  TUMBLE(
    TABLE `default`.`citibikes_data`.`station_current_status`,
    DESCRIPTOR(ingested_at),
    INTERVAL '5' MINUTES
  )
)
-- Only include stations that are actively renting
WHERE is_renting = 1
  AND num_bikes_available IS NOT NULL
GROUP BY
  window_start,
  window_end,
  station_id,
  name,
  capacity
HAVING SUM(num_bikes_available) > 0
ORDER BY
  window_start DESC,
  total_bikes_available ASC;
