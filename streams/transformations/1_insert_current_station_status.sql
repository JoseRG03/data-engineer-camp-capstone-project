INSERT INTO `default`.`citibikes_data`.`station_current_status`
SELECT
  s.station_id,
  si.name,
  si.lat,
  si.lon,
  s.ingested_at,
  s.eightd_has_available_keys,
  CAST(s.is_installed AS INT) is_installed,
  CAST(s.is_renting   AS INT) is_renting,
  CAST(s.is_returning AS INT) is_returning,
  CAST(s.last_reported AS BIGINT) last_reported,
  s.legacy_id,
  CAST(s.num_bikes_available      AS INT) num_bikes_available,
  CAST(s.num_bikes_disabled       AS INT) num_bikes_disabled,
  CAST(s.num_docks_available      AS INT) num_docks_available,
  CAST(s.num_docks_disabled       AS INT) num_docks_disabled,
  CAST(s.num_ebikes_available     AS INT) num_ebikes_available,
  CAST(s.num_scooters_available   AS INT) num_scooters_available,
  CAST(s.num_scooters_unavailable AS INT) num_scooters_unavailable,
  CAST(si.capacity AS INT) capacity,
  si.short_name
FROM `default`.`citibikes_data`.`stations` s
LEFT JOIN `default`.`citibikes_data`.`station_information` si
  ON s.station_id = si.`key`;