CREATE TABLE `default`.`citibikes_data`.`station_information` (
  `key`   VARCHAR NOT NULL,
  name         VARCHAR,
  lat          DOUBLE,
  lon          DOUBLE,
  capacity     INT,
  short_name   VARCHAR,
  region_id    VARCHAR,
  rental_uris  VARCHAR,
  _ingested_at TIMESTAMP(3),
  _source      VARCHAR,
  PRIMARY KEY (`key`) NOT ENFORCED
) WITH (
  'connector'                        = 'confluent',
  'key.format'                       = 'raw',
  'value.format'                     = 'json-registry',
  'changelog.mode'                   = 'upsert',
  'scan.startup.mode'                = 'earliest-offset'
);