/*
Helps define schema for station information table. 
We do this because this allows us to set the station_id as the primary key, which confluent requires for us to
use upserts when joining stations and station_information into station_current_status instead of regular inserts.
*/

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