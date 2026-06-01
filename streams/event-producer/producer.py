import json
import os
import sys
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

## File change to trigger github actions pipeline

STATION_STATUS_URL = os.getenv("STATION_STATUS_URL")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "30"))
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

logger.remove()
logger.add(sys.stdout, level="INFO", format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}")
logger.add(
    "logs/producer.log",
    level="INFO",
    rotation="10 MB",
    retention="7 days",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
)

# Fields that constitute a meaningful status change
TRACKED_FIELDS = {
    "num_bikes_available",
    "num_ebikes_available",
    "num_bikes_disabled",
    "num_docks_available",
    "num_docks_disabled",
    "is_installed",
    "is_renting",
    "is_returning",
}


def read_ccloud_config(config_file: str) -> dict:
    """Parse a Confluent Cloud client.properties file into a dict."""
    config = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if line and not line.startswith("#"):
                key, value = line.split("=", 1)
                config[key.strip()] = value.strip()
    return config


def delivery_report(err, msg):
    if err:
        logger.error("Delivery failed for station {}: {}", msg.key(), err)


def fetch_station_statuses() -> list[dict]:
    response = requests.get(STATION_STATUS_URL, timeout=10)
    response.raise_for_status()
    return response.json()["data"]["stations"]


def station_state(station: dict) -> tuple:
    return tuple(station.get(f) for f in sorted(TRACKED_FIELDS))


def find_changed(
    current: list[dict], previous: dict[str, tuple]
) -> list[dict]:
    changed = []
    for station in current:
        sid = station["station_id"]
        if previous.get(sid) != station_state(station):
            changed.append(station)
    return changed


def publish_stations(producer: Producer, stations: list[dict], ingested_at: str):
    for station in stations:
        event = {**station, "ingested_at": ingested_at}
        producer.produce(
            topic=KAFKA_TOPIC,
            key=station["station_id"],
            value=json.dumps(event),
            on_delivery=delivery_report,
        )
    producer.flush()


def main():
    producer = Producer(read_ccloud_config("client.properties"))
    logger.info("Polling {} every {}s → topic '{}'", STATION_STATUS_URL, POLL_INTERVAL_SECONDS, KAFKA_TOPIC)

    previous_states: dict[str, tuple] = {}

    while True:
        try:
            ingested_at = datetime.now(timezone.utc).isoformat()
            stations = fetch_station_statuses()

            changed = find_changed(stations, previous_states)

            if changed:
                publish_stations(producer, changed, ingested_at)
                logger.info("Published {}/{} changed stations", len(changed), len(stations))
            else:
                logger.info("No changes across {} stations", len(stations))

            # Update state cache after publishing
            previous_states = {s["station_id"]: station_state(s) for s in stations}

        except requests.RequestException as exc:
            logger.error("HTTP error fetching station status: {}", exc)
        except Exception as exc:
            logger.exception("Unexpected error: {}", exc)

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
