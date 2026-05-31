import json
import os
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

STATION_STATUS_URL = os.getenv("STATION_STATUS_URL")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "30"))
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

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
        print(f"Delivery failed for station {msg.key()}: {err}")


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
    print(f"Polling {STATION_STATUS_URL} every {POLL_INTERVAL_SECONDS}s → topic '{KAFKA_TOPIC}'")

    previous_states: dict[str, tuple] = {}

    while True:
        try:
            ingested_at = datetime.now(timezone.utc).isoformat()
            stations = fetch_station_statuses()

            changed = find_changed(stations, previous_states)

            if changed:
                publish_stations(producer, changed, ingested_at)
                print(f"[{ingested_at}] Published {len(changed)}/{len(stations)} changed stations")
            else:
                print(f"[{ingested_at}] No changes across {len(stations)} stations")

            # Update state cache after publishing
            previous_states = {s["station_id"]: station_state(s) for s in stations}

        except requests.RequestException as exc:
            print(f"HTTP error fetching station status: {exc}")
        except Exception as exc:
            print(f"Unexpected error: {exc}")

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
