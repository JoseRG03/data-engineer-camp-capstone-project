"""
Fetches Lyft/Citi Bike station information from the GBFS feed and
produces each station as a JSON message to a Confluent Kafka topic.

Configuration:
    Set the environment variables below (or edit the CONFIG dict directly).
"""

import json
import os
import sys
import time
import requests
from confluent_kafka import Producer, KafkaException

# ---------------------------------------------------------------------------
# Configuration — override with environment variables or edit here directly
# ---------------------------------------------------------------------------

TOPIC = "station_information"
GBFS_URL = "https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_information.json"

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
# ---------------------------------------------------------------------------
# Delivery callback
# ---------------------------------------------------------------------------
def delivery_report(err, msg):
    if err:
        print(f"  [ERROR] Delivery failed for key {msg.key()}: {err}", file=sys.stderr)
    else:
        print(f"  [OK] station_id={msg.key().decode()} → partition={msg.partition()} offset={msg.offset()}")


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------
def fetch_stations(url: str) -> list[dict]:
    print(f"Fetching {url} ...")
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    payload = resp.json()
    stations = payload["data"]["stations"]
    print(f"Retrieved {len(stations)} stations (last_updated={payload.get('last_updated')})")
    return stations


# ---------------------------------------------------------------------------
# Produce
# ---------------------------------------------------------------------------
def produce(stations: list[dict]) -> None:
    producer = Producer(read_ccloud_config("client.properties"))
    print(f"\nProducing {len(stations)} messages → topic '{TOPIC}' ...")

    for station in stations:
        key = station["station_id"]
        value = json.dumps(
            {
                **station,
                "_ingested_at": int(time.time()),
                "_source": GBFS_URL,
            },
            ensure_ascii=False,
        )
        try:
            producer.produce(
                topic=TOPIC,
                key=key.encode("utf-8"),
                value=value.encode("utf-8"),
                callback=delivery_report,
            )
            # Throttle the internal queue to avoid buffer overflow on large batches
            producer.poll(0)
        except KafkaException as exc:
            print(f"  [ERROR] Failed to enqueue {key}: {exc}", file=sys.stderr)

    # Wait for all outstanding messages to be delivered
    remaining = producer.flush(timeout=30)
    if remaining:
        print(f"\n[WARN] {remaining} message(s) were NOT delivered before timeout.", file=sys.stderr)
    else:
        print("\nAll messages delivered successfully.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    stations = fetch_stations(GBFS_URL)
    produce(stations)