"""
producer.py
-----------
Reads LIVE ship positions (AIS) around the Port of Hamburg from aisstream.io
and sends them into the Kafka topic "ship-positions".

Reliability features:
  1. Auto-reconnect   -> if the live stream drops, we connect again
  2. Duplicate filter -> if the source sends the same message twice, we send it only once
  3. Idempotent Kafka -> network retries never create duplicates inside Kafka
  4. Recording        -> every raw message is saved to a file (backup for replay)
"""

import asyncio
import json
import os
import time
from datetime import datetime, timezone

import websockets
from confluent_kafka import Producer

# ---------------- Settings ----------------
API_KEY = os.environ.get("AISSTREAM_API_KEY")       # your key, set in the terminal
STREAM_URL = "wss://stream.aisstream.io/v0/stream"
KAFKA_SERVER = "localhost:9092"
TOPIC = "ship-positions"
RECORD_FILE = "recorded_ais.jsonl"

# Area = Port of Hamburg + river Elbe, as [[south-west corner], [north-east corner]] in [lat, lon]
BOUNDING_BOX = [[[53.45, 9.65], [53.62, 10.10]]]

DEDUP_WINDOW_SECONDS = 600   # remember message IDs for 10 minutes

# ---------------- Kafka producer ----------------
producer = Producer({
    "bootstrap.servers": KAFKA_SERVER,
    "enable.idempotence": True,   # Kafka drops duplicates caused by retries (exactly once per message)
    "acks": "all",                # a message counts as sent only after Kafka has saved it
})

seen_ids = {}                                   # message_id -> when we saw it (duplicate filter)
stats = {"sent": 0, "duplicates": 0, "skipped": 0}


def delivery_report(error, message):
    """Kafka calls this after each message. We only print if sending failed."""
    if error is not None:
        print(f"Delivery failed: {error}")


def parse_time(time_utc):
    """'2026-11-11 19:00:00.123456 +0000 UTC'  ->  seconds since 1970 (UTC)."""
    dt = datetime.strptime(time_utc[:19], "%Y-%m-%d %H:%M:%S")
    return dt.replace(tzinfo=timezone.utc).timestamp()


def to_ship_event(raw):
    """Turn one raw aisstream message into a small clean dictionary.
    Returns None if the message is not a usable position report."""
    msg_type = raw.get("MessageType")
    if msg_type not in ("PositionReport", "StandardClassBPositionReport"):
        return None

    meta = raw["MetaData"]
    report = raw["Message"][msg_type]
    speed = report.get("Sog")                 # Sog = speed over ground, in knots
    if speed is None or speed >= 102.2:       # 102.3 means "speed not available"
        return None

    return {
        "mmsi": meta["MMSI"],                 # unique ship ID
        "name": meta.get("ShipName", "").strip(),
        "lat": meta["latitude"],
        "lon": meta["longitude"],
        "speed": speed,
        "event_time": parse_time(meta["time_utc"]),   # when the ship sent it
        "ingest_time": time.time(),                   # when we received it
    }


def is_duplicate(event):
    """Same ship + same time + same position = the same message seen twice."""
    message_id = f'{event["mmsi"]}-{event["event_time"]}-{event["lat"]}-{event["lon"]}'
    if message_id in seen_ids:
        return True
    seen_ids[message_id] = time.time()
    return False


def forget_old_ids():
    """Remove IDs older than 10 minutes so memory does not keep growing."""
    limit = time.time() - DEDUP_WINDOW_SECONDS
    for message_id in [m for m, seen_at in seen_ids.items() if seen_at < limit]:
        del seen_ids[message_id]


def handle_message(text, record_file):
    """Process one raw message: record it, clean it, filter duplicates, send to Kafka."""
    if isinstance(text, bytes):
        text = text.decode("utf-8")
    record_file.write(text.strip() + "\n")    # backup copy of the real data

    event = to_ship_event(json.loads(text))
    if event is None:
        stats["skipped"] += 1
        return
    if is_duplicate(event):
        stats["duplicates"] += 1
        return

    # Key = ship ID -> all messages of one ship go to the same partition, in order
    producer.produce(TOPIC, key=str(event["mmsi"]), value=json.dumps(event),
                     callback=delivery_report)
    producer.poll(0)                          # let Kafka run the delivery callbacks
    stats["sent"] += 1


async def stream_forever():
    """Connect to aisstream.io and keep reading. Reconnect if the connection drops."""
    subscription = {
        "APIKey": API_KEY,
        "BoundingBoxes": BOUNDING_BOX,
        "FilterMessageTypes": ["PositionReport", "StandardClassBPositionReport"],
    }
    wait = 1                                  # seconds to wait before reconnecting

    with open(RECORD_FILE, "a", encoding="utf-8") as record_file:
        while True:
            try:
                async with websockets.connect(STREAM_URL) as ws:
                    await ws.send(json.dumps(subscription))   # must be sent within 3 s
                    print("Connected to live AIS stream (Port of Hamburg)")
                    wait = 1
                    last_report, last_sent = time.time(), 0

                    async for text in ws:
                        handle_message(text, record_file)

                        # Every 10 seconds: print throughput + clean duplicate memory
                        if time.time() - last_report >= 10:
                            rate = (stats["sent"] - last_sent) / (time.time() - last_report)
                            print(f"sent={stats['sent']}  duplicates={stats['duplicates']}  "
                                  f"skipped={stats['skipped']}  throughput={rate:.1f} msg/s")
                            record_file.flush()
                            forget_old_ids()
                            last_report, last_sent = time.time(), stats["sent"]

            except Exception as error:
                print(f"Stream lost ({error}). Reconnecting in {wait} s...")
                await asyncio.sleep(wait)
                wait = min(wait * 2, 60)          # wait longer after each failure, max 60 s


if __name__ == "__main__":
    if not API_KEY:
        raise SystemExit("Please set AISSTREAM_API_KEY first (see README).")
    try:
        asyncio.run(stream_forever())
    except KeyboardInterrupt:
        print("Stopping... sending remaining messages to Kafka")
        producer.flush()                      # make sure nothing is lost on exit
