"""
detector.py  -  Rule 1: Signal gap ("going dark")
------------------------------------------------
Reads ship positions from Kafka. If a MOVING ship sends no message for more
than 60 seconds while inside the area, an alert is sent to "ship-alerts".

Scalable: you can start this script 2 or 3 times. All copies use the same
consumer group, so Kafka splits the 3 partitions between them. Because the
producer uses the ship ID as key, every message of one ship lands in the same
partition -> each copy always sees the complete history of "its" ships.
"""

import json
import time

from confluent_kafka import Consumer, Producer

# ---------------- Settings ----------------
KAFKA_SERVER = "localhost:9092"
GAP_SECONDS = 60          # one ship silent longer than this = "going dark"
OUTAGE_SECONDS = 30       # NO ship at all for this long = the data source was down, not the ships
MOVING_SPEED = 1.0        # knots; only moving ships count for this rule
FORGET_AFTER = 1800       # forget ships not seen for 30 min (they left)

# Same area as the producer. Ships near the edge may simply have sailed out,
# so we do not raise gap alerts for them.
SOUTH, WEST, NORTH, EAST = 53.45, 9.65, 53.62, 10.10
EDGE = 0.01               # about 1 km

# ---------------- Kafka ----------------
consumer = Consumer({
    "bootstrap.servers": KAFKA_SERVER,
    "group.id": "gap-detector",        # all copies of this script share this group
    "auto.offset.reset": "earliest",   # first start: read from the beginning
    "enable.auto.commit": False,       # we save our progress ourselves, AFTER processing
})
consumer.subscribe(["ship-positions"])

producer = Producer({
    "bootstrap.servers": KAFKA_SERVER,
    "enable.idempotence": True,
    "acks": "all",
})

ships = {}         # ship ID -> latest message of that ship (+ partition, "alerted" flag)

# One clock per partition. Each clock = newest ship timestamp seen in that partition.
# (While catching up, Kafka delivers partitions in batches, so one shared clock
#  would run ahead and make ships of the other partitions look silent.)
clocks = {}        # partition -> newest ship time
outage_end = {}    # partition -> time the last data outage ended


def near_edge(lat, lon):
    return (lat < SOUTH + EDGE or lat > NORTH - EDGE or
            lon < WEST + EDGE or lon > EAST - EDGE)


def send_alert(ship, silence):
    alert = {
        # Fixed ID: if this alert is ever produced twice, the dashboard can drop the copy
        "alert_id": f'{ship["mmsi"]}-gap-{int(ship["event_time"])}',
        "rule": "signal_gap",
        "mmsi": ship["mmsi"],
        "name": ship["name"],
        "lat": ship["lat"],
        "lon": ship["lon"],
        "message": f"No AIS signal for {int(silence)} s while moving at {ship['speed']} kn",
        "alert_time": time.time(),
    }
    producer.produce("ship-alerts", key=str(ship["mmsi"]), value=json.dumps(alert))
    producer.poll(0)
    print(f"ALERT going dark: {ship['name'] or ship['mmsi']}  silent {int(silence)} s")


def check_gaps(partition):
    """Look at every ship in this partition: who has been silent too long?"""
    now = clocks[partition]
    for mmsi, ship in list(ships.items()):
        if ship["partition"] != partition:
            continue
        if now - ship["event_time"] > FORGET_AFTER:
            del ships[mmsi]                      # ship has left, forget it
            continue
        # Count silence only from the end of the last data outage
        silence = now - max(ship["event_time"], outage_end.get(partition, 0))
        if (silence > GAP_SECONDS and ship["speed"] > MOVING_SPEED
                and not ship["alerted"] and not near_edge(ship["lat"], ship["lon"])):
            send_alert(ship, silence)
            ship["alerted"] = True               # alert only once per gap


if __name__ == "__main__":
    print("Gap detector running... (Ctrl + C to stop)")
    processed, latencies, last_report = 0, [], time.time()
    try:
        while True:
            msg = consumer.poll(1.0)             # wait up to 1 s for a message
            if msg is None:
                continue
            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue

            event = json.loads(msg.value())
            p = msg.partition()

            # Use the SHIP's timestamp as clock (event time), not the computer clock.
            # A big jump means no data arrived at all for a while (producer or
            # aisstream was down) -> do not blame the ships for that time.
            if p in clocks and event["event_time"] - clocks[p] > OUTAGE_SECONDS:
                print(f"Data outage of {int(event['event_time'] - clocks[p])} s "
                      f"in partition {p} - no gap alerts for that time")
                outage_end[p] = event["event_time"]
            clocks[p] = max(clocks.get(p, 0), event["event_time"])

            old = ships.get(event["mmsi"])
            if old is None or event["event_time"] >= old["event_time"]:
                event["partition"] = p
                event["alerted"] = False         # ship is sending again -> reset
                ships[event["mmsi"]] = event

            check_gaps(p)

            # Save progress only AFTER processing (at-least-once)
            consumer.commit(message=msg, asynchronous=True)

            # Latency = time from receiving the ship message to finishing processing it
            processed += 1
            latencies.append(time.time() - event["ingest_time"])
            if time.time() - last_report >= 10:
                avg_ms = sum(latencies) / len(latencies) * 1000
                print(f"processed={processed}  ships tracked={len(ships)}  "
                      f"latency (last 10 s)={avg_ms:.0f} ms")
                latencies, last_report = [], time.time()
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        producer.flush()
        consumer.close()                         # tells Kafka we left -> partitions go to other copies
