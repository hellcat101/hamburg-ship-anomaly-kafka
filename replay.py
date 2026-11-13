"""
replay.py  -  Backup if the live feed fails
-------------------------------------------
Sends the REAL AIS data recorded earlier by producer.py (recorded_ais.jsonl)
into Kafka again, through the same cleaning + duplicate filter as producer.py.

Timestamps are shifted to "now", so detector, stats and dashboard handle it
like live data. Long pauses in the recording (e.g. producer was stopped) are
skipped. In the demo, say openly: "this is real data, recorded earlier".

Usage:
    python replay.py        -> original speed
    python replay.py 10     -> 10x faster (good for a throughput test)

Stop producer.py first, so live and replayed data are not mixed.
"""

import json
import sys
import time

import producer as p      # reuse the producer's Kafka settings, cleaning and duplicate filter

SPEED = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
MAX_PAUSE = 30             # pauses longer than 30 s in the recording are skipped

start = time.time()
first = previous = None
skipped_pause = 0          # total seconds of pauses we skipped
sent = 0

print(f"Replaying {p.RECORD_FILE} at {SPEED}x speed... (Ctrl + C to stop)")
try:
    with open(p.RECORD_FILE, encoding="utf-8") as f:
        for line in f:
            event = p.to_ship_event(json.loads(line))
            if event is None:
                continue

            original = event["event_time"]
            if first is None:
                first = previous = original
            if original - previous > MAX_PAUSE:              # recording had a break -> skip it
                skipped_pause += original - previous - 1
            previous = max(previous, original)

            # Position of this message in the replay timeline (in ship time)
            ship_offset = original - first - skipped_pause
            # Wait until it is time to send it (faster if SPEED > 1)
            wait = start + ship_offset / SPEED - time.time()
            if wait > 0:
                time.sleep(wait)

            if p.is_duplicate(event):                          # check with the ORIGINAL time
                continue
            # Timestamp = now. (At higher speed, ships simply seem to report more
            # often, so time never runs ahead of the clock and nothing gets confused.)
            event["event_time"] = event["ingest_time"] = time.time()

            p.producer.produce(p.TOPIC, key=str(event["mmsi"]), value=json.dumps(event),
                               callback=p.delivery_report)
            p.producer.poll(0)
            sent += 1
            if sent % 500 == 0:
                rate = sent / (time.time() - start)
                print(f"replayed={sent}  throughput={rate:.1f} msg/s")
                p.forget_old_ids()
except KeyboardInterrupt:
    print("Stopping...")
finally:
    p.producer.flush()
    print(f"Done. Replayed {sent} messages.")
