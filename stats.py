"""
stats.py  -  Rule 2 (port traffic counter) + Rule 3 (ship waiting time)
---------------------------------------------------------------------
Every 1 minute of ship data, it calculates port statistics and sends them to
the topic "ship-stats". Dynamic alerts go to "ship-alerts".

  Rule 2: active ships, cumulative ships, mean/median speed, speed histogram
          ALERT "congestion" if active ships > average + 2 x std of last 15 minutes
  Rule 3: waiting (stopped) ships, mean/median waiting time, waiting histogram,
          cumulative waiting minutes
          ALERT "long wait" if a ship waits longer than 3 x the median waiting time

Runs as ONE copy in its own consumer group "port-stats": totals, mean and median
need to see ALL ships. It reads the same topic as detector.py, independently.
Its counters are saved to stats_state.json, so a restart continues the totals.
"""

import json
import os
import statistics
import time
from collections import deque
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer

# ---------------- Settings ----------------
KAFKA_SERVER = "localhost:9092"
WINDOW = 60               # statistics every 60 s of ship time
ACTIVE_WITHIN = 300       # a ship is "active" if seen in the last 5 min
STOPPED_SPEED = 0.5       # knots; below this a ship is waiting
MOVING_SPEED = 1.0        # knots; above this a ship is moving
HISTORY = 15              # minutes of history for the dynamic congestion limit
MIN_WAIT_ALERT = 10       # never alert for waits shorter than 10 min
OUTAGE_SECONDS = 30       # no data at all for this long = data source/producer was down
STATE_FILE = "stats_state.json"

# ---------------- Kafka ----------------
consumer = Consumer({
    "bootstrap.servers": KAFKA_SERVER,
    "group.id": "port-stats",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})
consumer.subscribe(["ship-positions"])

producer = Producer({
    "bootstrap.servers": KAFKA_SERVER,
    "enable.idempotence": True,
    "acks": "all",
})

# ---------------- State (saved to file, so restarts keep the totals) ----------------
state = {
    "ships": {},                    # ship ID -> name, speed, last_time, stopped_since, wait_alerted
    "all_ships_seen": [],           # every ship ID ever seen (cumulative)
    "cumulative_wait_minutes": 0,   # total ship-minutes spent waiting
    "active_history": [],           # active ship counts of the last minutes
    "window_start": None,
}
if os.path.exists(STATE_FILE):
    with open(STATE_FILE) as f:
        state = json.load(f)
    print(f"Restored state: {len(state['all_ships_seen'])} ships seen so far")

ships = state["ships"]
all_ships_seen = set(state["all_ships_seen"])
active_history = deque(state["active_history"], maxlen=HISTORY)


def save_state():
    state["all_ships_seen"] = list(all_ships_seen)
    state["active_history"] = list(active_history)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


def send(topic, key, data):
    producer.produce(topic, key=key, value=json.dumps(data))
    producer.poll(0)


def send_alert(alert_id, rule, message, ship_id=None, name=""):
    send("ship-alerts", alert_id, {
        "alert_id": alert_id, "rule": rule, "mmsi": ship_id, "name": name,
        "message": message, "alert_time": time.time(),
    })
    print(f"ALERT {rule}: {message}")


def speed_bucket(speed):
    if speed < 1:  return "0-1 kn"
    if speed < 5:  return "1-5 kn"
    if speed < 10: return "5-10 kn"
    if speed < 15: return "10-15 kn"
    return "15+ kn"


def wait_bucket(minutes):
    if minutes < 10: return "0-10 min"
    if minutes < 30: return "10-30 min"
    if minutes < 60: return "30-60 min"
    return "60+ min"


def update_ship(event):
    """Remember the latest position/speed of a ship and when it stopped."""
    key = str(event["mmsi"])
    ship = ships.setdefault(key, {"last_time": 0, "stopped_since": None, "wait_alerted": False})
    if event["event_time"] < ship["last_time"]:
        return                                   # older than what we know, ignore
    ship.update(name=event["name"], speed=event["speed"], last_time=event["event_time"])

    if event["speed"] < STOPPED_SPEED:
        if ship["stopped_since"] is None:
            ship["stopped_since"] = event["event_time"]   # ship just started waiting
    else:
        ship["stopped_since"] = None                       # ship is moving again
        ship["wait_alerted"] = False
    all_ships_seen.add(key)


def close_window(end_time):
    """One minute of ship time is over: calculate and publish statistics."""
    # Forget ships not seen for 30 min (they left the area)
    for key in [k for k, s in ships.items() if end_time - s["last_time"] > 1800]:
        del ships[key]

    active = {k: s for k, s in ships.items() if end_time - s["last_time"] <= ACTIVE_WITHIN}
    moving_speeds = [s["speed"] for s in active.values() if s["speed"] > MOVING_SPEED]
    waiting = {k: max(end_time - s["stopped_since"], 0) / 60
               for k, s in active.items() if s["stopped_since"] is not None}

    state["cumulative_wait_minutes"] += len(waiting)       # each waiting ship adds 1 minute
    median_wait = statistics.median(waiting.values()) if waiting else 0

    speed_hist = {b: 0 for b in ["0-1 kn", "1-5 kn", "5-10 kn", "10-15 kn", "15+ kn"]}
    for s in active.values():
        speed_hist[speed_bucket(s["speed"])] += 1
    wait_hist = {b: 0 for b in ["0-10 min", "10-30 min", "30-60 min", "60+ min"]}
    for minutes in waiting.values():
        wait_hist[wait_bucket(minutes)] += 1

    # ---- Rule 2: dynamic congestion limit (needs 5 minutes of history first) ----
    congestion_limit = None
    if len(active_history) >= 5:
        mean = statistics.mean(active_history)
        std = max(statistics.pstdev(active_history), 1)    # at least 1, so a flat history still works
        congestion_limit = round(mean + 2 * std, 1)
        if len(active) > congestion_limit:
            send_alert(f"port-congestion-{int(end_time)}", "congestion",
                       f"{len(active)} active ships, normal is up to {congestion_limit}")
    active_history.append(len(active))

    # ---- Rule 3: dynamic long-wait limit (needs at least 3 waiting ships) ----
    wait_limit = None
    if len(waiting) >= 3:
        wait_limit = max(3 * median_wait, MIN_WAIT_ALERT)
        for key, minutes in waiting.items():
            ship = ships[key]
            if minutes > wait_limit and not ship["wait_alerted"]:
                send_alert(f"{key}-longwait-{int(ship['stopped_since'])}", "long_wait",
                           f"{ship['name'] or key} waiting {minutes:.0f} min "
                           f"(limit {wait_limit:.0f} min = 3 x median)", key, ship["name"])
                ship["wait_alerted"] = True

    stats = {
        "window_end": end_time,
        "time": datetime.fromtimestamp(end_time, timezone.utc).strftime("%Y-%m-%d %H:%M"),
        # Rule 2
        "active_ships": len(active),
        "cumulative_ships": len(all_ships_seen),
        "mean_speed": round(statistics.mean(moving_speeds), 1) if moving_speeds else 0,
        "median_speed": round(statistics.median(moving_speeds), 1) if moving_speeds else 0,
        "speed_histogram": speed_hist,
        "congestion_limit": congestion_limit,
        # Rule 3
        "waiting_ships": len(waiting),
        "mean_wait_min": round(statistics.mean(waiting.values()), 1) if waiting else 0,
        "median_wait_min": round(median_wait, 1),
        "wait_histogram": wait_hist,
        "cumulative_wait_minutes": state["cumulative_wait_minutes"],
    }
    send("ship-stats", "port", stats)
    print(f"[{stats['time']}] active={stats['active_ships']}  total={stats['cumulative_ships']}  "
          f"speed mean/median={stats['mean_speed']}/{stats['median_speed']} kn  "
          f"waiting={stats['waiting_ships']}  wait median={stats['median_wait_min']} min")


if __name__ == "__main__":
    print("Port statistics running... (Ctrl + C to stop)")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue

            event = json.loads(msg.value())

            # Data outage? (producer or aisstream was down) We do not know what the
            # ships did during that time, so waiting clocks restart from now on.
            last = state.get("last_event_time", 0)
            if last and event["event_time"] - last > OUTAGE_SECONDS:
                print(f"Data outage of {int(event['event_time'] - last)} s - "
                      f"waiting times restart, no wait alerts for that time")
                for ship in ships.values():
                    if ship["stopped_since"] is not None:
                        ship["stopped_since"] = event["event_time"]
            state["last_event_time"] = max(last, event["event_time"])

            # Which minute does this message belong to? (ship time, not computer time)
            # If a new minute has started, first finish the previous one.
            minute = int(event["event_time"] // WINDOW) * WINDOW
            if state["window_start"] is None:
                state["window_start"] = minute
            elif minute > state["window_start"]:
                close_window(state["window_start"] + WINDOW)
                state["window_start"] = minute
                save_state()

            update_ship(event)

            consumer.commit(message=msg, asynchronous=True)   # progress saved AFTER processing
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        save_state()
        producer.flush()
        consumer.close()
