"""
dashboard.py  -  Live dashboard (Streamlit)
-------------------------------------------
Reads the three Kafka topics and shows:
  - a live map of the ships
  - Rule 2: active / cumulative ships, mean & median speed, speed histogram
  - Rule 3: waiting ships, mean & median waiting time, waiting histogram
  - all alerts (duplicates removed by alert_id)
  - a warning if no new data arrives (producer or data source down)

Start with:   streamlit run dashboard.py
"""

import json
import time
import uuid

import pandas as pd
import streamlit as st
from confluent_kafka import Consumer

REFRESH_SECONDS = 5
STALE_AFTER = 60          # warn if no new ship data for 60 s

st.set_page_config(page_title="Hamburg Port Monitor", layout="wide")


# ---------- Kafka: one consumer per browser session, kept between refreshes ----------
if "consumer" not in st.session_state:
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": f"dashboard-{uuid.uuid4()}",   # own group -> reads everything, disturbs no one
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe(["ship-positions", "ship-alerts", "ship-stats"])
    st.session_state.consumer = consumer
    st.session_state.ships = {}        # ship ID -> latest position
    st.session_state.alerts = {}       # alert_id -> alert (same ID twice = stored once)
    st.session_state.stats = {}        # window_end -> stats of that minute
    st.session_state.last_ingest = 0   # when the producer received the newest message


def read_new_messages():
    """Read everything that is new in Kafka since the last refresh."""
    for _ in range(20):                                   # max 20 x 500 messages per refresh
        batch = st.session_state.consumer.consume(num_messages=500, timeout=0.5)
        if not batch:
            break
        for msg in batch:
            if msg.error():
                continue
            data = json.loads(msg.value())
            topic = msg.topic()
            if topic == "ship-positions":
                st.session_state.ships[data["mmsi"]] = data
                st.session_state.last_ingest = max(st.session_state.last_ingest, data["ingest_time"])
            elif topic == "ship-alerts":
                st.session_state.alerts[data["alert_id"]] = data
            elif topic == "ship-stats":
                st.session_state.stats[data["window_end"]] = data


read_new_messages()

st.title("🚢 Port of Hamburg – Live Ship Traffic Monitor")

# ---------- Pipeline health (what happens when the producer / source is down) ----------
age = time.time() - st.session_state.last_ingest
if st.session_state.last_ingest == 0:
    st.info("Waiting for the first ship data...")
elif age > STALE_AFTER:
    st.warning(f"No new ship data for {int(age)} s – producer or data source may be down. "
               "Showing last known data; Kafka keeps everything until it is back.")
else:
    st.success(f"Live – last ship message {int(age)} s ago")

stats = pd.DataFrame(sorted(st.session_state.stats.values(), key=lambda s: s["window_end"]))
if not stats.empty:
    stats["time"] = pd.to_datetime(stats["window_end"], unit="s", utc=True).dt.tz_convert("Europe/Berlin")
    stats = stats.set_index("time")
    latest = stats.iloc[-1]

    # ---------- Key numbers ----------
    c = st.columns(7)
    c[0].metric("Active ships", int(latest["active_ships"]))
    c[1].metric("Cumulative ships", int(latest["cumulative_ships"]))
    c[2].metric("Mean speed", f'{latest["mean_speed"]} kn')
    c[3].metric("Median speed", f'{latest["median_speed"]} kn')
    c[4].metric("Waiting ships", int(latest["waiting_ships"]))
    c[5].metric("Median wait", f'{latest["median_wait_min"]} min')
    c[6].metric("Total wait (ship-min)", f'{int(latest["cumulative_wait_minutes"]):,}')

# ---------- Map ----------
ships = pd.DataFrame(st.session_state.ships.values())
if not ships.empty:
    newest = ships["event_time"].max()
    ships = ships[newest - ships["event_time"] <= 600]               # seen in the last 10 min
    dark = {a["mmsi"] for a in st.session_state.alerts.values() if a["rule"] == "signal_gap"}
    ships["color"] = ["#e63946" if m in dark else ("#2a9d8f" if s > 1 else "#999999")
                      for m, s in zip(ships["mmsi"], ships["speed"])]
    st.subheader("Live map")
    st.caption("green = moving · grey = stopped · red = had a 'going dark' alert")
    st.map(ships, latitude="lat", longitude="lon", color="color", size=40, zoom=11)

# ---------- Charts ----------
if not stats.empty:
    left, right = st.columns(2)
    with left:
        st.subheader("Rule 2 – Port traffic")
        st.caption("Cumulative ships (only goes up)")
        st.line_chart(stats["cumulative_ships"])
        st.caption("Active ships vs. dynamic congestion limit (average + 2 × std)")
        st.line_chart(stats[["active_ships", "congestion_limit"]])
        st.caption("Mean and median speed of moving ships (kn)")
        st.line_chart(stats[["mean_speed", "median_speed"]])
        st.caption("Speed histogram (latest minute)")
        st.bar_chart(pd.Series(latest["speed_histogram"], name="ships"))
    with right:
        st.subheader("Rule 3 – Waiting time")
        st.caption("Cumulative waiting time (ship-minutes, only goes up)")
        st.line_chart(stats["cumulative_wait_minutes"])
        st.caption("Mean and median waiting time (min)")
        st.line_chart(stats[["mean_wait_min", "median_wait_min"]])
        st.caption("Waiting ships")
        st.line_chart(stats["waiting_ships"])
        st.caption("Waiting time histogram (latest minute)")
        st.bar_chart(pd.Series(latest["wait_histogram"], name="ships"))

# ---------- Alerts ----------
st.subheader(f"Alerts ({len(st.session_state.alerts)})")
alerts = pd.DataFrame(st.session_state.alerts.values())
if alerts.empty:
    st.write("No alerts yet.")
else:
    alerts["time"] = pd.to_datetime(alerts["alert_time"], unit="s", utc=True).dt.tz_convert("Europe/Berlin")
    st.write(alerts["rule"].value_counts().to_dict())
    st.dataframe(alerts.sort_values("time", ascending=False)[["time", "rule", "name", "mmsi", "message"]],
                 hide_index=True)

# ---------- Refresh ----------
time.sleep(REFRESH_SECONDS)
st.rerun()
