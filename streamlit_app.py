import streamlit as st
from confluent_kafka import Consumer
import json
import pandas as pd
import time
from datetime import datetime

# 🔷 Page Config
st.set_page_config(
    page_title="Footfall Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 🔷 Title
st.title("📊 Smart Footfall Analytics Dashboard")

# 🔷 Kafka Consumer
@st.cache_resource
def get_consumer():
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'streamlit-dashboard',
        'auto.offset.reset': 'latest'
    })
    consumer.subscribe(['footfall_topic'])
    return consumer

consumer = get_consumer()

# 🔷 Session State Initialization
if "data" not in st.session_state:
    st.session_state.data = {
        "entrance": {"IN": 0, "OUT": 0, "dwell": 0},
        "shelf": {"IN": 0, "OUT": 0, "dwell": 0}
    }

if "history" not in st.session_state:
    st.session_state.history = pd.DataFrame(
        columns=["time", "zone", "IN", "OUT"]
    )

# 🔷 Kafka Poll Function
def fetch_kafka():
    msg = consumer.poll(0.1)

    if msg is None or msg.error():
        return

    try:
        data = json.loads(msg.value().decode('utf-8'))
        zone = data.get("zone", "")

        current_time = datetime.now().strftime("%H:%M:%S")

        if zone == "":
            zone_name = "entrance"
        elif zone == "shelf_area":
            zone_name = "shelf"
        else:
            return

        # Update live data
        st.session_state.data[zone_name] = {
            "IN": data.get("IN", 0),
            "OUT": data.get("OUT", 0),
            "dwell": data.get("actual_time", 0)
        }

        # Append to history
        new_row = {
            "time": current_time,
            "zone": zone_name,
            "IN": data.get("IN", 0),
            "OUT": data.get("OUT", 0)
        }

        st.session_state.history = pd.concat(
            [st.session_state.history, pd.DataFrame([new_row])],
            ignore_index=True
        )

    except Exception as e:
        st.error(f"Kafka Error: {e}")


# 🔄 Poll Kafka multiple times
for _ in range(5):
    fetch_kafka()


# =========================
# 🔷 KPI SECTION
# =========================

st.subheader("📍 Real-Time Zone Metrics")

col1, col2 = st.columns(2)

# 🔹 Entrance
with col1:
    st.markdown("### 🚪 Entrance Zone")
    c1, c2, c3 = st.columns(3)
    c1.metric("IN", st.session_state.data["entrance"]["IN"])
    c2.metric("OUT", st.session_state.data["entrance"]["OUT"])
    c3.metric("Dwell", f'{st.session_state.data["entrance"]["dwell"]} s')

# 🔹 Shelf
with col2:
    st.markdown("### 🛒 Shelf Area")
    c1, c2, c3 = st.columns(3)
    c1.metric("IN", st.session_state.data["shelf"]["IN"])
    c2.metric("OUT", st.session_state.data["shelf"]["OUT"])
    c3.metric("Dwell", f'{st.session_state.data["shelf"]["dwell"]} s')


# =========================
# 🔷 CHARTS
# =========================

st.subheader("📈 Footfall Trends Over Time")

if not st.session_state.history.empty:

    df = st.session_state.history.copy()

    # Separate zones
    entrance_df = df[df["zone"] == "entrance"]
    shelf_df = df[df["zone"] == "shelf"]

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Entrance Trends")
        st.line_chart(
            entrance_df.set_index("time")[["IN", "OUT"]]
        )

    with col2:
        st.markdown("#### Shelf Trends")
        st.line_chart(
            shelf_df.set_index("time")[["IN", "OUT"]]
        )

else:
    st.info("Waiting for Kafka data...")

# =========================
# 🔷 SUMMARY COMPARISON
# =========================

st.subheader("📊 Zone Comparison")

comparison_df = pd.DataFrame({
    "Zone": ["Entrance", "Shelf"],
    "IN": [
        st.session_state.data["entrance"]["IN"],
        st.session_state.data["shelf"]["IN"]
    ],
    "OUT": [
        st.session_state.data["entrance"]["OUT"],
        st.session_state.data["shelf"]["OUT"]
    ]
})

st.bar_chart(comparison_df.set_index("Zone"))


# =========================
# 🔷 SIDEBAR
# =========================

st.sidebar.header("⚙️ System Info")

st.sidebar.write("Kafka Topic: footfall_topic")
st.sidebar.write("Zones: Entrance, Shelf")
st.sidebar.write("Update Interval: ~2 sec")

# =========================
# 🔄 AUTO REFRESH
# =========================

time.sleep(2)
st.experimental_rerun()