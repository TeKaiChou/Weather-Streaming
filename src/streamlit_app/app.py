import streamlit as st
import pandas as pd
import clickhouse_connect
from streamlit_autorefresh import st_autorefresh
from datetime import datetime, timezone
import os

# ----- Config -----
host = os.getenv("CLICKHOUSE_HOST", "localhost")
port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
user = os.getenv("CLICKHOUSE_USER", "default")
password = os.getenv("CLICKHOUSE_PASSWORD", "default_pw")
database = os.getenv("CLICKHOUSE_DB", "weather")
REFRESH_SEC = int(os.getenv("REFRESH_SEC", "30")) 


st.set_page_config(page_title="Weather Streaming", layout="wide")
st.title("Weather Streaming Dashboard")

# refresh data every 30 seconds
_ = st_autorefresh(interval=REFRESH_SEC * 1000, limit=None, key="weather_refresh")

# ----- Query ClickHouse -----
client = clickhouse_connect.get_client(
    host=host,
    port=port,
    username=user,
    password=password,
    database=database,
)

window_minutes = st.slider("Look back (minutes)", 5, 180, 30, 5)

rows = client.query(f"""
  SELECT 
    city, 
    window_start, 
    window_end, 
    count, 
    avg_temp_c, 
    avg_humidity, 
    avg_wind_kmh
  FROM weather.agg_5m_by_city
  WHERE window_start >= now() - INTERVAL {window_minutes} MINUTE
  ORDER BY window_start ASC
""").result_rows

df = pd.DataFrame(rows, columns=["city","window_start","window_end","count","avg_temp_c","avg_humidity","avg_wind_kmh"])

# last updated time
st.caption(f"Last updated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")

if df.empty:
    st.info("No data yet. Keep the pipeline running for a few minutes.")
else:
    cities = sorted(df["city"].dropna().unique().tolist())
    city = st.selectbox("City", cities, index=0 if cities else None)

    sub = df[df["city"] == city].copy()
    if sub.empty:
        st.warning("No data for the selected city in the current window.")
    else:
        sub = sub.set_index("window_start")

        c1, c2, c3 = st.columns(3)
        c1.metric("Latest temp (°C)", f"{sub['avg_temp_c'].iloc[-1]:.1f}")
        c2.metric("Latest humidity", f"{sub['avg_humidity'].iloc[-1]:.2f}")
        c3.metric("Records in window", int(sub['count'].sum()))

        st.line_chart(sub[["avg_temp_c", "avg_humidity", "avg_wind_kmh"]])