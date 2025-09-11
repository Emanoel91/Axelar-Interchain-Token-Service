import streamlit as st
import pandas as pd
import requests
import time
import plotly.graph_objects as go
from pandasql import sqldf

# --- Helper for SQL queries on pandas ---
pysqldf = lambda q: sqldf(q, globals())

# --- Page Config ---
st.set_page_config(
    page_title="Axelar Interchain Token Service (ITS)",
    page_icon="🚀",
    layout="wide"
)

st.title("🚀 Interchain Transfers (SQL + Plotly)")

# --- Date Inputs ---
col1, col2, col3 = st.columns(3)
with col1:
    timeframe = st.selectbox("Select Time Frame", ["day", "week", "month"])
with col2:
    start_date = st.date_input("Start Date", value=pd.to_datetime("2022-09-01"))
with col3:
    end_date = st.date_input("End Date", value=pd.to_datetime("2025-09-30"))

# --- Function to fetch API data ---
def fetch_and_decode_data(api_url, start_date, end_date, label):
    from_time = int(time.mktime(start_date.timetuple()))
    to_time = int(time.mktime(end_date.timetuple()))
    url = f"{api_url}&fromTime={from_time}&toTime={to_time}"
    response = requests.get(url)
    if response.status_code != 200:
        st.error(f"API call failed for {label}")
        return pd.DataFrame()
    data = response.json().get("data", [])
    if not data:
        st.warning(f"No data for {label}")
        return pd.DataFrame()
    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["timestamp"], unit="ms")
    df = df[["date", "volume", "num_txs"]]
    df["label"] = label
    return df

# --- API Endpoints ---
api1 = "https://api.axelarscan.io/gmp/GMPChart?contractAddress=0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C"
api2 = "https://api.axelarscan.io/gmp/GMPChart?contractAddress=axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr"

# --- Fetch Data ---
df1 = fetch_and_decode_data(api1, start_date, end_date, "Interchain Token Service")
df2 = fetch_and_decode_data(api2, start_date, end_date, "Axelar ITS Hub")

# --- Merge Data ---
final_df = pd.concat([df1, df2], ignore_index=True)
st.subheader("📋 Combined Data")
st.dataframe(final_df, use_container_width=True)

# --- Build SQL query dynamically based on timeframe ---
if timeframe == "day":
    trunc = "date(date)"
elif timeframe == "week":
    trunc = "strftime('%Y-%W', date)"
elif timeframe == "month":
    trunc = "strftime('%Y-%m', date)"

query = f"""
SELECT 
    {trunc} as Date,
    label,
    SUM(num_txs) as Number_of_Transfers,
    SUM(SUM(num_txs)) OVER (PARTITION BY label ORDER BY {trunc}) as Total_Number_of_Transfers
FROM final_df
GROUP BY Date, label
ORDER BY Date
"""

df_sql = pysqldf(query)
st.subheader("📊 Aggregated Data (SQL)")
st.dataframe(df_sql, use_container_width=True)

# --- Plot Combined Column + Line Chart ---
fig = go.Figure()

# Bar: Number of Transfers
fig.add_trace(
    go.Bar(
        x=df_sql["Date"],
        y=df_sql["Number_of_Transfers"],
        name="Number of Transfers",
        marker_color="steelblue",
        yaxis="y1"
    )
)

# Line: Total Number of Transfers
fig.add_trace(
    go.Scatter(
        x=df_sql["Date"],
        y=df_sql["Total_Number_of_Transfers"],
        name="Total Number of Transfers",
        mode="lines+markers",
        line=dict(color="darkorange", width=2),
        yaxis="y2"
    )
)

# Layout with two y-axes
fig.update_layout(
    title=f"Transfers per {timeframe.capitalize()}",
    xaxis=dict(title="Date"),
    yaxis=dict(title="Number of Transfers", side="left"),
    yaxis2=dict(title="Total Number of Transfers", overlaying="y", side="right"),
    barmode="group",
    hovermode="x unified"
)

st.plotly_chart(fig, use_container_width=True)
