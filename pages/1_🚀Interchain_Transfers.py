import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.express as px
import plotly.graph_objects as go
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import networkx as nx
import requests
# --- Page Config ------------------------------------------------------------------------------------------------------
st.set_page_config(
    page_title="Axelar Interchain Token Service (ITS)",
    page_icon="https://pbs.twimg.com/profile_images/1869486848646537216/rs71wCQo_400x400.jpg",
    layout="wide"
)

# --- Title -----------------------------------------------------------------------------------------------------
st.title("🚀Interchain Transfers")

st.info("📊Charts initially display data for a default time range. Select a custom range to view results for your desired period.")
st.info("⏳On-chain data retrieval may take a few moments. Please wait while the results load.")

# --- Sidebar Footer Slightly Left-Aligned ---
st.sidebar.markdown(
    """
    <style>
    .sidebar-footer {
        position: fixed;
        bottom: 20px;
        width: 250px;
        font-size: 13px;
        color: gray;
        margin-left: 5px; # -- MOVE LEFT
        text-align: left;  
    }
    .sidebar-footer img {
        width: 16px;
        height: 16px;
        vertical-align: middle;
        border-radius: 50%;
        margin-right: 5px;
    }
    .sidebar-footer a {
        color: gray;
        text-decoration: none;
    }
    </style>

    <div class="sidebar-footer">
        <div>
            <a href="https://x.com/axelar" target="_blank">
                <img src="https://img.cryptorank.io/coins/axelar1663924228506.png" alt="Axelar Logo">
                Powered by Axelar
            </a>
        </div>
        <div style="margin-top: 5px;">
            <a href="https://x.com/0xeman_raz" target="_blank">
                <img src="https://pbs.twimg.com/profile_images/1841479747332608000/bindDGZQ_400x400.jpg" alt="Eman Raz">
                Built by Eman Raz
            </a>
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

# --- Snowflake Connection ----------------------------------------------------------------------------------------
snowflake_secrets = st.secrets["snowflake"]
user = snowflake_secrets["user"]
account = snowflake_secrets["account"]
private_key_str = snowflake_secrets["private_key"]
warehouse = snowflake_secrets.get("warehouse", "")
database = snowflake_secrets.get("database", "")
schema = snowflake_secrets.get("schema", "")

private_key_pem = f"-----BEGIN PRIVATE KEY-----\n{private_key_str}\n-----END PRIVATE KEY-----".encode("utf-8")
private_key = serialization.load_pem_private_key(
    private_key_pem,
    password=None,
    backend=default_backend()
)
private_key_bytes = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

conn = snowflake.connector.connect(
    user=user,
    account=account,
    private_key=private_key_bytes,
    warehouse=warehouse,
    database=database,
    schema=schema
)

# --- Date Inputs ---------------------------------------------------------------------------------------------------
col1, col2, col3 = st.columns(3)

with col1:
    timeframe = st.selectbox("Select Time Frame", ["month", "week", "day"])

with col2:
    start_date = st.date_input("Start Date", value=pd.to_datetime("2022-09-01"))

with col3:
    end_date = st.date_input("End Date", value=pd.to_datetime("2025-09-30"))
# --- Fetch Data from APIs ------------------------------------------------------------------------------------------
api_urls = [
    "https://api.axelarscan.io/gmp/GMPChart?contractAddress=0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C",
    "https://api.axelarscan.io/gmp/GMPChart?contractAddress=axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr"
]

dfs = []
for url in api_urls:
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()["data"]
        df = pd.DataFrame(data)
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit='ms')
        dfs.append(df)
    else:
        st.error(f"Failed to fetch data from {url}")

# --- Combine and Filter ------------------------------------------------------------------------------------------------
df_all = pd.concat(dfs)
df_all = df_all[(df_all["timestamp"].dt.date >= start_date) & (df_all["timestamp"].dt.date <= end_date)]

# --- Aggregate by Timeframe ----------------------------------------------------------------------------------------
if timeframe == "week":
    df_all["period"] = df_all["timestamp"].dt.to_period("W").apply(lambda r: r.start_time)
elif timeframe == "month":
    df_all["period"] = df_all["timestamp"].dt.to_period("M").apply(lambda r: r.start_time)
else:
    df_all["period"] = df_all["timestamp"]

agg_df = df_all.groupby("period").agg({
    "num_txs": "sum",
    "volume": "sum"
}).reset_index()

agg_df = agg_df.sort_values("period")
agg_df["cum_num_txs"] = agg_df["num_txs"].cumsum()
agg_df["cum_volume"] = agg_df["volume"].cumsum()

# --- KPIs -----------------------------------------------------------------------------------------------------------
col1, col2 = st.columns(2)
col1.metric("Total Transfers Count", agg_df["num_txs"].sum())
col2.metric("Total Transfers Volume", agg_df["volume"].sum())

# --- Plots ----------------------------------------------------------------------------------------------------------
col1, col2 = st.columns(2)

# Number of Interchain Transfers Over Time
fig1 = go.Figure()
fig1.add_trace(go.Bar(x=agg_df["period"], y=agg_df["num_txs"], name="Transfers", yaxis="y1"))
fig1.add_trace(go.Scatter(x=agg_df["period"], y=agg_df["cum_num_txs"], name="Total Transfers", yaxis="y2", mode="lines"))
fig1.update_layout(title="Number of Interchain Transfers Over Time", yaxis=dict(title="Txns count"), yaxis2=dict(title="Txns count", overlaying="y", side="right"),
    xaxis_title="", legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5))
col1.plotly_chart(fig1, use_container_width=True)

# Volume of Interchain Transfers Over Time
fig2 = go.Figure()
fig2.add_trace(go.Bar(x=agg_df["period"], y=agg_df["volume"], name="Volume", yaxis="y1"))
fig2.add_trace(go.Scatter(x=agg_df["period"], y=agg_df["cum_volume"],name="Total Volume", yaxis="y2", mode="lines"))
fig2.update_layout(title="Volume of Interchain Transfers Over Time", yaxis=dict(title="$USD"), yaxis2=dict(title="$USD", overlaying="y", side="right"), xaxis_title="",
    legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5))
col2.plotly_chart(fig2, use_container_width=True)
