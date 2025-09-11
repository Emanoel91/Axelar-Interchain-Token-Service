import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.express as px
import plotly.graph_objects as go
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import requests
import time
import networkx as nx

# --- Page Config ------------------------------------------------------------------------------------------------------
st.set_page_config(
    page_title="Axelar Interchain Token Service (ITS)",
    page_icon="https://pbs.twimg.com/profile_images/1869486848646537216/rs71wCQo_400x400.jpg",
    layout="wide"
)

# --- Title -----------------------------------------------------------------------------------------------------
st.title("🚀 Interchain Transfers")

st.info("📊 Charts initially display data for a default time range. Select a custom range to view results for your desired period.")
st.info("⏳ On-chain data retrieval may take a few moments. Please wait while the results load.")

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


# --- Function to fetch & decode API data --------------------------------------------------------------------------
def fetch_and_decode_data(api_url, start_date, end_date, label):
    """
    Fetch and decode data from Axelar API and return DataFrame with label column.
    """

    # --- Convert date inputs to unixtime (seconds) ---
    from_time = int(time.mktime(start_date.timetuple()))
    to_time = int(time.mktime(end_date.timetuple()))

    # --- Build API URL with time range ---
    url = f"{api_url}&fromTime={from_time}&toTime={to_time}"

    # --- Fetch data ---
    response = requests.get(url)
    if response.status_code != 200:
        st.error(f"❌ API call failed for {label}!")
        return pd.DataFrame()

    data = response.json().get("data", [])

    if not data:
        st.warning(f"⚠️ No data available for {label} in the selected range.")
        return pd.DataFrame()

    # --- Decode & Normalize ---
    df = pd.DataFrame(data)

    # Convert timestamp (ms) → datetime
    df["date"] = pd.to_datetime(df["timestamp"], unit="ms")

    # Reorder + add label column
    df = df[["date", "volume", "num_txs"]]
    df["label"] = label

    return df


# --- API Endpoints ------------------------------------------------------------------------------------------------
api1 = "https://api.axelarscan.io/gmp/GMPChart?contractAddress=0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C"
api2 = "https://api.axelarscan.io/gmp/GMPChart?contractAddress=axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr"

# --- Fetch Data ---------------------------------------------------------------------------------------------------
df1 = fetch_and_decode_data(api1, start_date, end_date, "Interchain Token Service")
df2 = fetch_and_decode_data(api2, start_date, end_date, "Axelar ITS Hub")

# Merge into one table
final_df = pd.concat([df1, df2], ignore_index=True)

# --- Display Data ------------------------------------------------------------------------------------------------
st.subheader("📋 Combined API Data Table")
st.dataframe(final_df, use_container_width=True)
