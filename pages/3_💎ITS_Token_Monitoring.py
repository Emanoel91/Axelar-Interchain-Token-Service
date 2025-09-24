import streamlit as st
import pandas as pd
import requests
import plotly.express as px
import datetime

# ------------------------
# Config / Endpoints
# ------------------------
ASSETS_API = "https://api.axelarscan.io/api/getITSAssets"
GMP_API = "https://api.axelarscan.io/gmp/searchGMP"

st.set_page_config(page_title="Interchain Token Service", layout="wide")
st.title("Interchain Token Service Dashboard")

# ------------------------
# Helpers
# ------------------------
def fetch_assets():
    try:
        r = requests.get(ASSETS_API, timeout=15)
        r.raise_for_status()
        data = r.json()
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()
    except:
        return pd.DataFrame()

def fetch_gmp_data(symbol="XRP", from_time=None, to_time=None):
    params = {"symbol": symbol}
    if from_time: params["fromTime"] = int(from_time)
    if to_time: params["toTime"] = int(to_time)
    try:
        r = requests.get(GMP_API, params=params, timeout=30)
        r.raise_for_status()
        payload = r.json()
        if isinstance(payload, dict) and "data" in payload:
            return pd.DataFrame(payload["data"])
        return pd.DataFrame()
    except:
        return pd.DataFrame()

def preprocess(df, timeframe):
    if df.empty: return df

    transfers = df["interchain_transfer"].apply(lambda x: x if isinstance(x, dict) else {})
    transfers = transfers.apply(pd.Series)

    def get_col(name):
        if name in df.columns: return df[name]
        if name in transfers.columns: return transfers[name]
        return pd.Series([None] * len(df), index=df.index)

    df["sourceAddress"] = get_col("sourceAddress")
    df["destinationAddress"] = get_col("destinationAddress")

    # fix fillna bug: make sure replacement is a valid Series
    src_chain = get_col("sourceChain")
    if "origin_chain" in df.columns:
        src_chain = src_chain.fillna(df["origin_chain"])
    df["sourceChain"] = src_chain

    dst_chain = get_col("destinationChain")
    if "destination_chain" in df.columns:
        dst_chain = dst_chain.fillna(df["destination_chain"])
    elif "callback_chain" in df.columns:
        dst_chain = dst_chain.fillna(df["callback_chain"])
    df["destinationChain"] = dst_chain

    df["path"] = df["sourceChain"].fillna("Unknown").astype(str) + " → " + df["destinationChain"].fillna("Unknown").astype(str)

    df["tx_amount"] = pd.to_numeric(get_col("amount"), errors="coerce")
    df["value"] = pd.to_numeric(get_col("value"), errors="coerce")

    def extract_fee(x):
        if isinstance(x, dict):
            return x.get("base_fee_usd") or x.get("express_fee_usd")
        return None
    df["fee"] = pd.to_numeric(df.get("fees", pd.Series([None]*len(df))).apply(extract_fee), errors="coerce")

    def extract_ts(row):
        for key in ["executed", "call", "approved"]:
            val = row.get(key)
            if isinstance(val, dict) and "block_timestamp" in val:
                return val["block_timestamp"]
        return row.get("block_timestamp") or row.get("timestamp")
    df["timestamp"] = pd.to_datetime(df.apply(extract_ts, axis=1), unit="s", errors="coerce")
    df = df.dropna(subset=["timestamp"])

    if timeframe == "Day":
        df["period"] = df["timestamp"].dt.to_period("D").dt.to_timestamp()
    elif timeframe == "Week":
        df["period"] = df["timestamp"].dt.to_period("W").dt.start_time
    else:
        df["period"] = df["timestamp"].dt.to_period("M").dt.to_timestamp()

    return df

# ------------------------
# Filters (main page)
# ------------------------
assets_df = fetch_assets()
symbols = sorted(assets_df["symbol"].dropna().unique()) if not assets_df.empty else ["XRP"]
if "XRP" not in symbols: symbols.insert(0, "XRP")

today = datetime.date.today()
default_start = today - datetime.timedelta(days=30)
default_end = today - datetime.timedelta(days=1)

f1, f2, f3 = st.columns(3)
with f1:
    selected_symbol = st.selectbox("Select Token", symbols, index=symbols.index("XRP"))
with f2:
    from_date = st.date_input("From Date", value=default_start)
with f3:
    to_date = st.date_input("To Date", value=default_end)

timeframe = st.selectbox("Timeframe", ["Day", "Week", "Month"], index=0)  # default = Day

from_unix = int(datetime.datetime.combine(from_date, datetime.time.min).timestamp())
to_unix = int(datetime.datetime.combine(to_date, datetime.time.max).timestamp())

# ------------------------
# Fetch + Preprocess
# ------------------------
df = fetch_gmp_data(selected_symbol, from_unix, to_unix)
df = preprocess(df, timeframe)

if df.empty:
    st.warning("No data available for selected filters.")
    st.stop()

st.success(f"Loaded {len(df)} records for {selected_symbol}")
