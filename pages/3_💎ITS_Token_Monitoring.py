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

# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# ------------------------
# KPI Cards
# ------------------------
total_volume = df["tx_amount"].sum()
total_txs = len(df)
unique_users = df["sourceAddress"].nunique()
total_fee = df["fee"].sum()
unique_paths = df["path"].nunique()
unique_src_chains = df["sourceChain"].nunique()
unique_dst_chains = df["destinationChain"].nunique()
median_fee = df["fee"].median()

kpi1, kpi2, kpi3, kpi4 = st.columns(4)
with kpi1: st.metric("Total Volume", f"${total_volume:,.2f}")
with kpi2: st.metric("Total Transactions", f"{total_txs:,}")
with kpi3: st.metric("Unique Users", f"{unique_users:,}")
with kpi4: st.metric("Total Fees", f"${total_fee:,.2f}")

kpi5, kpi6, kpi7, kpi8 = st.columns(4)
with kpi5: st.metric("Unique Paths", f"{unique_paths:,}")
with kpi6: st.metric("Source Chains", f"{unique_src_chains:,}")
with kpi7: st.metric("Destination Chains", f"{unique_dst_chains:,}")
with kpi8: st.metric("Median Fee", f"${median_fee:,.4f}")

# ------------------------
# Charts: Time series
# ------------------------
# Transaction count
tx_counts = df.groupby("period").size().reset_index(name="count")
st.plotly_chart(px.line(tx_counts, x="period", y="count", title="Transaction Count Over Time"), use_container_width=True)

# Transaction volume
tx_volume = df.groupby("period")["tx_amount"].sum().reset_index()
st.plotly_chart(px.bar(tx_volume, x="period", y="tx_amount", title="Transaction Volume Over Time"), use_container_width=True)

# Unique users over time
user_counts = df.groupby("period")["sourceAddress"].nunique().reset_index(name="users")
st.plotly_chart(px.line(user_counts, x="period", y="users", title="Unique Users Over Time"), use_container_width=True)

# Fees over time
fee_series = df.groupby("period")["fee"].sum().reset_index()
st.plotly_chart(px.line(fee_series, x="period", y="fee", title="Fees Over Time"), use_container_width=True)

# ------------------------
# Charts: Stacked by Path
# ------------------------
stack_tx = df.groupby(["period","path"]).size().reset_index(name="count")
st.plotly_chart(px.bar(stack_tx, x="period", y="count", color="path", title="Transactions Over Time by Path"), use_container_width=True)

stack_vol = df.groupby(["period","path"])["tx_amount"].sum().reset_index()
st.plotly_chart(px.bar(stack_vol, x="period", y="tx_amount", color="path", title="Volume Over Time by Path"), use_container_width=True)

stack_users = df.groupby(["period","path"])["sourceAddress"].nunique().reset_index(name="users")
st.plotly_chart(px.bar(stack_users, x="period", y="users", color="path", title="Users Over Time by Path"), use_container_width=True)

stack_fee = df.groupby(["period","path"])["fee"].sum().reset_index()
st.plotly_chart(px.bar(stack_fee, x="period", y="fee", color="path", title="Fees Over Time by Path"), use_container_width=True)

# ------------------------
# Charts: Distribution by Path
# ------------------------
vol_path = df.groupby("path")["tx_amount"].sum().reset_index()
st.plotly_chart(px.pie(vol_path, names="path", values="tx_amount", title="Total Volume by Path"), use_container_width=True)

tx_path = df.groupby("path").size().reset_index(name="count")
st.plotly_chart(px.pie(tx_path, names="path", values="count", title="Total Transactions by Path"), use_container_width=True)

user_path = df.groupby("path")["sourceAddress"].nunique().reset_index(name="users")
st.plotly_chart(px.bar(user_path, x="path", y="users", title="Unique Users by Path"), use_container_width=True)

fee_path = df.groupby("path")["fee"].sum().reset_index()
st.plotly_chart(px.bar(fee_path, x="path", y="fee", title="Total Fees by Path"), use_container_width=True)

# ------------------------
# Top Users
# ------------------------
top_users_tx = df.groupby("sourceAddress").size().reset_index(name="count").sort_values("count", ascending=False).head(20)
st.plotly_chart(px.bar(top_users_tx, x="count", y="sourceAddress", orientation="h", title="Top Users by Transactions"), use_container_width=True)

top_users_vol = df.groupby("sourceAddress")["tx_amount"].sum().reset_index().sort_values("tx_amount", ascending=False).head(20)
st.plotly_chart(px.bar(top_users_vol, x="tx_amount", y="sourceAddress", orientation="h", title="Top Users by Volume"), use_container_width=True)

# ------------------------
# User Buckets
# ------------------------
# By transaction count
user_tx_counts = df.groupby("sourceAddress").size()
bins_tx = [1,5,10,20,50,100,float("inf")]
labels_tx = ["1","2-5","6-10","11-20","21-50","51-100",">100"]
bucket_tx = pd.cut(user_tx_counts, bins=[0]+bins_tx, labels=labels_tx, right=True).value_counts().reset_index()
bucket_tx.columns = ["range","users"]
st.plotly_chart(px.pie(bucket_tx, names="range", values="users", hole=0.5, title="Users by Transaction Count"), use_container_width=True)

# By transaction volume
user_volumes = df.groupby("sourceAddress")["tx_amount"].sum()
bins_vol = [1,10,100,1000,10000,float("inf")]
labels_vol = ["<1$","1-10$","10-100$","100-1000$","1000-10000$",">10000$"]
bucket_vol = pd.cut(user_volumes, bins=[-float("inf")]+bins_vol, labels=labels_vol, right=True).value_counts().reset_index()
bucket_vol.columns = ["range","users"]
st.plotly_chart(px.pie(bucket_vol, names="range", values="users", hole=0.5, title="Users by Volume"), use_container_width=True)

