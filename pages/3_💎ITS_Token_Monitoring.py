import streamlit as st
import pandas as pd
import requests
import plotly.express as px
import time

# ------------------------
# API endpoints
# ------------------------
ASSETS_API = "https://api.axelarscan.io/api/getITSAssets"
GMP_API = "https://api.axelarscan.io/gmp/searchGMP"

# ------------------------
# Helper functions
# ------------------------
def fetch_assets():
    resp = requests.get(ASSETS_API)
    if resp.status_code == 200:
        return pd.DataFrame(resp.json())
    return pd.DataFrame()


def fetch_gmp_data(symbol="XRP", from_time=None, to_time=None):
    params = {"symbol": symbol}
    if from_time:
        params["fromTime"] = int(from_time)
    if to_time:
        params["toTime"] = int(to_time)
    resp = requests.get(GMP_API, params=params)
    if resp.status_code == 200:
        data = resp.json().get("data", [])
        return pd.DataFrame(data)
    return pd.DataFrame()


def preprocess(df, timeframe):
    if df.empty:
        return df
    # Extract interchain transfer details
    transfers = df["interchain_transfer"].dropna().apply(pd.Series)
    df = pd.concat([df, transfers], axis=1)
    # Convert timestamp
    df["timestamp"] = pd.to_datetime(df["executed"].apply(lambda x: x.get("block_timestamp") if isinstance(x, dict) else None), unit="s")
    df = df.dropna(subset=["timestamp"])

    if timeframe == "Day":
        df["period"] = df["timestamp"].dt.to_period("D").dt.to_timestamp()
    elif timeframe == "Week":
        df["period"] = df["timestamp"].dt.to_period("W").dt.start_time
    else:  # Month
        df["period"] = df["timestamp"].dt.to_period("M").dt.to_timestamp()

    # Amount to numeric
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["fee"] = pd.to_numeric(df["fees"].apply(lambda x: x.get("base_fee_usd") if isinstance(x, dict) else None), errors="coerce")

    return df

# ------------------------
# Streamlit UI
# ------------------------
st.set_page_config(page_title="Interchain Token Service", layout="wide")
st.title("Interchain Token Service Dashboard")

# Load assets list
assets_df = fetch_assets()
symbols = sorted(assets_df["symbol"].unique()) if not assets_df.empty else ["XRP"]

# Filters
selected_symbol = st.sidebar.selectbox("Select Token", symbols, index=symbols.index("XRP") if "XRP" in symbols else 0)
from_date = st.sidebar.date_input("From Date")
to_date = st.sidebar.date_input("To Date")
timeframe = st.sidebar.selectbox("Timeframe", ["Day", "Week", "Month"], index=2)

from_unix = int(time.mktime(pd.to_datetime(from_date).timetuple())) if from_date else None
to_unix = int(time.mktime(pd.to_datetime(to_date).timetuple())) if to_date else None

# Fetch data
df = fetch_gmp_data(symbol=selected_symbol, from_time=from_unix, to_time=to_unix)
df = preprocess(df, timeframe)

if df.empty:
    st.warning("No data available for the selected filters.")
    st.stop()

# ------------------------
# KPIs
# ------------------------
total_volume = df["value"].sum()
total_txs = len(df)
unique_users = df["sourceAddress"].nunique()
total_fees = df["fee"].sum()
unique_paths = df[["sourceChain", "destinationChain"]].dropna().drop_duplicates().shape[0]
unique_source_chains = df["sourceChain"].nunique()
unique_dest_chains = df["destinationChain"].nunique()
median_fee = df["fee"].median()

kpi1, kpi2, kpi3, kpi4 = st.columns(4)
with kpi1: st.metric("Total Volume ($)", f"{total_volume:,.2f}")
with kpi2: st.metric("Total Transactions", f"{total_txs:,}")
with kpi3: st.metric("Unique Users", f"{unique_users:,}")
with kpi4: st.metric("Total Fees ($)", f"{total_fees:,.2f}")

kpi5, kpi6, kpi7, kpi8 = st.columns(4)
with kpi5: st.metric("Unique Paths", f"{unique_paths:,}")
with kpi6: st.metric("Source Chains", f"{unique_source_chains:,}")
with kpi7: st.metric("Destination Chains", f"{unique_dest_chains:,}")
with kpi8: st.metric("Median Fee ($)", f"{median_fee:,.2f}")

# ------------------------
# Charts Over Time
# ------------------------
st.subheader("Trends Over Time")

col1, col2 = st.columns(2)
with col1:
    st.plotly_chart(px.line(df, x="period", y="value", title="Volume Over Time"), use_container_width=True)
    st.plotly_chart(px.line(df, x="period", y=df.groupby("period")["sourceAddress"].transform("nunique"), title="Unique Users Over Time"), use_container_width=True)

with col2:
    st.plotly_chart(px.line(df, x="period", y=df.groupby("period")["amount"].transform("count"), title="Transactions Over Time"), use_container_width=True)
    st.plotly_chart(px.line(df, x="period", y="fee", title="Fees Over Time"), use_container_width=True)

# ------------------------
# Grouped by Path
# ------------------------
st.subheader("Grouped by Cross-chain Path")

df["path"] = df["sourceChain"] + " → " + df["destinationChain"]

col3, col4 = st.columns(2)
with col3:
    st.plotly_chart(px.bar(df, x="period", y="amount", color="path", title="Transactions by Path", barmode="stack"), use_container_width=True)
    st.plotly_chart(px.bar(df, x="period", y="value", color="path", title="Volume by Path", barmode="stack"), use_container_width=True)

with col4:
    st.plotly_chart(px.bar(df, x="period", y=df.groupby(["period", "path"])["sourceAddress"].transform("nunique"), color="path", title="Users by Path", barmode="stack"), use_container_width=True)
    st.plotly_chart(px.bar(df, x="period", y=df.groupby(["period", "path"])["fee"].transform("sum"), color="path", title="Fees by Path", barmode="stack"), use_container_width=True)

# ------------------------
# Totals by Path
# ------------------------
st.subheader("Totals by Cross-chain Path")

path_group = df.groupby("path").agg({"value":"sum", "amount":"count", "sourceAddress":"nunique", "fee":"sum"}).reset_index()

col5, col6 = st.columns(2)
with col5:
    st.plotly_chart(px.pie(path_group, names="path", values="value", title="Total Volume by Path"), use_container_width=True)
    st.plotly_chart(px.bar(path_group, x="path", y="sourceAddress", title="Total Users by Path"), use_container_width=True)

with col6:
    st.plotly_chart(px.pie(path_group, names="path", values="amount", title="Total Transactions by Path"), use_container_width=True)
    st.plotly_chart(px.bar(path_group, x="path", y="fee", title="Total Fees by Path"), use_container_width=True)

# ------------------------
# Top Users
# ------------------------
st.subheader("Top Users")

user_group = df.groupby("sourceAddress").agg({"amount":"count", "value":"sum"}).reset_index()

col7, col8 = st.columns(2)
with col7:
    top_users_tx = user_group.sort_values("amount", ascending=False).head(10)
    st.plotly_chart(px.bar(top_users_tx, x="amount", y="sourceAddress", orientation="h", title="Top Users by Transactions"), use_container_width=True)

with col8:
    top_users_vol = user_group.sort_values("value", ascending=False).head(10)
    st.plotly_chart(px.bar(top_users_vol, x="value", y="sourceAddress", orientation="h", title="Top Users by Volume"), use_container_width=True)

# ------------------------
# User Categorization
# ------------------------
st.subheader("User Categorization")

# Categorize by transactions
user_group["tx_bucket"] = pd.cut(user_group["amount"], bins=[0,1,5,10,20,50,100,float("inf")], labels=["1 Tx", "2-5 Tx", "6-10 Tx", "11-20 Tx", "21-50 Tx", "51-100 Tx", ">100 Tx"])
cat_tx = user_group.groupby("tx_bucket").size().reset_index(name="count")
st.plotly_chart(px.pie(cat_tx, names="tx_bucket", values="count", hole=0.4, title="Users by Transaction Count"), use_container_width=True)

# Categorize by volume
user_group["vol_bucket"] = pd.cut(user_group["value"], bins=[0,1,10,100,1000,10000,float("inf")], labels=["< $1","$1-$10","$10-$100","$100-$1k","$1k-$10k","> $10k"])
cat_vol = user_group.groupby("vol_bucket").size().reset_index(name="count")
st.plotly_chart(px.pie(cat_vol, names="vol_bucket", values="count", hole=0.4, title="Users by Volume"), use_container_width=True)
