# app.py
import streamlit as st
import pandas as pd
import requests
import plotly.express as px
import datetime
import time

# ------------------------
# Config / Endpoints
# ------------------------
ASSETS_API = "https://api.axelarscan.io/api/getITSAssets"
GMP_API = "https://api.axelarscan.io/gmp/searchGMP"

st.set_page_config(page_title="Interchain Token Service", layout="wide")
st.title("Interchain Token Service Dashboard")

# ------------------------
# Helpers: fetch data
# ------------------------
def fetch_assets():
    try:
        r = requests.get(ASSETS_API, timeout=15)
        r.raise_for_status()
        data = r.json()
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()
    except Exception as e:
        st.error(f"Error fetching assets: {e}")
        return pd.DataFrame()

def fetch_gmp_data(symbol="XRP", from_time=None, to_time=None):
    params = {}
    if symbol:
        params["symbol"] = symbol
    if from_time is not None:
        params["fromTime"] = int(from_time)
    if to_time is not None:
        params["toTime"] = int(to_time)
    try:
        r = requests.get(GMP_API, params=params, timeout=30)
        r.raise_for_status()
        payload = r.json()
        # API returns {"data": [...], "total": ...}
        if isinstance(payload, dict) and "data" in payload:
            return pd.DataFrame(payload["data"])
        if isinstance(payload, list):
            return pd.DataFrame(payload)
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error fetching GMP data: {e}")
        return pd.DataFrame()

# ------------------------
# Preprocess (extract useful fields robustly)
# ------------------------
def preprocess(df, timeframe):
    if df.empty:
        return df

    # expand interchain_transfer if present
    if "interchain_transfer" in df.columns:
        transfers = df["interchain_transfer"].apply(lambda x: x if isinstance(x, dict) else {})
        transfers = transfers.apply(pd.Series)
    else:
        transfers = pd.DataFrame(index=df.index)

    def get_col(name):
        if name in df.columns:
            return df[name]
        if name in transfers.columns:
            return transfers[name]
        return pd.Series([None] * len(df), index=df.index)

    # canonical columns
    df["sourceAddress"] = get_col("sourceAddress")
    df["destinationAddress"] = get_col("destinationAddress")
    # chains
    src_chain = get_col("sourceChain").fillna(df.get("origin_chain"))
    dst_chain = get_col("destinationChain").fillna(df.get("destination_chain")).fillna(df.get("callback_chain"))
    df["sourceChain"] = src_chain
    df["destinationChain"] = dst_chain
    df["path"] = df["sourceChain"].fillna("Unknown").astype(str) + " → " + df["destinationChain"].fillna("Unknown").astype(str)

    # amounts / values / fees
    df["tx_amount"] = pd.to_numeric(get_col("amount"), errors="coerce")
    if df["tx_amount"].isna().all() and "amount" in df.columns:
        df["tx_amount"] = pd.to_numeric(df["amount"], errors="coerce")

    df["value"] = pd.to_numeric(get_col("value"), errors="coerce")
    if df["value"].isna().all() and "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # fees: try base_fee_usd or express_fee_usd or fallback
    def extract_fee_cell(x):
        if isinstance(x, dict):
            return x.get("base_fee_usd") or x.get("express_fee_usd") or x.get("express_fee") or x.get("fee") or None
        return None
    if "fees" in df.columns:
        df["fee"] = pd.to_numeric(df["fees"].apply(extract_fee_cell), errors="coerce")
    else:
        df["fee"] = pd.Series([None] * len(df), index=df.index)

    # timestamp: try multiple nested fields (executed.block_timestamp, transaction.timestamp, etc.)
    def extract_ts_row(row):
        # row is a Series; check common sub-objects
        for key in ["executed", "call", "approved", "confirm", "transaction"]:
            val = row.get(key)
            if isinstance(val, dict):
                for candidate in ["block_timestamp", "timestamp", "l1Timestamp", "l1_timestamp"]:
                    t = val.get(candidate)
                    if t:
                        try:
                            return int(t)
                        except:
                            pass
        # top-level fallback
        for candidate in ["block_timestamp", "timestamp"]:
            t = row.get(candidate)
            if t:
                try:
                    return int(t)
                except:
                    pass
        return None

    ts_series = df.apply(extract_ts_row, axis=1)
    df["timestamp"] = pd.to_datetime(ts_series, unit="s", errors="coerce")
    df = df.dropna(subset=["timestamp"]).copy()

    # period based on requested timeframe
    if timeframe == "Day":
        df["period"] = df["timestamp"].dt.to_period("D").dt.to_timestamp()
    elif timeframe == "Week":
        df["period"] = df["timestamp"].dt.to_period("W").dt.start_time
    else:
        df["period"] = df["timestamp"].dt.to_period("M").dt.to_timestamp()

    # ensure sourceAddress exists (fallbacks)
    if "sourceAddress" not in df.columns or df["sourceAddress"].isna().all():
        if "from" in df.columns:
            df["sourceAddress"] = df["from"]
    return df

# ------------------------
# Main UI: Filters (on main page)
# ------------------------
assets_df = fetch_assets()
symbols = sorted([s for s in assets_df["symbol"].dropna().unique()]) if not assets_df.empty else ["XRP"]
if "XRP" not in symbols:
    symbols = ["XRP"] + symbols

# default date range: from (today - 30 days) to (today - 1 day)
today = datetime.date.today()
default_end = today - datetime.timedelta(days=1)
default_start = today - datetime.timedelta(days=30)

st.markdown("### Filters")
fcol1, fcol2, fcol3 = st.columns([2,2,1])
with fcol1:
    selected_symbol = st.selectbox("Token (symbol)", symbols, index=symbols.index("XRP") if "XRP" in symbols else 0)
with fcol2:
    date_range = st.date_input("Date range (start → end)", value=(default_start, default_end))
    # date_input with tuple returns tuple (start, end)
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date = date_range
        end_date = date_range
with fcol3:
    timeframe = st.selectbox("Timeframe", ["Day", "Week", "Month"], index=2)

# convert to unix (start at 00:00:00, end at 23:59:59)
from_unix = int(datetime.datetime.combine(start_date, datetime.time.min).timestamp())
to_unix = int(datetime.datetime.combine(end_date, datetime.time.max).timestamp())

# ------------------------
# Fetch & preprocess
# ------------------------
with st.spinner("Fetching data..."):
    raw = fetch_gmp_data(symbol=selected_symbol, from_time=from_unix, to_time=to_unix)
    df = preprocess(raw, timeframe)

if df.empty:
    st.warning("هیچ داده‌ای برای فیلترهای انتخاب‌شده یافت نشد.")
    st.stop()

# ------------------------
# Aggregations for charts (avoid passing transform(...) directly to plotly)
# ------------------------
# period-level aggregation
period_agg = (
    df.groupby("period", as_index=False)
      .agg(total_volume=("value", "sum"),
           total_txs=("tx_amount", "count"),
           unique_users=("sourceAddress", "nunique"),
           total_fees=("fee", "sum"))
      .sort_values("period")
)

# period x path aggregation (for stacked charts)
period_path = (
    df.groupby(["period", "path"], as_index=False)
      .agg(volume=("value", "sum"),
           tx_count=("tx_amount", "count"),
           users=("sourceAddress", "nunique"),
           fees=("fee", "sum"))
)

# totals by path
path_group = (
    df.groupby("path", as_index=False)
      .agg(total_volume=("value", "sum"),
           total_txs=("tx_amount", "count"),
           total_users=("sourceAddress", "nunique"),
           total_fees=("fee", "sum"))
      .sort_values("total_volume", ascending=False)
)

# top users
user_group = (
    df.groupby("sourceAddress", as_index=False)
      .agg(tx_count=("tx_amount", "count"),
           volume=("value", "sum"))
      .dropna(subset=["sourceAddress"])
)

# ------------------------
# KPIs
# ------------------------
st.subheader("KPIs")
kpi1, kpi2, kpi3, kpi4 = st.columns(4)
kpi1.metric("Total Volume ($)", f"{period_agg['total_volume'].sum():,.2f}")
kpi2.metric("Total Transactions", f"{period_agg['total_txs'].sum():,}")
kpi3.metric("Unique Users (period)", f"{period_agg['unique_users'].sum():,}")
kpi4.metric("Total Fees ($)", f"{period_agg['total_fees'].sum():,.2f}")

kpi5, kpi6, kpi7, kpi8 = st.columns(4)
unique_paths = df[["sourceChain", "destinationChain"]].dropna().drop_duplicates().shape[0]
kpi5.metric("Unique Paths", f"{unique_paths:,}")
kpi6.metric("Source Chains", f"{df['sourceChain'].nunique():,}")
kpi7.metric("Destination Chains", f"{df['destinationChain'].nunique():,}")
kpi8.metric("Median Fee ($)", f"{df['fee'].median():,.4f}" if not df["fee"].isna().all() else "N/A")

# ------------------------
# Time series charts
# ------------------------
st.subheader("Trends Over Time")
c1, c2 = st.columns(2)
with c1:
    fig_vol = px.line(period_agg, x="period", y="total_volume", title="Volume Over Time")
    st.plotly_chart(fig_vol, use_container_width=True)
    fig_users = px.line(period_agg, x="period", y="unique_users", title="Unique Users Over Time")
    st.plotly_chart(fig_users, use_container_width=True)
with c2:
    fig_txs = px.line(period_agg, x="period", y="total_txs", title="Transactions Over Time")
    st.plotly_chart(fig_txs, use_container_width=True)
    fig_fees = px.line(period_agg, x="period", y="total_fees", title="Fees Over Time")
    st.plotly_chart(fig_fees, use_container_width=True)

# ------------------------
# Stacked charts by path
# ------------------------
st.subheader("By Cross-chain Path (stacked)")
p1, p2 = st.columns(2)
with p1:
    fig_tx_by_path = px.bar(period_path, x="period", y="tx_count", color="path", title="Transactions by Path (stacked)", barmode="stack")
    st.plotly_chart(fig_tx_by_path, use_container_width=True)
    fig_vol_by_path = px.bar(period_path, x="period", y="volume", color="path", title="Volume by Path (stacked)", barmode="stack")
    st.plotly_chart(fig_vol_by_path, use_container_width=True)
with p2:
    fig_users_by_path = px.bar(period_path, x="period", y="users", color="path", title="Users by Path (stacked)", barmode="stack")
    st.plotly_chart(fig_users_by_path, use_container_width=True)
    fig_fees_by_path = px.bar(period_path, x="period", y="fees", color="path", title="Fees by Path (stacked)", barmode="stack")
    st.plotly_chart(fig_fees_by_path, use_container_width=True)

# ------------------------
# Totals by path (pie / bar)
# ------------------------
st.subheader("Totals by Path")
t1, t2 = st.columns(2)
with t1:
    fig_pie_vol = px.pie(path_group, names="path", values="total_volume", title="Total Volume by Path")
    st.plotly_chart(fig_pie_vol, use_container_width=True)
    fig_bar_users = px.bar(path_group, x="path", y="total_users", title="Total Users by Path")
    st.plotly_chart(fig_bar_users, use_container_width=True)
with t2:
    fig_pie_txs = px.pie(path_group, names="path", values="total_txs", title="Total Transactions by Path")
    st.plotly_chart(fig_pie_txs, use_container_width=True)
    fig_bar_fees = px.bar(path_group, x="path", y="total_fees", title="Total Fees by Path ($)")
    st.plotly_chart(fig_bar_fees, use_container_width=True)

# ------------------------
# Top users
# ------------------------
st.subheader("Top Users")
u1, u2 = st.columns(2)
top_users_tx = user_group.sort_values("tx_count", ascending=False).head(10)
top_users_vol = user_group.sort_values("volume", ascending=False).head(10)
with u1:
    fig_top_tx = px.bar(top_users_tx, x="tx_count", y="sourceAddress", orientation="h", title="Top Users by Transactions")
    st.plotly_chart(fig_top_tx, use_container_width=True)
with u2:
    fig_top_vol = px.bar(top_users_vol, x="volume", y="sourceAddress", orientation="h", title="Top Users by Volume ($)")
    st.plotly_chart(fig_top_vol, use_container_width=True)

# ------------------------
# User categorization (donut charts)
# ------------------------
st.subheader("User Categorization")
# tx buckets
user_group["tx_bucket"] = pd.cut(user_group["tx_count"],
                                 bins=[0,1,5,10,20,50,100,float("inf")],
                                 labels=["1 Tx","2-5 Tx","6-10 Tx","11-20 Tx","21-50 Tx","51-100 Tx",">100 Tx"])
tx_cat = user_group.groupby("tx_bucket", dropna=False).size().reset_index(name="count")
fig_tx_cat = px.pie(tx_cat, names="tx_bucket", values="count", hole=0.4, title="Users by Transaction Count")
st.plotly_chart(fig_tx_cat, use_container_width=True)

# volume buckets
user_group["vol_bucket"] = pd.cut(user_group["volume"].fillna(0),
                                  bins=[0,1,10,100,1000,10000,float("inf")],
                                  labels=["< $1","$1-$10","$10-$100","$100-$1k","$1k-$10k","> $10k"])
vol_cat = user_group.groupby("vol_bucket", dropna=False).size().reset_index(name="count")
fig_vol_cat = px.pie(vol_cat, names="vol_bucket", values="count", hole=0.4, title="Users by Volume")
st.plotly_chart(fig_vol_cat, use_container_width=True)
