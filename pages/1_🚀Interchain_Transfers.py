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
# --- Fetch Data from APIs --------------------------------------------------------------------------------------------------------
@st.cache_data
def load_interchain_stats(start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_service AS (
  
  SELECT  created_at, LOWER(data:call.chain::STRING) AS source_chain, LOWER(data:call.returnValues.destinationChain::STRING) AS destination_chain,
    data:call.transaction.from::STRING AS user, CASE 
      WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
      WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
      ELSE NULL
    END AS amount, CASE 
      WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
      WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
      ELSE NULL
    END AS amount_usd, COALESCE(CASE 
        WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
          OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
        THEN NULL
        WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
          AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
        THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
        ELSE NULL END, CASE 
        WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
        WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
        ELSE NULL END) AS fee, id, data:symbol::STRING AS Symbol
  FROM axelar.axelscan.fact_gmp 
  WHERE status = 'executed' AND simplified_status = 'received' AND (
        data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
        or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
        ))

SELECT count(distinct user) as "Unique Users", count(distinct (source_chain || '➡' || destination_chain)) as "Paths", 
count(distinct symbol) as "Tokens", round(sum(fee)) as "Total Transfer Fees"
FROM axelar_service
where created_at::date>='{start_str}' and created_at::date<='{end_str}'
    """

    df = pd.read_sql(query, conn)
    return df

# --- Load Data --------------------------------------------------------------------------------------------------------------------
df_interchain_stats = load_interchain_stats(start_date, end_date)
# ---Axelarscan api ----------------------------------------------------------------------------------------------------------------
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
card_style = """
    <div style="
        background-color: #f9f9f9;
        border: 1px solid #e0e0e0;
        border-radius: 12px;
        padding: 20px;
        text-align: center;
        box-shadow: 2px 2px 10px rgba(0,0,0,0.05);
        ">
        <h4 style="margin: 0; font-size: 20px; color: #555;">{label}</h4>
        <p style="margin: 5px 0 0; font-size: 20px; font-weight: bold; color: #000;">{value}</p>
    </div>
"""

col1, col2, col3 = st.columns(3)
with col1:
    st.markdown(card_style.format(label="Total Number of Transfers", value=f"{agg_df['num_txs'].sum():,} Txns"), unsafe_allow_html=True)

with col2:
    st.markdown(card_style.format(label="Total Volume of Transfers", value=f"${round(agg_df['volume'].sum()):,}"), unsafe_allow_html=True)

with col3:
    st.markdown(card_style.format(label="Unique Users", value=f"{df_interchain_stats['Unique Users'][0]:,} Wallets"), unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

col4, col5, col6 = st.columns(3)
with col4:
    st.markdown(card_style.format(label="Unique Paths", value=f"{df_interchain_stats['Paths'][0]:,}"), unsafe_allow_html=True)

with col5:
    st.markdown(card_style.format(label="Number of Tokens", value=f"{df_interchain_stats['Tokens'][0]:,}"), unsafe_allow_html=True)

with col6:
    st.markdown(card_style.format(label="Total Transfer Fees", value=f"${df_interchain_stats['Total Transfer Fees'][0]:,}"), unsafe_allow_html=True)

# --- Plots ----------------------------------------------------------------------------------------------------------
col1, col2 = st.columns(2)

# Number of Interchain Transfers Over Time
fig1 = go.Figure()
fig1.add_trace(go.Bar(x=agg_df["period"], y=agg_df["num_txs"], name="Transfers", yaxis="y1", marker_color="#ff7f27"))
fig1.add_trace(go.Scatter(x=agg_df["period"], y=agg_df["cum_num_txs"], name="Total Transfers", yaxis="y2", mode="lines", line=dict(color="black")))
fig1.update_layout(title="Number of Interchain Transfers Over Time", yaxis=dict(title="Txns count"), yaxis2=dict(title="Txns count", overlaying="y", side="right"),
    xaxis_title="", legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5))
col1.plotly_chart(fig1, use_container_width=True)

# Volume of Interchain Transfers Over Time
fig2 = go.Figure()
fig2.add_trace(go.Bar(x=agg_df["period"], y=agg_df["volume"], name="Volume", yaxis="y1", marker_color="#ff7f27"))
fig2.add_trace(go.Scatter(x=agg_df["period"], y=agg_df["cum_volume"],name="Total Volume", yaxis="y2", mode="lines", line=dict(color="black")))
fig2.update_layout(title="Volume of Interchain Transfers Over Time", yaxis=dict(title="$USD"), yaxis2=dict(title="$USD", overlaying="y", side="right"), xaxis_title="",
    legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5))
col2.plotly_chart(fig2, use_container_width=True)


@st.cache_data
def load_interchain_users_data(timeframe, start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    with table1 as (
    WITH tab1 AS (
    SELECT data:call.transaction.from::STRING AS user, min(created_at::date) as first_txn_date
    FROM axelar.axelscan.fact_gmp 
    WHERE status = 'executed' AND simplified_status = 'received' AND (
        data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
        or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
        )
    group by 1)
    select date_trunc('{timeframe}',first_txn_date) as "Date", count(distinct user) as "New Users", sum("New Users") over (order by "Date") as "User Growth"
    from tab1 
    where first_txn_date>='{start_str}' and first_txn_date<='{end_str}'
    group by 1),
    table2 as (SELECT date_trunc('{timeframe}',created_at) as "Date", count(distinct data:call.transaction.from::STRING) AS "Total Users"
    FROM axelar.axelscan.fact_gmp 
    WHERE created_at::date>='{start_str}' and created_at::date<='{end_str}' and status = 'executed' AND simplified_status = 'received' AND (
    data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
    or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
        ) 
    group by 1)
    select table1."Date" as "Date", "New Users", "Total Users", "Total Users"-"New Users" as "Returning Users", "User Growth",
    round((("New Users"/"Total Users")*100),1) as "%Growth Rate"
    from table1 left join table2 on table1."Date"=table2."Date"
    order by 1
    """

    df = pd.read_sql(query, conn)
    return df

@st.cache_data
def load_interchain_fees_data(timeframe, start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_service AS (  
    SELECT  
    created_at, COALESCE(CASE 
        WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
          OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
        THEN NULL
        WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
          AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
        THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
        ELSE NULL
      END, CASE 
        WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
        WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
        ELSE NULL
      END) AS fee,
    FROM axelar.axelscan.fact_gmp 
    WHERE status = 'executed'
    AND simplified_status = 'received'
    AND (
        data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
        or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
        ))
    SELECT date_trunc('month',created_at) as "Date", round(sum(fee)) as "Transfer Fees", sum("Transfer Fees") over (order by "Date") as "Total Transfer Fees",
    round(avg(fee),3) as "Average Gas Fee", round(median(fee),3) as "Median Gas Fee"
    FROM axelar_service
    group by 1
    order by 1
    """

    df = pd.read_sql(query, conn)
    return df

@st.cache_data
def load_interchain_fees_stats(start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_service AS (
  
  SELECT  
    created_at, COALESCE(CASE 
        WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
          OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
        THEN NULL
        WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
          AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
        THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
        ELSE NULL
      END, CASE 
        WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
        WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
        ELSE NULL
      END) AS fee,
  FROM axelar.axelscan.fact_gmp 
  WHERE status = 'executed'
    AND simplified_status = 'received'
    AND (
        data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
        or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
        ) 
)

SELECT round(avg(fee),2) as "Average Gas Fee", round(median(fee),2) as "Median Gas Fee"
FROM axelar_service
where created_at::date>='{start_str}' and created_at::date<='{end_str}'
    """

    df = pd.read_sql(query, conn)
    return df

# --- Load Data --------------------------------------------------------------------------------------------------------------------
df_interchain_users_data = load_interchain_users_data(timeframe, start_date, end_date)
df_interchain_fees_data = load_interchain_fees_data(timeframe, start_date, end_date)
df_interchain_fees_stats = load_interchain_fees_stats(start_date, end_date)
# ----------------------------------------------------------------------------------------------------------------------------------
col1, col2 = st.columns(2)

with col1:
    fig_b1 = go.Figure()
    # Stacked Bars
    fig_b1.add_trace(go.Bar(x=df_interchain_users_data["Date"], y=df_interchain_users_data["New Users"], name="New Users", marker_color="#0ed145"))
    fig_b1.add_trace(go.Bar(x=df_interchain_users_data["Date"], y=df_interchain_users_data["Returning Users"], name="Returning Users", marker_color="#ff7f27"))
    fig_b1.add_trace(go.Scatter(x=df_interchain_users_data["Date"], y=df_interchain_users_data["Total Users"], name="Total Users", mode="lines", line=dict(color="black", width=2)))
    fig_b1.update_layout(barmode="stack", title="Number of Users Over Time", yaxis=dict(title="Wallet count"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5))
    st.plotly_chart(fig_b1, use_container_width=True)

with col2:
    fig2 = px.area(df_interchain_users_data, x="Date", y="User Growth", title="Interchain Users Growth Over Time", color_discrete_sequence=["#ff7f27"])
    fig2.add_trace(go.Scatter(x=df_interchain_users_data["Date"], y=df_interchain_users_data["%Growth Rate"], name="%Growth Rate", mode="lines", yaxis="y2", line=dict(color="black")))
    fig2.update_layout(xaxis_title="", yaxis_title="wallet count",  yaxis2=dict(title="%", overlaying="y", side="right"), template="plotly_white")
    st.plotly_chart(fig2, use_container_width=True)

card_style = """
    <div style="
        background-color: #f9f9f9;
        border: 1px solid #e0e0e0;
        border-radius: 12px;
        padding: 20px;
        text-align: center;
        box-shadow: 2px 2px 10px rgba(0,0,0,0.05);
        ">
        <h4 style="margin: 0; font-size: 20px; color: #555;">{label}</h4>
        <p style="margin: 5px 0 0; font-size: 20px; font-weight: bold; color: #000;">{value}</p>
    </div>
"""

col3, col4 = st.columns(2)
with col3:
    st.markdown(card_style.format(label="Average Gas Fee", value=f"${df_interchain_fees_stats['Average Gas Fee'][0]:,}"), unsafe_allow_html=True)

with col4:
    st.markdown(card_style.format(label="Median Gas Fee", value=f"${df_interchain_fees_stats['Median Gas Fee'][0]:,}"), unsafe_allow_html=True)
    

col5, col6 = st.columns(2)

with col5:
    fig5 = go.Figure()
    fig5.add_bar(x=df_interchain_fees_data["Date"], y=df_interchain_fees_data["Transfer Fees"], name="Fee", yaxis="y1", marker_color="#ff7f27")
    fig5.add_trace(go.Scatter(x=df_interchain_fees_data["Date"], y=df_interchain_fees_data["Total Transfer Fees"], name="Total Fees", mode="lines", 
                              yaxis="y2", line=dict(color="black")))
    fig5.update_layout(title="Interchain Transfer Fees Over Time", yaxis=dict(title="$USD"), yaxis2=dict(title="$USD", overlaying="y", side="right"), xaxis=dict(title=""),
        barmode="group", legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5))
    st.plotly_chart(fig5, use_container_width=True)

with col6:
    fig6 = go.Figure()
    fig6.add_trace(go.Scatter(x=df_interchain_fees_data["Date"], y=df_interchain_fees_data["Average Gas Fee"], name="Avg Gas Fee", mode="lines", 
                              yaxis="y1", line=dict(color="blue")))
    fig6.add_trace(go.Scatter(x=df_interchain_fees_data["Date"], y=df_interchain_fees_data["Median Gas Fee"], name="Median Gas Fee", mode="lines", 
                              yaxis="y2", line=dict(color="green")))
    fig6.update_layout(title="Average & Median Transfer Fees Over Time", yaxis=dict(title="$USD"), yaxis2=dict(title="$USD", overlaying="y", side="right"), xaxis=dict(title=""),
        barmode="group", legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5))
    st.plotly_chart(fig6, use_container_width=True)

# --------------------------------------------------------------------------------------------------------------------------------------------------------
# --- Chains Analysis-------------------------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------------------------------------------

def to_timestamp(date):
    return int(pd.Timestamp(date).timestamp())

@st.cache_data
def load_chain_stats(start_date, end_date):
    from_time = to_timestamp(start_date)
    to_time = to_timestamp(end_date)

    api_urls = [
        f"https://api.axelarscan.io/gmp/GMPStatsByChains?contractAddress=0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C&fromTime={from_time}&toTime={to_time}",
        f"https://api.axelarscan.io/gmp/GMPStatsByChains?contractAddress=axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr&fromTime={from_time}&toTime={to_time}"
    ]

    all_sources = []
    all_destinations = []
    all_paths = []

    for url in api_urls:
        resp = requests.get(url)
        if resp.status_code == 200:
            data = resp.json()["source_chains"]
            for s in data:
                # source chain aggregation
                all_sources.append({
                    "source_chain": s["key"],
                    "num_txs": s.get("num_txs", 0),
                    "volume": s.get("volume", 0.0)
                })
                # destination chain aggregation
                for d in s["destination_chains"]:
                    all_destinations.append({
                        "destination_chain": d["key"],
                        "num_txs": d.get("num_txs", 0),
                        "volume": d.get("volume", 0.0)
                    })
                    # paths aggregation
                    all_paths.append({
                        "path": f"{s['key']} ➡ {d['key']}",
                        "num_txs": d.get("num_txs", 0),
                        "volume": d.get("volume", 0.0)
                    })

    df_sources = pd.DataFrame(all_sources).groupby("source_chain", as_index=False).sum()
    df_destinations = pd.DataFrame(all_destinations).groupby("destination_chain", as_index=False).sum()
    df_paths = pd.DataFrame(all_paths).groupby("path", as_index=False).sum()

    return df_sources, df_destinations, df_paths
    
# ------- Source Chains: Snowflake ------------------------------------
@st.cache_data
def load_source_chains_stats(start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_service AS (
  
  SELECT  
    created_at,
    LOWER(data:call.chain::STRING) AS source_chain,
    LOWER(data:call.returnValues.destinationChain::STRING) AS destination_chain,
    data:call.transaction.from::STRING AS user,

    CASE 
      WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
      WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
      ELSE NULL
    END AS amount,

    CASE 
      WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
      WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
      ELSE NULL
    END AS amount_usd,

    COALESCE(
      CASE 
        WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
          OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
        THEN NULL
        WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
          AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
        THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
        ELSE NULL
      END,
      CASE 
        WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
        WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
        ELSE NULL
      END
    ) AS fee,

    id,  
    data:symbol::STRING AS Symbol

  FROM axelar.axelscan.fact_gmp 
  WHERE status = 'executed'
    AND simplified_status = 'received'
    AND (
        data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
        or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
        ) 
)

SELECT source_chain as "Source Chain", count(distinct user) as "Number of Users", round(sum(fee)) as "Total Gas Fee"
FROM axelar_service
where created_at::date>='{start_str}' and created_at::date<='{end_str}'
group by 1
order by 2 desc 
    """

    df = pd.read_sql(query, conn)
    return df

# ------- Top 5: Source Chains: Snowflake ------------------------------------
@st.cache_data
def load_Top_source_chains_stats(start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_service AS (
  
  SELECT  
    created_at,
    LOWER(data:call.chain::STRING) AS source_chain,
    LOWER(data:call.returnValues.destinationChain::STRING) AS destination_chain,
    data:call.transaction.from::STRING AS user,

    CASE 
      WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
      WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
      ELSE NULL
    END AS amount,

    CASE 
      WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
      WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
      ELSE NULL
    END AS amount_usd,

    COALESCE(
      CASE 
        WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
          OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
        THEN NULL
        WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
          AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
        THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
        ELSE NULL
      END,
      CASE 
        WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
        WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
        ELSE NULL
      END
    ) AS fee,

    id,  
    data:symbol::STRING AS Symbol

  FROM axelar.axelscan.fact_gmp 
  WHERE status = 'executed'
    AND simplified_status = 'received'
    AND (
        data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
        or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
        ) 
)

SELECT source_chain as "Source Chain", count(distinct user) as "Number of Users", round(sum(fee)) as "Total Gas Fee"
FROM axelar_service
where created_at::date>='{start_str}' and created_at::date<='{end_str}'
group by 1
order by 2 desc 
limit 5
    """

    df = pd.read_sql(query, conn)
    return df

# --- Load Data ---------------------------------------------------------------
df_sources, df_destinations, df_paths = load_chain_stats(start_date, end_date)
df_source_chains_stats = load_source_chains_stats(start_date, end_date)
df_Top_source_chains_stats = load_Top_source_chains_stats(start_date, end_date)

# === Source Chains Tables ===================================================
col1, col2, col3 = st.columns(3)
with col1:
    st.subheader("Source Chains by Transactions")
    st.dataframe(df_sources.sort_values("num_txs", ascending=False).reset_index(drop=True))
with col2:
    st.subheader("Source Chains by Volume")
    st.dataframe(df_sources.sort_values("volume", ascending=False).reset_index(drop=True))
with col3:
    st.subheader("Source Chains by User")
    df_display = df_source_chains_stats.copy()
    df_display.index = df_display.index + 1
    df_display = df_display.applymap(lambda x: f"{x:,}" if isinstance(x, (int, float)) else x)
    st.dataframe(df_display, use_container_width=True)

# === Source Chains Charts ====
col1, col2, col3 = st.columns(3)
with col1:
    top5 = df_sources.sort_values("num_txs", ascending=False).head(5)
    fig = px.bar(top5, x="source_chain", y="num_txs", title="Top 5 Source Chains by Transactions", text="num_txs")
    st.plotly_chart(fig, use_container_width=True)
with col2:
    top5 = df_sources.sort_values("volume", ascending=False).head(5)
    fig = px.bar(top5, x="source_chain", y="volume", title="Top 5 Source Chains by Volume", text="volume")
    st.plotly_chart(fig, use_container_width=True)
with col3:
    fig = px.bar(df_Top_source_chains_stats, x="Source Chain", y="Number of Users", title="Top 5 Source Chains by Users", text="Number of Users")
    st.plotly_chart(fig, use_container_width=True)

# ------- Destination Chains: Snowflake ------------------------------------
@st.cache_data
def load_destination_chains_stats(start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_service AS (
  
  SELECT  
    created_at,
    LOWER(data:call.chain::STRING) AS source_chain,
    LOWER(data:call.returnValues.destinationChain::STRING) AS destination_chain,
    data:call.transaction.from::STRING AS user,

    CASE 
      WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
      WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
      ELSE NULL
    END AS amount,

    CASE 
      WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
      WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
      ELSE NULL
    END AS amount_usd,

    COALESCE(
      CASE 
        WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
          OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
        THEN NULL
        WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
          AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
        THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
        ELSE NULL
      END,
      CASE 
        WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
        WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
        ELSE NULL
      END
    ) AS fee,

    id,  
    data:symbol::STRING AS Symbol

  FROM axelar.axelscan.fact_gmp 
  WHERE status = 'executed'
    AND simplified_status = 'received'
    AND (
        data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
        or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
        ) 
)

SELECT destination_chain as "Destination Chain", count(distinct user) as "Number of Users", round(sum(fee)) as "Total Gas Fee"
FROM axelar_service
where created_at::date>='{start_str}' and created_at::date<='{end_str}'
group by 1
order by 2 desc 
    """

    df = pd.read_sql(query, conn)
    return df

# ------- Top 5: Destination Chains: Snowflake ------------------------------------
@st.cache_data
def load_top_destination_chains_stats(start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_service AS (
  
  SELECT  
    created_at,
    LOWER(data:call.chain::STRING) AS source_chain,
    LOWER(data:call.returnValues.destinationChain::STRING) AS destination_chain,
    data:call.transaction.from::STRING AS user,

    CASE 
      WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
      WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
      ELSE NULL
    END AS amount,

    CASE 
      WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
      WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
      ELSE NULL
    END AS amount_usd,

    COALESCE(
      CASE 
        WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
          OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
        THEN NULL
        WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
          AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
        THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
        ELSE NULL
      END,
      CASE 
        WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
        WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
        ELSE NULL
      END
    ) AS fee,

    id,  
    data:symbol::STRING AS Symbol

  FROM axelar.axelscan.fact_gmp 
  WHERE status = 'executed'
    AND simplified_status = 'received'
    AND (
        data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
        or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
        ) 
)

SELECT destination_chain as "Destination Chain", count(distinct user) as "Number of Users", round(sum(fee)) as "Total Gas Fee"
FROM axelar_service
where created_at::date>='{start_str}' and created_at::date<='{end_str}'
group by 1
order by 2 desc 
limit 5
    """

    df = pd.read_sql(query, conn)
    return df
# --- Load Data -------------------------------------------------------------------------
df_destination_chains_stats = load_destination_chains_stats(start_date, end_date)
df_top_destination_chains_stats = load_top_destination_chains_stats(start_date, end_date)

# === Destination Chains Tables =========================================================
col1, col2, col3 = st.columns(3)

# Destination Chains by Transactions
with col1:
    st.subheader("Destination Chains Sorted by Transactions")
    df_display1 = df_destinations[["destination_chain", "num_txs"]].copy()
    df_display1 = df_display1.sort_values("num_txs", ascending=False).reset_index(drop=True)
    df_display1.index = df_display1.index + 1  
    df_display1["num_txs"] = df_display1["num_txs"].apply(lambda x: f"{x:,}")  
    df_display1 = df_display1.rename(columns={
        "destination_chain": "Destination Chain",
        "num_txs": "Number of Transfers"
    })
    st.dataframe(df_display1, use_container_width=True)

# Destination Chains by Volume
with col2:
    st.subheader("Destination Chains Sorted by Volume")
    df_display2 = df_destinations[["destination_chain", "volume"]].copy()
    df_display2 = df_display2.sort_values("volume", ascending=False).reset_index(drop=True)
    df_display2.index = df_display2.index + 1  
    df_display2["volume"] = df_display2["volume"].apply(lambda x: f"{x:,.2f}") 
    df_display2 = df_display2.rename(columns={
        "destination_chain": "Destination Chain",
        "volume": "Volume of Transfers ($USD)"
    })
    st.dataframe(df_display2, use_container_width=True)

# Destination Chains by Users
with col3:
    st.subheader("Destination Chains Sorted by Users")
    df_display3 = df_destination_chains_stats.copy()
    df_display3.index = df_display3.index + 1
    df_display3 = df_display3.applymap(lambda x: f"{x:,}" if isinstance(x, (int, float)) else x)
    st.dataframe(df_display3, use_container_width=True)

# === Destination Chains Charts ===


# ------- Path: Snowflake ------------------------------------
@st.cache_data
def load_paths_stats(start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_service AS (
  
  SELECT  
    created_at,
    LOWER(data:call.chain::STRING) AS source_chain,
    LOWER(data:call.returnValues.destinationChain::STRING) AS destination_chain,
    data:call.transaction.from::STRING AS user,

    CASE 
      WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
      WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
      ELSE NULL
    END AS amount,

    CASE 
      WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
      WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
      ELSE NULL
    END AS amount_usd,

    COALESCE(
      CASE 
        WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
          OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
        THEN NULL
        WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
          AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
        THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
        ELSE NULL
      END,
      CASE 
        WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
        WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
        ELSE NULL
      END
    ) AS fee,

    id,  
    data:symbol::STRING AS Symbol

  FROM axelar.axelscan.fact_gmp 
  WHERE status = 'executed'
    AND simplified_status = 'received'
    AND (
        data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
        or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
        ) 
)

SELECT (source_chain || '➡' || destination_chain) as "Path", count(distinct user) as "Number of Users"
FROM axelar_service
where created_at::date>='{start_str}' and created_at::date<='{end_str}'
group by 1
order by 2 desc 
    """

    df = pd.read_sql(query, conn)
    return df

# ------- Top 5: Paths: Snowflake ------------------------------------
@st.cache_data
def load_top_paths_stats(start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_service AS (
  
  SELECT  
    created_at,
    LOWER(data:call.chain::STRING) AS source_chain,
    LOWER(data:call.returnValues.destinationChain::STRING) AS destination_chain,
    data:call.transaction.from::STRING AS user,

    CASE 
      WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
      WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
      ELSE NULL
    END AS amount,

    CASE 
      WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
      WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
      ELSE NULL
    END AS amount_usd,

    COALESCE(
      CASE 
        WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
          OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
        THEN NULL
        WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
          AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
        THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
        ELSE NULL
      END,
      CASE 
        WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
        WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
        ELSE NULL
      END
    ) AS fee,

    id,  
    data:symbol::STRING AS Symbol

  FROM axelar.axelscan.fact_gmp 
  WHERE status = 'executed'
    AND simplified_status = 'received'
    AND (
        data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
        or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
        ) 
)

SELECT (source_chain || '➡' || destination_chain) as "Path", count(distinct user) as "Number of Users"
FROM axelar_service
where created_at::date>='{start_str}' and created_at::date<='{end_str}'
group by 1
order by 2 desc 
limit 5
    """

    df = pd.read_sql(query, conn)
    return df
# --- Load Data -------------------------------------------------------------------------
df_paths_stats = load_paths_stats(start_date, end_date)
df_top_paths_stats = load_top_paths_stats(start_date, end_date)

# === Paths Tables ======================================================================
col1, col2, col3 = st.columns(3)

# Paths by Transactions
with col1:
    st.subheader("Paths Sorted by Transactions")
    df_display1 = df_paths[["path", "num_txs"]].copy()
    df_display1 = df_display1.sort_values("num_txs", ascending=False).reset_index(drop=True)
    df_display1.index = df_display1.index + 1  
    df_display1["num_txs"] = df_display1["num_txs"].apply(lambda x: f"{x:,}")  
    df_display1 = df_display1.rename(columns={
        "path": "Path",
        "num_txs": "Number of Transfers"
    })
    st.dataframe(df_display1, use_container_width=True)

# Paths by Volume
with col2:
    st.subheader("Paths Sorted by Volume")
    df_display2 = df_paths[["path", "volume"]].copy()
    df_display2 = df_display2.sort_values("volume", ascending=False).reset_index(drop=True)
    df_display2.index = df_display2.index + 1  
    df_display2["volume"] = df_display2["volume"].apply(lambda x: f"{x:,.2f}") 
    df_display2 = df_display2.rename(columns={
        "path": "Path",
        "volume": "Volume of Transfers ($USD)"
    })
    st.dataframe(df_display2, use_container_width=True)

# Paths by Users
with col3:
    st.subheader("Paths Sorted by Users")
    df_display3 = df_paths_stats.copy()
    df_display3.index = df_display3.index + 1
    df_display3 = df_display3.applymap(lambda x: f"{x:,}" if isinstance(x, (int, float)) else x)
    st.dataframe(df_display3, use_container_width=True)

# === Paths Charts ===================================================================================
col1, col2, col3 = st.columns(3)
with col1:
    top5 = df_paths.sort_values("num_txs", ascending=False).head(5)
    fig = px.bar(top5, x="path", y="num_txs", title="Top 5 Paths by Transactions", text="num_txs")
    st.plotly_chart(fig, use_container_width=True)
with col2:
    top5 = df_paths.sort_values("volume", ascending=False).head(5)
    fig = px.bar(top5, x="path", y="volume", title="Top 5 Paths by Volume", text="volume")
    st.plotly_chart(fig, use_container_width=True)
with col3:
    fig = px.bar(df_top_paths_stats, x="Path", y="Number of Users", title="Top 5 Paths by Users", text="Number of Users")
    st.plotly_chart(fig, use_container_width=True)
