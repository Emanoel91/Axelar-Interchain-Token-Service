import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.express as px
import plotly.graph_objects as go
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import requests
import matplotlib.pyplot as plt

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
        margin-left: 5px;
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
        SELECT created_at, LOWER(data:call.chain::STRING) AS source_chain,
               LOWER(data:call.returnValues.destinationChain::STRING) AS destination_chain,
               data:call.transaction.from::STRING AS user,
               CASE WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
                    WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
                    ELSE NULL END AS amount,
               CASE WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
                    WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
                    ELSE NULL END AS amount_usd,
               COALESCE(CASE WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
                              OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) 
                              OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) THEN NULL
                             WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
                              AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
                             THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) 
                                  * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
                             ELSE NULL END,
                        CASE WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
                             WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL 
                             THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
                             ELSE NULL END) AS fee,
               id, data:symbol::STRING AS symbol
        FROM axelar.axelscan.fact_gmp 
        WHERE status = 'executed' AND simplified_status = 'received'
          AND (data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%'
               OR data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%')
    )
    SELECT count(distinct user) as "Unique Users",
           count(distinct (source_chain || '➡' || destination_chain)) as "Paths", 
           count(distinct symbol) as "Tokens",
           round(sum(fee)) as "Total Transfer Fees"
    FROM axelar_service
    WHERE created_at::date>='{start_str}' AND created_at::date<='{end_str}'
    """
    df = pd.read_sql(query, conn)
    return df

# --- Load Data --------------------------------------------------------------------------------------------------------------------
df_interchain_stats = load_interchain_stats(start_date, end_date)

# --- Axelarscan API ---------------------------------------------------------------------------------------------------------------
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

# --- Aggregations for Source, Destination, Path --------------------------------------------------------------------
source_agg = df_all.groupby("source_chain").agg({"num_txs": "sum", "volume": "sum"}).reset_index()
destination_agg = df_all.groupby("destination_chain").agg({"num_txs": "sum", "volume": "sum"}).reset_index()
path_agg = df_all.groupby(["source_chain", "destination_chain"]).agg({"num_txs": "sum", "volume": "sum"}).reset_index()

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

col4, col5, col6 = st.columns(3)
with col4:
    st.markdown(card_style.format(label="Unique Paths", value=f"{df_interchain_stats['Paths'][0]:,}"), unsafe_allow_html=True)

with col5:
    st.markdown(card_style.format(label="Number of Tokens", value=f"{df_interchain_stats['Tokens'][0]:,}"), unsafe_allow_html=True)

with col6:
    st.markdown(card_style.format(label="Total Transfer Fees", value=f"${df_interchain_stats['Total Transfer Fees'][0]:,}"), unsafe_allow_html=True)

# --- Top 5 Charts ---------------------------------------------------------------------------------------------------
st.subheader("Top 5 Source Chains")
col1, col2 = st.columns(2)
with col1:
    top5 = source_agg.sort_values("num_txs", ascending=False).head(5)
    fig, ax = plt.subplots()
    ax.bar(top5["source_chain"], top5["num_txs"])
    ax.set_title("Top 5 by Transactions")
    st.pyplot(fig)

with col2:
    top5 = source_agg.sort_values("volume", ascending=False).head(5)
    fig, ax = plt.subplots()
    ax.bar(top5["source_chain"], top5["volume"])
    ax.set_title("Top 5 by Volume")
    st.pyplot(fig)

st.subheader("Top 5 Destination Chains")
col1, col2 = st.columns(2)
with col1:
    top5 = destination_agg.sort_values("num_txs", ascending=False).head(5)
    fig, ax = plt.subplots()
    ax.bar(top5["destination_chain"], top5["num_txs"])
    ax.set_title("Top 5 by Transactions")
    st.pyplot(fig)

with col2:
    top5 = destination_agg.sort_values("volume", ascending=False).head(5)
    fig, ax = plt.subplots()
    ax.bar(top5["destination_chain"], top5["volume"])
    ax.set_title("Top 5 by Volume")
    st.pyplot(fig)

st.subheader("Top 5 Paths (Source → Destination)")
col1, col2 = st.columns(2)
with col1:
    top5 = path_agg.sort_values("num_txs", ascending=False).head(5)
    fig, ax = plt.subplots()
    ax.bar([f"{r['source_chain']}→{r['destination_chain']}" for _, r in top5.iterrows()], top5["num_txs"])
    ax.set_title("Top 5 by Transactions")
    st.pyplot(fig)

with col2:
    top5 = path_agg.sort_values("volume", ascending=False).head(5)
    fig, ax = plt.subplots()
    ax.bar([f"{r['source_chain']}→{r['destination_chain']}" for _, r in top5.iterrows()], top5["volume"])
    ax.set_title("Top 5 by Volume")
    st.pyplot(fig)
