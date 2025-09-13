import streamlit as st
import pandas as pd
import requests
import time
import plotly.express as px

# --- Page Config ------------------------------------------------------------------------------------------------------
st.set_page_config(
    page_title="Axelar Interchain Token Service (ITS)",
    page_icon="https://pbs.twimg.com/profile_images/1869486848646537216/rs71wCQo_400x400.jpg", layout="wide")

# --- Title -----------------------------------------------------------------------------------------------------
st.title("✨ITS Tokens")

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

# --- Convert date to unix (sec) ----------------------------------------------------------------------------------
def to_unix_timestamp(dt):
    return int(time.mktime(dt.timetuple()))

# --- Getting APIs -----------------------------------------------------------------------------------------
@st.cache_data
def load_data(start_date, end_date):
    from_time = to_unix_timestamp(pd.to_datetime(start_date))
    to_time = to_unix_timestamp(pd.to_datetime(end_date))

    url_tx = f"https://api.axelarscan.io/gmp/GMPTopITSAssets?fromTime={from_time}&toTime={to_time}"
    tx_data = requests.get(url_tx).json().get("data", [])

    url_assets = "https://api.axelarscan.io/api/getITSAssets"
    assets_data = requests.get(url_assets).json()

    address_to_symbol = {}
    symbol_to_image = {}
    for asset in assets_data:
        symbol = asset.get("symbol", "")
        image = asset.get("image", "")
        symbol_to_image[symbol] = image
        addresses = asset.get("addresses", [])
        if isinstance(addresses, str):
            try:
                addresses = eval(addresses)
            except:
                addresses = []
        for addr in addresses:
            address_to_symbol[addr.lower()] = symbol

    df = pd.DataFrame(tx_data)
    if df.empty:
        return pd.DataFrame(columns=["Token Address", "Symbol", "Logo", "Number of Transfers", "Volume of Transfers"]), {}

    df["Token Address"] = df["key"]
    df["Symbol"] = df["key"].str.lower().map(address_to_symbol).fillna("Unknown")
    df["Logo"] = df["Symbol"].map(symbol_to_image).fillna("")
    df["Number of Transfers"] = df["num_txs"].astype(int)
    df["Volume of Transfers"] = df["volume"].astype(float)

    df = df[["Token Address", "Symbol", "Logo", "Number of Transfers", "Volume of Transfers"]]

    return df, symbol_to_image

# --- Main Run ------------------------------------------------------------------------------------------------------
st.set_page_config(page_title="ITS Dashboard", layout="wide")
st.title("✨ Interchain Token Service (ITS) Dashboard")

col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("Start Date", value=pd.to_datetime("2023-12-01"))
with col2:
    end_date = st.date_input("End Date", value=pd.to_datetime("2025-09-30"))

df, symbol_to_image = load_data(start_date, end_date)

if df.empty:
    st.warning("⛔ No data available for the selected time range.")
else:

    df_display = df.copy()
    df_display["Number of Transfers"] = df_display["Number of Transfers"].map("{:,}".format)
    df_display["Volume of Transfers"] = df_display["Volume of Transfers"].map("{:,.0f}".format)

    def logo_html(url):
        if url:
            return f'<img src="{url}" style="width:20px;height:20px;border-radius:50%;">'
        return ""

    df_display["Logo"] = df_display["Logo"].apply(logo_html)

    st.subheader("📑 Interchain Token Transfers Table")

    scrollable_table = f"""
    <div style="max-height:700px; overflow-y:auto;">
        {df_display.to_html(escape=False, index=False)}
    </div>
    """

    st.write(scrollable_table, unsafe_allow_html=True)

    # --- chart 1: Top 10 by Volume (without Unknown) -------------------------------------------------------------------
    df_grouped = (
        df[df["Symbol"] != "Unknown"]
        .groupby("Symbol", as_index=False)
        .agg({
            "Number of Transfers": "sum",
            "Volume of Transfers": "sum"
        })
    )

    top_volume = df_grouped.sort_values("Volume of Transfers", ascending=False).head(10)
    fig1 = px.bar(
        top_volume,
        x="Symbol",
        y="Volume of Transfers",
        text="Volume of Transfers",
        color="Symbol"
    )
    fig1.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
    fig1.update_layout(
        title="Top 10 Tokens by Interchain Transfers Volume",
        xaxis_title=" ",
        yaxis_title="$USD",
        showlegend=False
    )

    # --- chart2: Top 10 by Transfers Count (without Unknown + volume > 0) ------------------------------------------------
    df_nonzero = df_grouped[df_grouped["Volume of Transfers"] > 0]
    top_transfers = df_nonzero.sort_values("Number of Transfers", ascending=False).head(10)

    fig2 = px.bar(
        top_transfers,
        x="Symbol",
        y="Number of Transfers",
        text="Number of Transfers",
        color="Symbol"
    )
    fig2.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
    fig2.update_layout(
        title="Top 10 Tokens by Interchain Transfers Count",
        xaxis_title=" ",
        yaxis_title="Transfers count",
        showlegend=False
    )

    st.plotly_chart(fig1, use_container_width=True)
    st.plotly_chart(fig2, use_container_width=True)
