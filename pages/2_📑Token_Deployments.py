import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.express as px
import plotly.graph_objects as go
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import networkx as nx

# --- Page Config ------------------------------------------------------------------------------------------------------
st.set_page_config(
    page_title="Axelar Interchain Token Service (ITS)",
    page_icon="https://pbs.twimg.com/profile_images/1869486848646537216/rs71wCQo_400x400.jpg",
    layout="wide"
)

# --- Title -----------------------------------------------------------------------------------------------------
st.title("📑Token Deployments")

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
    start_date = st.date_input("Start Date", value=pd.to_datetime("2023-12-01"))

with col3:
    end_date = st.date_input("End Date", value=pd.to_datetime("2025-09-30"))


# --- Row 1 ------------------------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_deploy_stats(start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    with table1 as (
SELECT data:interchain_token_deployment_started:tokenId as token, 
data:call:transaction:from as deployer, COALESCE(CASE 
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
      END) AS fee
FROM axelar.axelscan.fact_gmp 
WHERE status = 'executed' AND simplified_status = 'received' AND (
data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
) AND data:interchain_token_deployment_started:event='InterchainTokenDeploymentStarted'
and created_at::date>='{start_str}' and created_at::date<='{end_str}')

select count(distinct token) as "Total Number of Deployed Tokens",
count(distinct deployer) as "Total Number of Token Deployers",
round(sum(fee)) as "Total Gas Fees"
from table1

    """

    df = pd.read_sql(query, conn)
    return df

# === Load Data: Row 1 =================================================
df_deploy_stats = load_deploy_stats(start_date, end_date)
# === KPIs: Row 1 ======================================================
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
    st.markdown(card_style.format(label="Number of Deployed Tokens", value=f"✨{df_deploy_stats["Total Number of Deployed Tokens"][0]:,}"), unsafe_allow_html=True)
with col2:
    st.markdown(card_style.format(label="Number of Token Deployers", value=f"👨‍💻{df_deploy_stats["Total Number of Token Deployers"][0]:,}"), unsafe_allow_html=True)
with col3:
    st.markdown(card_style.format(label="Total Gas Fees", value=f"⛽${df_deploy_stats["Total Gas Fees"][0]:,}"), unsafe_allow_html=True)

# --- Row 2: Number of Deployer --------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_deployers_overtime(timeframe, start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    with table1 as (SELECT date_trunc('{timeframe}',created_at) as "Date", count(distinct data:call:transaction:from) as "Total Deployers"
FROM axelar.axelscan.fact_gmp 
WHERE status = 'executed' AND simplified_status = 'received' AND (
data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
) AND data:interchain_token_deployment_started:event='InterchainTokenDeploymentStarted'
and created_at::date>='{start_str}' and created_at::date<='{end_str}'
group by 1
order by 1),

table2 as (with tab1 as (
SELECT data:call:transaction:from as deployer, min(created_at::date) as first_deployment_date
FROM axelar.axelscan.fact_gmp 
WHERE status = 'executed' AND simplified_status = 'received' AND (
data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
) AND data:interchain_token_deployment_started:event='InterchainTokenDeploymentStarted'
group by 1)

select date_trunc('{timeframe}',first_deployment_date) as "Date", count(distinct deployer) as "New Deployers"
from tab1
where first_deployment_date>='{start_str}' and first_deployment_date<='{end_str}'
group by 1)

select table1."Date" as "Date", "Total Deployers", "New Deployers", "Total Deployers"-"New Deployers" as "Returning Deployers"
from table1 left join table2 on table1."Date"=table2."Date"
order by 1

    """

    df = pd.read_sql(query, conn)
    return df

# --- Row 2: Number of Tokens Deployed ----------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_deployed_tokens(timeframe, start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    SELECT date_trunc('{timeframe}',created_at) as "Date", count(distinct data:interchain_token_deployment_started:tokenId) as "Number of Tokens", case 
when (call:receipt:logs[0]:address ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' or 
call:receipt:logs[0]:address ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%') then 'Existing Tokens'
else 'Newly Minted Token' end as "Token Type"
FROM axelar.axelscan.fact_gmp 
WHERE status = 'executed' AND simplified_status = 'received' AND (
data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
) AND data:interchain_token_deployment_started:event='InterchainTokenDeploymentStarted'
AND created_at::date>='{start_str}' and created_at::date<='{end_str}'
group by 1, 3 
order by 1

    """

    df = pd.read_sql(query, conn)
    return df

# === Load Data: Row 2 ====================================================================
df_deployers_overtime = load_deployers_overtime(timeframe, start_date, end_date)
df_deployed_tokens = load_deployed_tokens(timeframe, start_date, end_date)
# === Chart: Row 2 ========================================================================
color_map = {
    "Existing Tokens": "#858dff",
    "Newly Minted Token": "#fc9047"
}

col1, col2 = st.columns(2)

with col1:
    fig_b1 = go.Figure()
    # Stacked Bars
    fig_b1.add_trace(go.Bar(x=df_deployers_overtime["Date"], y=df_deployers_overtime["New Deployers"], name="New Deployers", marker_color="#fc9047"))
    fig_b1.add_trace(go.Bar(x=df_deployers_overtime["Date"], y=df_deployers_overtime["Returning Deployers"], name="Returning Deployers", marker_color="#858dff"))
    fig_b1.add_trace(go.Scatter(x=df_deployers_overtime["Date"], y=df_deployers_overtime["Total Deployers"], name="Total Deployers", mode="lines", line=dict(color="black", width=2)))
    fig_b1.update_layout(barmode="stack", title="Number of Token Deployers Over Time", yaxis=dict(title="Address count"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5))
    st.plotly_chart(fig_b1, use_container_width=True)

with col2:
    fig_stacked_tokens = px.bar(df_deployed_tokens, x="Date", y="Number of Tokens", color="Token Type", title="Number of Tokens Deployed Over Time", color_discrete_map=color_map)
    fig_stacked_tokens.update_layout(barmode="stack", yaxis_title="Number of Tokens", xaxis_title="", legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5, title=""))
    st.plotly_chart(fig_stacked_tokens, use_container_width=True)

# --- Row 3,4 -------------------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_deploy_fee_stats_overtime(timeframe, start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    with table1 as (
SELECT created_at, data:interchain_token_deployment_started:tokenId as token, 
data:call:transaction:from as deployer, COALESCE(CASE 
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
      LOWER(data:call.chain::STRING) AS "Deployed Chain"
FROM axelar.axelscan.fact_gmp 
WHERE status = 'executed' AND simplified_status = 'received' AND (
data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
) AND data:interchain_token_deployment_started:event='InterchainTokenDeploymentStarted'
and created_at::date>='{start_str}' and created_at::date<='{end_str}')

select date_trunc('{timeframe}',created_at) as "Date", "Deployed Chain", round(sum(fee),2) as "Total Gas Fees",
round(avg(fee),3) as "Avg Gas Fee"
from table1
group by 1, 2
order by 1

    """

    df = pd.read_sql(query, conn)
    return df

# --- Row 4 -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_gas_fee_stats(start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    with table1 as (
SELECT created_at, data:interchain_token_deployment_started:tokenId as token, 
data:call:transaction:from as deployer, COALESCE(CASE 
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
      END) AS fee
FROM axelar.axelscan.fact_gmp 
WHERE status = 'executed' AND simplified_status = 'received' AND (
data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
) AND data:interchain_token_deployment_started:event='InterchainTokenDeploymentStarted'
and created_at::date>='{start_str}' and created_at::date<='{end_str}')

select round(avg(fee),3) as "Avg Gas Fee", round(median(fee),3) as "Median Gas Fee", round(max(fee)) as "Max Gas Fee"
from table1

    """

    df = pd.read_sql(query, conn)
    return df
    
# --- Row 5 ---------------------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_avg_median_fee_stats(timeframe, start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    with table1 as (
SELECT created_at, data:interchain_token_deployment_started:tokenId as token, 
data:call:transaction:from as deployer, COALESCE(CASE 
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
      END) AS fee
FROM axelar.axelscan.fact_gmp 
WHERE status = 'executed' AND simplified_status = 'received' AND (
data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
) AND data:interchain_token_deployment_started:event='InterchainTokenDeploymentStarted'
and created_at::date>='{start_str}' and created_at::date<='{end_str}')

select date_trunc('{timeframe}',created_at) as "Date", round(avg(fee),3) as "Avg Gas Fee",
round(median(fee),3) as "Median Gas Fee"
from table1
group by 1
order by 1

    """

    df = pd.read_sql(query, conn)
    return df

# === Load Data: Row 3,4,5 ==================================================================
df_deploy_fee_stats_overtime = load_deploy_fee_stats_overtime(timeframe, start_date, end_date)
df_avg_median_fee_stats = load_avg_median_fee_stats(timeframe, start_date, end_date)
df_gas_fee_stats = load_gas_fee_stats(start_date, end_date)
# === Charts: Row 3 =====================================================================

col1, col2 = st.columns(2)

with col1:
    fig_b1 = go.Figure()
    # Stacked Bars
    fig_stacked_fee_chain = px.bar(df_deploy_fee_stats_overtime, x="Date", y="Total Gas Fees", color="Deployed Chain", 
                                title="Amount of Fees Paid Based on the Deployed Chain Over Time")
    fig_stacked_fee_chain.update_layout(barmode="stack", yaxis_title="$USD", xaxis_title="", legend=dict(title=""))
    st.plotly_chart(fig_stacked_fee_chain, use_container_width=True)

with col2:
    df_norm = df_deploy_fee_stats_overtime.copy()
    df_norm['total_per_date'] = df_norm.groupby('Date')['Total Gas Fees'].transform('sum')
    df_norm['normalized'] = df_norm['Total Gas Fees'] / df_norm['total_per_date']
    fig_norm_stacked_fee_chain = px.bar(df_norm, x='Date', y='normalized', color='Deployed Chain', title="Share of Fees Paid Based on the Deployed Chain Over Time",
                                     text=df_norm['Total Gas Fees'].astype(str))
    fig_norm_stacked_fee_chain.update_layout(barmode='stack', xaxis_title="", yaxis_title="%", yaxis=dict(tickformat='%'), legend=dict(title=""))
    fig_norm_stacked_fee_chain.update_traces(textposition='inside')
    st.plotly_chart(fig_norm_stacked_fee_chain, use_container_width=True)

# === KPIs: Row 4 ======================================================
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
    st.markdown(card_style.format(label="Avg Gas Fee", value=f"📊${df_gas_fee_stats["Avg Gas Fee"][0]:,}"), unsafe_allow_html=True)
with col2:
    st.markdown(card_style.format(label="Median Gas Fee", value=f"📋${df_gas_fee_stats["Median Gas Fee"][0]:,}"), unsafe_allow_html=True)
with col3:
    st.markdown(card_style.format(label="Max Gas Fee", value=f"📈${df_gas_fee_stats["Max Gas Fee"][0]:,}"), unsafe_allow_html=True)


# --- Row 6 -----------------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_deploy_stats_by_chain(start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    with table1 as (
SELECT created_at, data:interchain_token_deployment_started:tokenId as token, 
data:call:transaction:from as deployer, COALESCE(CASE 
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
      LOWER(data:call.chain::STRING) AS "Deployed Chain"
FROM axelar.axelscan.fact_gmp 
WHERE status = 'executed' AND simplified_status = 'received' AND (
data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' -- Interchain Token Service
or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%' -- Axelar ITS Hub
) AND data:interchain_token_deployment_started:event='InterchainTokenDeploymentStarted'
and created_at::date>='{start_str}' and created_at::date<='{end_str}')

select "Deployed Chain", round(sum(fee),2) as "Total Gas Fees", count(distinct token) as "Number of Tokens"
from table1
group by 1
order by 2 desc 

    """

    df = pd.read_sql(query, conn)
    return df

# --- Row 7 --------------------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_list_tokens(start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    with tab3 as (with tab1 as (SELECT data:interchain_token_deployment_started:tokenName as token_name,
data:interchain_token_deployment_started:tokenSymbol as symbol,
call:chain as chain
FROM axelar.axelscan.fact_gmp
where data:interchain_token_deployment_started:event='InterchainTokenDeploymentStarted'
and (data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' 
or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%')
and status='executed'
and event='ContractCall'
and simplified_status='received'
and (call:receipt:logs[0]:address not ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' and 
call:receipt:logs[0]:address not ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%')
and created_at::date between '{start_str}' and '{end_str}'),

tab2 as (SELECT data:interchain_token_deployment_started:tokenName as token_name,
data:interchain_token_deployment_started:tokenSymbol as symbol,
data:call:returnValues:destinationChain as chain

FROM axelar.axelscan.fact_gmp

where data:interchain_token_deployment_started:event='InterchainTokenDeploymentStarted'
and (data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' 
or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%')
and status='executed'
and simplified_status='received'
and created_at::date between '{start_str}' and '{end_str}')

select * from tab1 union all 
select * from tab2)

select token_name, symbol, count(distinct lower(chain)) as chain_count 
from tab3
group by 1,2
having count(distinct lower(chain))>1
order by 3 desc 

    """

    df = pd.read_sql(query, conn)
    return df

# --- Row 8 -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_tracking_tokens(start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    SELECT created_at as "Date", data:call:transaction:from as "Deployer", data:interchain_token_deployment_started:tokenName as "Token Name",
data:interchain_token_deployment_started:tokenSymbol as "Token Symbol", case 
when (call:receipt:logs[0]:address ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' or 
call:receipt:logs[0]:address ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%') then 'Existing Tokens'
else 'Newly Minted Token' end as "Token Type", call:chain as "Deployed Chain",
data:call:returnValues:destinationChain as "Registered Chain",
data:interchain_token_deployment_started:tokenId as "Token ID", COALESCE(CASE 
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
      END) AS "Fee"
FROM axelar.axelscan.fact_gmp
where data:interchain_token_deployment_started:event='InterchainTokenDeploymentStarted'
and (data:approved:returnValues:contractAddress ilike '%0xB5FB4BE02232B1bBA4dC8f81dc24C26980dE9e3C%' 
or data:approved:returnValues:contractAddress ilike '%axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr%')
and status='executed'
and simplified_status='received'
and created_at::date between '{start_str}' and '{end_str}'
order by 1 desc 

    """

    df = pd.read_sql(query, conn)
    return df

# --- Load Data --------------------------------------------------------------------------------------------------------------------



df_deploy_stats_by_chain = load_deploy_stats_by_chain(start_date, end_date)

df_list_tokens = load_list_tokens(start_date, end_date)
df_tracking_tokens = load_tracking_tokens(start_date, end_date)
