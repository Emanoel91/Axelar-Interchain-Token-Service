import streamlit as st
import pandas as pd
import requests
import time
import plotly.express as px

# --- تبدیل تاریخ به یونیکس (ثانیه) ----------------------------------------------------------------------------------
def to_unix_timestamp(dt):
    return int(time.mktime(dt.timetuple()))

# --- دریافت داده‌ها از API ها -----------------------------------------------------------------------------------------
@st.cache_data
def load_data(start_date, end_date):
    from_time = to_unix_timestamp(pd.to_datetime(start_date))
    to_time = to_unix_timestamp(pd.to_datetime(end_date))

    # API اول: تراکنش‌ها
    url_tx = f"https://api.axelarscan.io/gmp/GMPTopITSAssets?fromTime={from_time}&toTime={to_time}"
    tx_data = requests.get(url_tx).json().get("data", [])

    # API دوم: اطلاعات توکن‌ها
    url_assets = "https://api.axelarscan.io/api/getITSAssets"
    assets_data = requests.get(url_assets).json()

    # ساخت نگاشت‌ها: address → symbol و symbol → image
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

    # ساخت DataFrame از داده‌های تراکنش
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

# --- اجرای اصلی ------------------------------------------------------------------------------------------------------
st.set_page_config(page_title="ITS Dashboard", layout="wide")
st.title("✨ Interchain Token Service (ITS) Dashboard")

# انتخاب بازه زمانی
col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("Start Date", value=pd.to_datetime("2022-09-01"))
with col2:
    end_date = st.date_input("End Date", value=pd.to_datetime("2025-09-30"))

# بارگذاری داده‌ها
df, symbol_to_image = load_data(start_date, end_date)

if df.empty:
    st.warning("⛔ No data available for the selected time range.")
else:
    # جدول (اعداد فرمت شده با ویرگول سه‌رقمی)
    df_display = df.copy()
    df_display["Number of Transfers"] = df_display["Number of Transfers"].map("{:,}".format)
    df_display["Volume of Transfers"] = df_display["Volume of Transfers"].map("{:,.0f}".format)

    # نمایش جدول با 20 ردیف و اسکرول
    st.subheader("📑 Interchain Token Transfers Table")
    st.dataframe(df_display, use_container_width=True, height=700)  # حدودا 20 ردیف

    # --- تجمیع داده‌ها برای نمودارها (بدون Unknown) ------------------------------------------------------------------
    df_grouped = (
        df[df["Symbol"] != "Unknown"]
        .groupby("Symbol", as_index=False)
        .agg({
            "Number of Transfers": "sum",
            "Volume of Transfers": "sum"
        })
    )

    # ستون جدید برای نمایش لوگو در محور X
    df_grouped["Symbol+Logo"] = df_grouped["Symbol"].apply(
        lambda s: f"<img src='{symbol_to_image.get(s, '')}' style='width:20px;height:20px;border-radius:50%;vertical-align:middle;margin-right:5px;'> {s}"
    )

    # --- نمودار ۱: Top 10 by Volume ----------------------------------------------------------------------------------
    top_volume = df_grouped.sort_values("Volume of Transfers", ascending=False).head(10)
    fig1 = px.bar(
        top_volume,
        x="Symbol+Logo",
        y="Volume of Transfers",
        text="Volume of Transfers",
        color="Symbol"
    )
    fig1.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
    fig1.update_layout(
        title="Top 10 Tokens by Interchain Transfers Volume",
        xaxis_title="Token",
        yaxis_title="Volume",
        showlegend=False
    )

    # --- نمودار ۲: Top 10 by Transfers Count ------------------------------------------------------------------------
    df_nonzero = df_grouped[df_grouped["Volume of Transfers"] > 0]
    top_transfers = df_nonzero.sort_values("Number of Transfers", ascending=False).head(10)
    fig2 = px.bar(
        top_transfers,
        x="Symbol+Logo",
        y="Number of Transfers",
        text="Number of Transfers",
        color="Symbol"
    )
    fig2.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
    fig2.update_layout(
        title="Top 10 Tokens by Interchain Transfers Count",
        xaxis_title="Token",
        yaxis_title="Transfers",
        showlegend=False
    )

    # نمایش نمودارها
    st.plotly_chart(fig1, use_container_width=True)
    st.plotly_chart(fig2, use_container_width=True)
