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
    # جدول HTML با لوگو
    df_display = df.copy()
    df_display["Number of Transfers"] = df_display["Number of Transfers"].map("{:,}".format)
    df_display["Volume of Transfers"] = df_display["Volume of Transfers"].map("{:,.0f}".format)

    # ساخت HTML جدول
    table_html = """
    <style>
    .scroll-table {
        display: block;
        max-height: 700px; /* حدود 20 ردیف */
        overflow-y: scroll;
        border: 1px solid #ddd;
    }
    table {
        border-collapse: collapse;
        width: 100%;
    }
    th, td {
        text-align: left;
        padding: 8px;
        border-bottom: 1px solid #ddd;
        font-size: 14px;
    }
    tr:hover {background-color: #f5f5f5;}
    img {
        width: 20px;
        height: 20px;
        border-radius: 50%;
        vertical-align: middle;
        margin-right: 5px;
    }
    </style>
    <div class="scroll-table">
    <table>
        <thead>
            <tr>
                <th>Token Address</th>
                <th>Symbol</th>
                <th>Logo</th>
                <th>Number of Transfers</th>
                <th>Volume of Transfers</th>
            </tr>
        </thead>
        <tbody>
    """
    for _, row in df_display.iterrows():
        logo_html = f"<img src='{row['Logo']}'>" if row['Logo'] else ""
        table_html += f"""
        <tr>
            <td>{row['Token Address']}</td>
            <td>{row['Symbol']}</td>
            <td>{logo_html}</td>
            <td>{row['Number of Transfers']}</td>
            <td>{row['Volume of Transfers']}</td>
        </tr>
        """
    table_html += "</tbody></table></div>"

    st.subheader("📑 Interchain Token Transfers Table")
    st.markdown(table_html, unsafe_allow_html=True)

    # --- تجمیع داده‌ها برای نمودارها (بدون Unknown) ------------------------------------------------------------------
    df_grouped = (
        df[df["Symbol"] != "Unknown"]
        .groupby("Symbol", as_index=False)
        .agg({
            "Number of Transfers": "sum",
            "Volume of Transfers": "sum"
        })
    )

    # --- نمودار ۱: Top 10 by Volume ----------------------------------------------------------------------------------
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
        xaxis_title="Token",
        yaxis_title="Volume",
        showlegend=False
    )

    # اضافه کردن لوگوها روی محور X
    for _, row in top_volume.iterrows():
        logo_url = symbol_to_image.get(row["Symbol"], "")
        if logo_url:
            fig1.add_layout_image(
                dict(
                    source=logo_url,
                    x=row["Symbol"],
                    y=-0.05 * top_volume["Volume of Transfers"].max(),
                    xref="x",
                    yref="y",
                    sizex=0.8,
                    sizey=0.8 * top_volume["Volume of Transfers"].max() / 10,
                    xanchor="center",
                    yanchor="top",
                    layer="above"
                )
            )

    # --- نمودار ۲: Top 10 by Transfers Count ------------------------------------------------------------------------
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
        xaxis_title="Token",
        yaxis_title="Transfers",
        showlegend=False
    )

    # اضافه کردن لوگوها روی محور X
    for _, row in top_transfers.iterrows():
        logo_url = symbol_to_image.get(row["Symbol"], "")
        if logo_url:
            fig2.add_layout_image(
                dict(
                    source=logo_url,
                    x=row["Symbol"],
                    y=-0.05 * top_transfers["Number of Transfers"].max(),
                    xref="x",
                    yref="y",
                    sizex=0.8,
                    sizey=0.8 * top_transfers["Number of Transfers"].max() / 10,
                    xanchor="center",
                    yanchor="top",
                    layer="above"
                )
            )

    # نمایش نمودارها
    st.plotly_chart(fig1, use_container_width=True)
    st.plotly_chart(fig2, use_container_width=True)
