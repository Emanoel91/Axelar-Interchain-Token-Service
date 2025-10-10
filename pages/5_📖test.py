import streamlit as st
import pandas as pd
import requests
import plotly.graph_objects as go

# -------------------------
# 1) ویجت‌ها (باید قبل از فراخوانی load_gmp_data باشند)
# -------------------------
col1, col2, col3, col4 = st.columns(4)

with col1:
    its_token = st.selectbox("Select ITS Token", [
        "ALVA","ACP","AETX","AI","AIPO","AITECH","$WAI","ATH","BAVA","BCT","Boop","BSW","BTCB","BUNNI","CAI","CATE","CATBOY",
        "CDFI","CFG","CHRP","CONK","DACKIE","DAI","DCB","DEAL","DOGE","EARTH","ELDA","END","ENSURE","ESE","FDUSD","FIG","FLOKI",
        "FS","FUN","FX","G3","GQ","GRAIN","GRAM","GROK","HOKK","HYB","IMP","IMR","IMT","ITHACA","KATA","KIP","KLIMA","KLAUS",
        "M-BTC","MEGALAND","MIRAI","MOLLARS","MOON","MSS","MSTR","MUNITY","MVX","NEIRO","NFTL","NUT","NUTS","OFF","OIK","OMKG",
        "OXYZ","PELL","PSTAKE","PUNDIAI","QUACK","RBX","RDX","RECALL","RMRK","RSTK","SATOSHI","SOULS","SPECU","Speed","STABLE",
        "TADA","TBH","TRIUMPH","TURBO","UNV","USDC.axl","USDFI","USDT","USDf","USDX","VOLS","WBTC","WETH","XAV","XDEFI","XRP",
        "YBR","YUP","agETH.axl","axlUSDM","mBASIS","mBTC","mEDGE","mF-ONE","mMEV","mRe7YIELD","mTBILL","oBUNNI","oooOOO",
        "sUSDX","sUSDf","sUSDz","wpFIL"
    ])

with col2:
    timeframe = st.selectbox("Select Time Frame", ["month", "week", "day"])

with col3:
    start_date = st.date_input("Start Date", value=pd.to_datetime("2025-01-01").date())

with col4:
    end_date = st.date_input("End Date", value=pd.to_datetime("2026-01-01").date())

# -------------------------
# 2) توابع کمکی: تبدیل تاریخ به unixtime (UTC)، بارگذاری از API و تبدیل timestamp
# -------------------------
def to_unix_seconds_from_date(d, end_of_day=False):
    """d می‌تواند date یا datetime باشد. اگر end_of_day True باشد، تا 23:59:59 آن روز را می‌گیرد."""
    ts = pd.to_datetime(d)
    if end_of_day:
        ts = ts.normalize() + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    else:
        ts = ts.normalize()
    # نالایز به UTC (اگر timezone نداشته باشد)
    if ts.tzinfo is None:
        ts = ts.tz_localize('UTC')
    else:
        ts = ts.tz_convert('UTC')
    return int(ts.timestamp())

@st.cache_data(ttl=300)
def load_gmp_data(symbol: str, start_date, end_date):
    """خواندن داده از API با پارامترهای یونیکس ثانیه؛ برمی‌گرداند DataFrame با ستون‌های timestamp (datetime UTC)، num_txs، volume"""
    from_unix = to_unix_seconds_from_date(start_date, end_of_day=False)
    to_unix = to_unix_seconds_from_date(end_date, end_of_day=True)

    url = "https://api.axelarscan.io/gmp/GMPChart"
    params = {"symbol": symbol, "fromTime": from_unix, "toTime": to_unix}

    resp = requests.get(url, params=params, timeout=20)
    resp.raise_for_status()
    payload = resp.json()
    data = payload.get("data", []) if isinstance(payload, dict) else []

    df = pd.DataFrame(data)
    if df.empty:
        return df

    # اطمینان از انواع عددی
    df['num_txs'] = pd.to_numeric(df.get('num_txs', 0), errors='coerce').fillna(0).astype(int)
    df['volume'] = pd.to_numeric(df.get('volume', 0.0), errors='coerce').fillna(0.0).astype(float)

    # تشخیص واحد timestamp و تبدیل امن به datetime (utc)
    def parse_timestamp_col(col):
        nums = pd.to_numeric(col, errors='coerce')
        if nums.notna().any():
            maxv = nums.max()
            # heuristics برای انتخاب unit
            if maxv > 1e18:
                unit = 'ns'
            elif maxv > 1e15:
                unit = 'us'
            elif maxv > 1e12:
                unit = 'ms'
            elif maxv > 1e9:
                unit = 's'
            else:
                unit = 's'
            # تبدیل اعدادی که non-null هستند
            dt_series = pd.to_datetime(nums.dropna().astype('int64'), unit=unit, utc=True, errors='coerce')
            # بازگرداندن به ایندکس اصلی (پر کردن NaT برای مقادیر Null)
            full = pd.Series(pd.NaT, index=nums.index)
            full.loc[nums.notna()] = dt_series.values
            # اگر هنوز تعداد زیادی NaT وجود داشت، تلاش برای پارس رشته‌ها (ISO)
            if full.isna().sum() > 0:
                parsed_strings = pd.to_datetime(col, utc=True, errors='coerce')
                full[full.isna()] = parsed_strings[full.isna()]
            return full
        else:
            # همه رشته‌اند یا همه نامعتبر -> مستقیم parse
            return pd.to_datetime(col, utc=True, errors='coerce')

    df['timestamp'] = parse_timestamp_col(df.get('timestamp'))
    # حذف ردیف‌هایی که timestamp معتبر ندارند
    df = df.dropna(subset=['timestamp']).sort_values('timestamp').reset_index(drop=True)
    return df

def aggregate_by_timeframe(df, timeframe):
    if df.empty:
        return df
    d = df.set_index('timestamp')
    if timeframe == "day":
        res = d.resample('D').agg({'num_txs': 'sum', 'volume': 'sum'})
    elif timeframe == "week":
        # هفته از Monday شروع می‌شود؛ در صورت نیاز تغییر دهید
        res = d.resample('W-MON').agg({'num_txs': 'sum', 'volume': 'sum'})
    elif timeframe == "month":
        res = d.resample('M').agg({'num_txs': 'sum', 'volume': 'sum'})
    else:
        res = d.resample('D').agg({'num_txs': 'sum', 'volume': 'sum'})
    res = res.reset_index()
    res['num_txs'] = res['num_txs'].fillna(0).astype(int)
    res['volume'] = res['volume'].fillna(0.0).astype(float)
    return res

# -------------------------
# 3) فراخوانی با هندل خطا و نمایش داده / KPI و نمودار
# -------------------------
try:
    df = load_gmp_data(its_token, start_date, end_date)
except Exception as e:
    st.error(f"Failed to load API data: {e}")
    df = pd.DataFrame(columns=["timestamp", "num_txs", "volume"])

# برای دیباگ: داده خام را در یک expander نمایش بده
with st.expander("Show raw API sample (debug)"):
    st.write(df.head(10))

df_agg = aggregate_by_timeframe(df, timeframe)

# محاسبه KPI ها از دادهٔ API
total_num_txs = int(df_agg['num_txs'].sum()) if not df_agg.empty else 0
total_volume = float(df_agg['volume'].sum()) if not df_agg.empty else 0.0

k1, k2, k3, k4 = st.columns(4)
k1.metric("Volume of Transfers (Native Token)", "—")
k2.metric("Volume of Transfers ($USD)", f"${total_volume:,.0f}")
k3.metric("Number of Transfers", f"{total_num_txs:,}")
k4.metric("Number of Senders", "—")

# رسم نمودار ترکیبی (bar = num_txs, line = volume)
if not df_agg.empty:
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df_agg['timestamp'],
        y=df_agg['num_txs'],
        name="Number of Transfers",
        yaxis="y1",
        opacity=0.8
    ))
    fig.add_trace(go.Scatter(
        x=df_agg['timestamp'],
        y=df_agg['volume'],
        name="Volume of Transfers ($USD)",
        yaxis="y2",
        mode="lines+markers",
        line=dict(width=2)
    ))

    fig.update_layout(
        title="Interchain Transfers Over Time",
        xaxis=dict(title="Date"),
        yaxis=dict(title="Number of Transfers", side="left"),
        yaxis2=dict(title="Volume of Transfers ($USD)", overlaying="y", side="right"),
        legend=dict(x=0.01, y=0.99),
        hovermode="x unified",
        template="plotly_white",
        height=520
    )

    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("No data found for the selected filters.")
