import requests
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# --- تبدیل تاریخ انتخابی کاربر به یونیکس ثانیه (UTC) ---
def to_unix_seconds(dt):
    ts = pd.to_datetime(dt)
    if ts.tzinfo is None:
        ts = ts.tz_localize('UTC')
    else:
        ts = ts.tz_convert('UTC')
    return int(ts.timestamp())

# --- بارگذاری مقاوم از API و تبدیل timestamp ---
@st.cache_data(ttl=300)
def load_gmp_data(its_token, start_date, end_date):
    from_unix = to_unix_seconds(start_date)
    to_unix = to_unix_seconds(end_date)

    url = f"https://api.axelarscan.io/gmp/GMPChart?symbol={its_token}&fromTime={from_unix}&toTime={to_unix}"
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    data = resp.json().get("data", [])
    df = pd.DataFrame(data)

    if df.empty:
        return df

    # اطمینان از نوع صحیح ستون‌های عددی
    df['num_txs'] = pd.to_numeric(df.get('num_txs', 0), errors='coerce').fillna(0).astype(int)
    df['volume'] = pd.to_numeric(df.get('volume', 0.0), errors='coerce').fillna(0.0)

    # تابع کمکی برای تبدیل timestamp (عدد یا رشته) با تشخیص واحد
    def parse_timestamp_col(col):
        # تلاش اول: تبدیل به عدد (در صورت امکان)
        nums = pd.to_numeric(col, errors='coerce')
        if nums.notna().any():
            maxv = nums.max()
            # heuristics برای تشخیص واحد
            if maxv > 10**17:
                unit = 'ns'
            elif maxv > 10**14:
                unit = 'us'
            elif maxv > 10**11:
                unit = 'ms'
            elif maxv > 10**9:
                unit = 's'
            else:
                # کوچک‌تر از ~1e9 را هم به عنوان ثانیه در نظر می‌گیریم (fallback)
                unit = 's'
            dt = pd.to_datetime(nums.astype('Int64'), unit=unit, utc=True, errors='coerce')
            # اگر تعداد زیادی NaT داشت، تلاش کنیم رشته‌ها رو مستقیماً پارس کنیم (ISO)
            if dt.isna().sum() > len(dt) * 0.5:
                dt2 = pd.to_datetime(col, utc=True, errors='coerce')
                # جایگزینی مقادیر NaT
                dt[dt.isna()] = dt2[dt.isna()]
            return dt
        else:
            # اگر اصلاً عددی نبود (مثلاً ISO strings)، مستقیم پارس می‌کنیم
            return pd.to_datetime(col, utc=True, errors='coerce')

    df['timestamp'] = parse_timestamp_col(df['timestamp'])
    # حذف ردیف‌هایی که تاریخ معتبر ندارند (تا از خطا جلوگیری شود)
    df = df.dropna(subset=['timestamp'])
    df = df.sort_values('timestamp').reset_index(drop=True)

    return df

# --- تجمیع بر اساس timeframe (روز/هفته/ماه) با resample (UTC-indexed) ---
def aggregate_by_timeframe(df, timeframe):
    if df.empty:
        return df
    df = df.copy()
    df.set_index('timestamp', inplace=True)

    if timeframe == "day":
        res = df.resample('D').sum()
    elif timeframe == "week":
        # هفته از Monday شروع می‌شود؛ می‌توانید 'W-SUN' را انتخاب کنید اگر می‌خواهید یکشنبه
        res = df.resample('W-MON').sum()
    elif timeframe == "month":
        res = df.resample('M').sum()
    else:
        res = df.resample('D').sum()

    res = res.reset_index()
    # مطمئن شویم انواع درست هستند
    res['num_txs'] = res['num_txs'].fillna(0).astype(int)
    res['volume'] = res['volume'].fillna(0.0)
    return res

# --- مثال استفاده و رسم (قرار دهید در همان جای قبلی شما) ---
df = load_gmp_data(its_token, start_date, end_date)
df_agg = aggregate_by_timeframe(df, timeframe)

# KPIها
total_num_txs = int(df_agg['num_txs'].sum()) if not df_agg.empty else 0
total_volume = float(df_agg['volume'].sum()) if not df_agg.empty else 0.0

k1, k2, k3, k4 = st.columns(4)
k1.metric("Volume of Transfers (Native Token)", "—")
k2.metric("Volume of Transfers ($USD)", f"${int(total_volume):,}")
k3.metric("Number of Transfers", f"{total_num_txs:,}")
k4.metric("Number of Senders", "—")

# رسم نمودار ترکیبی bar-line
if not df_agg.empty:
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df_agg['timestamp'],
        y=df_agg['num_txs'],
        name="Number of Transfers",
        yaxis="y1"
    ))
    fig.add_trace(go.Scatter(
        x=df_agg['timestamp'],
        y=df_agg['volume'],
        name="Volume of Transfers ($USD)",
        yaxis="y2",
        mode="lines+markers"
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
