import streamlit as st
import pandas as pd
import boto3
from boto3.dynamodb.conditions import Attr
import os
from decimal import Decimal
from datetime import datetime, timedelta
import pytz
import yfinance as yf
from streamlit_autorefresh import st_autorefresh
import gc 

# --- 0. MEMORY CLEANUP ---
gc.collect()

# --- 1. PAGE CONFIG ---
st.set_page_config(page_title="SignalX Dashboard", layout="wide", page_icon="⚡")

# --- CONFIG ---
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1") 
DYNAMODB_TABLE = os.getenv("DYNAMODB_TABLE", "SentAlerts")
DEFAULT_EXCHANGE = "NSE"

# === TRADINGVIEW MAPPING ===
TICKER_CORRECTIONS = {
    "LIC HOUSING FINANCE LTD": "LICHSGFIN", "INOX WIND LIMITED": "INOXWIND",
    "HINDUSTAN ZINC LIMITED": "HINDZINC", "HINDUSTAN UNILEVER LTD.": "HINDUNILVR",
    "TATA TECHNOLOGIES LIMITED": "TATATECH", "SYNGENE INTERNATIONAL LTD": "SYNGENE",
    "MARUTI SUZUKI INDIA LTD.": "MARUTI", "KEI INDUSTRIES LTD.": "KEI",
    "JINDAL STEEL LIMITED": "JINDALSTEL", "CENTRAL DEPO SER (I) LTD": "CDSL",
    "BAJAJ FINANCE LIMITED": "BAJFINANCE", "MAHINDRA & MAHINDRA LTD": "M&M", 
    "DABUR INDIA LTD": "DABUR", "TRENT LTD": "TRENT", "JIO FIN SERVICES LTD": "JIOFIN",
    "IIFL FINANCE LIMITED": "IIFL", "MUTHOOT FINANCE LIMITED": "MUTHOOTFIN",
    "BOSCH LIMITED": "BOSCHLTD", "HDFC LIFE INS CO LTD": "HDFCLIFE",
    "ASIAN PAINTS LIMITED": "ASIANPAINT", "DALMIA BHARAT LIMITED": "DALBHARAT",
    "BLUE STAR LIMITED": "BLUESTARCO", "HINDALCO INDUSTRIES LTD": "HINDALCO",
    "360 ONE WAM LIMITED": "360ONE", "HINDALCO INDUSTRIES LIMITED": "HINDALCO",
    "HINDALCO": "HINDALCO", "HINDALCO  INDUSTRIES  LTD": "HINDALCO",
    "PATANJALI FOODS LIMITED": "PATANJALI", "INDUSIND BANK LIMITED": "INDUSINDBK",
    "COAL INDIA LTD": "COALINDIA", "TATA MOTORS LIMITED": "TATAMOTORS",
    "INDIAN ENERGY EXC LTD": "IEX", "RELIANCE INDUSTRIES LTD": "RELIANCE",
    "GRASIM INDUSTRIES LTD": "GRASIM", "INDIAN RENEWABLE ENERGY": "IREDA",
    "LIFE INSURA CORP OF INDIA": "LICI", "FEDERAL BANK LTD": "FEDERALBNK",
    "JSW STEEL LIMITED": "JSWSTEEL", "RAIL VIKAS NIGAM LIMITED": "RVNL",
    "PIDILITE INDUSTRIES LTD": "PIDILITIND", "MANAPPURAM FINANCE LTD": "MANAPPURAM",
    "TORRENT PHARMACEUTICALS L": "TORNTPHARM", "BAJAJ AUTO LIMITED": "BAJAJ-AUTO",
    "DR. REDDY S LABORATORIES": "DRREDDY", "CIPLA LTD": "CIPLA",
    "MANKIND PHARMA LIMITED": "MANKIND", "NATIONAL ALUMINIUM CO LTD": "NATIONALUM",
    "CANARA BANK": "CANBK", "AVENUE SUPERMARTS LIMITED": "DMART",
    "STEEL AUTHORITY OF INDIA": "SAIL", "INDIAN RAIL TOUR CORP LTD": "IRCTC",
    "TORRENT POWER LTD": "TORNTPOWER", "JUBILANT FOODWORKS LTD": "JUBLFOOD",
    "INFO EDGE (I) LTD": "NAUKRI", "INTERGLOBE AVIATION LTD": "INDIGO",
    "UNION BANK OF INDIA": "UNIONBANK", "BAJAJ FINSERV LTD.": "BAJAJFINSV",
    "SUN PHARMACEUTICAL IND L": "SUNPHARMA", "EXIDE INDUSTRIES LTD": "EXIDEIND",
    "INDUS TOWERS LIMITED": "INDUSTOWER", "BHARAT PETROLEUM CORP  LT": "BPCL",
    "SOLAR INDUSTRIES (I) LTD": "SOLARINDS", "TATA POWER CO LTD": "TATAPOWER",
    "SUPREME INDUSTRIES LTD": "SUPREMEIND", "ONE 97 COMMUNICATIONS LTD": "PAYTM",
    "HERO MOTOCORP LIMITED": "HEROMOTOCO", "MARICO LIMITED": "MARICO",
    "VODAFONE IDEA LIMITED": "IDEA", "BRITANNIA INDUSTRIES LTD": "BRITANNIA",
    "ICICI PRU LIFE INS CO LTD": "ICICIPRULI", "AU SMALL FINANCE BANK LTD": "AUBANK",
    "INDIAN RAILWAY FIN CORP L": "IRFC", "FORTIS HEALTHCARE LTD": "FORTIS",
    "HINDUSTAN AERONAUTICS LTD": "HAL", "BHARAT FORGE LTD": "BHARATFORG",
    "BHARTI AIRTEL LIMITED": "BHARTIARTL", "KALYAN JEWELLERS IND LTD": "KALYANKJIL",
    "AXIS BANK LIMITED": "AXISBANK", "ZYDUS LIFESCIENCES LTD": "ZYDUSLIFE",
    "PI INDUSTRIES LTD": "PIIND", "HINDUSTAN PETROLEUM CORP": "HINDPETRO",
    "THE PHOENIX MILLS LTD": "PHOENIXLTD", "NBCC (INDIA) LIMITED": "NBCC",
    "BSE LIMITED": "BSE", "IDFC FIRST BANK LIMITED": "IDFCFIRSTB",
    "HSG & URBAN DEV CORPN LTD": "HUDCO", "ITC LTD": "ITC",
    "AUROBINDO PHARMA LTD": "AUROPHARMA", "KAYNES TECHNOLOGY IND LTD": "KAYNES",
    "TVS MOTOR COMPANY  LTD": "TVSMOTOR", "BHEL": "BHEL",
    "EICHER MOTORS LTD": "EICHERMOT", "TATA ELXSI LIMITED": "TATAELXSI",
    "NCC LIMITED": "NCC", "OBEROI REALTY LIMITED": "OBEROIRLTY",
    "HAVELLS INDIA LIMITED": "HAVELLS", "TATA CONSULTANCY SERV LT": "TCS",
    "CROMPT GREA CON ELEC LTD": "CROMPTON", "ALKEM LABORATORIES LTD.": "ALKEM",
    "ICICI BANK LTD.": "ICICIBANK", "DLF LIMITED": "DLF",
    "NESTLE INDIA LIMITED": "NESTLEIND", "AMBER ENTERPRISES (I) LTD": "AMBER",
    "DIXON TECHNO (INDIA) LTD": "DIXON", "COMPUTER AGE MNGT SER LTD": "CAMS",
    "BIOCON LIMITED.": "BIOCON", "PAGE INDUSTRIES LTD": "PAGEIND",
    "ADANI PORT & SEZ LTD": "ADANIPORTS", "SONA BLW PRECISION FRGS L": "SONACOMS",
    "CYIENT LIMITED": "CYIENT", "ADANI GREEN ENERGY LTD": "ADANIGREEN",
    "CONTAINER CORP OF IND LTD": "CONCOR", "COFORGE LIMITED": "COFORGE",
    "HDFC AMC LIMITED": "HDFCAMC", "ASTRAL LIMITED": "ASTRAL",
    "CUMMINS INDIA LTD": "CUMMINSIND", "ANGEL ONE LIMITED": "ANGELONE",
    "SUZLON ENERGY LIMITED": "SUZLON", "ETERNAL LIMITED": "ETERNAL",
    "APL APOLLO TUBES LTD": "APLAPOLLO", "HCL TECHNOLOGIES LTD": "HCLTECH",
    "AMBUJA CEMENTS LTD": "AMBUJACEM", "PG ELECTROPLAST LTD": "PGEL",
    "COLGATE PALMOLIVE LTD.": "COLPAL", "TATA CONSUMER PRODUCT LTD": "TATACONSUM",
    "KOTAK MAHINDRA BANK LTD": "KOTAKBANK", "NHPC LTD": "NHPC",
    "SBI CARDS & PAY SER LTD": "SBICARD", "ICICI LOMBARD GIC LIMITED": "ICICIGI",
    "HDFC BANK LTD": "HDFCBANK", "LUPIN LIMITED": "LUPIN",
    "SBI LIFE INSURANCE CO LTD": "SBILIFE", "NMDC LTD.": "NMDC",
    "APOLLO HOSPITALS ENTER. L": "APOLLOHOSP", "LODHA DEVELOPERS LIMITED": "LODHA",
    "NTPC LTD": "NTPC", "CG POWER AND IND SOL LTD": "CGPOWER",
    "INDIAN OIL CORP LTD": "IOC", "HFCL LIMITED": "HFCL",
    "KPIT TECHNOLOGIES LIMITED": "KPITTECH", "SAMMAAN CAPITAL LIMITED": "SAMMAANCAP",
    "PERSISTENT SYSTEMS LTD": "PERSISTENT", "CHOLAMANDALAM IN & FIN CO": "CHOLAFIN",
    "ABB INDIA LIMITED": "ABB", "LARSEN & TOUBRO LTD.": "LT",
    "ULTRATECH CEMENT LIMITED": "ULTRACEMCO", "INFOSYS LIMITED": "INFY",
    "GAIL (INDIA) LTD": "GAIL", "PETRONET LNG LIMITED": "PETRONET",
    "POWER FIN CORP LTD.": "PFC", "PNB HOUSING FIN LTD.": "PNBHOUSING",
    "WIPRO LTD": "WIPRO", "REC LIMITED": "RECLTD",
    "PRESTIGE ESTATE LTD": "PRESTIGE", "FSN E COMMERCE VENTURES": "NYKAA",
    "ADANI ENERGY SOLUTION LTD": "ADANIENSOL", "ADANI ENTERPRISES LIMITED": "ADANIENT",
    "TATA STEEL LIMITED": "TATASTEEL", "RBL BANK LIMITED": "RBLBANK",
    "INDRAPRASTHA GAS LTD": "IGL", "YES BANK LIMITED": "YESBANK",
    "SAMVRDHNA MTHRSN INTL LTD": "MOTHERSON", "GLENMARK PHARMACEUTICALS": "GLENMARK",
    "ADITYA BIRLA CAPITAL LTD.": "ABCAPITAL", "L&T FINANCE LIMITED": "LTF",
    "UNO MINDA LIMITED": "UNOMINDA", "ORACLE FIN SERV SOFT LTD.": "OFSS",
    "BANDHAN BANK LIMITED": "BANDHANBNK", "TECH MAHINDRA LIMITED": "TECHM",
    "MPHASIS LIMITED": "MPHASIS", "SIEMENS LTD": "SIEMENS",
    "OIL INDIA LTD": "OIL", "JSW ENERGY LIMITED": "JSWENERGY",
    "TUBE INVEST OF INDIA LTD": "TIINDIA", "LTIMINDTREE LIMITED": "LTIM",
    "ASHOK LEYLAND LTD": "ASHOKLEY", "THE INDIAN HOTELS CO. LTD": "INDHOTEL",
    "DELHIVERY LIMITED": "DELHIVERY", "DIVI S LABORATORIES LTD": "DIVISLAB",
    "TITAGARH RAIL SYSTEMS LTD": "TITAGARH", "BANK OF BARODA": "BANKBARODA",
    "MAX FINANCIAL SERV LTD": "MFSL", "MULTI COMMODITY EXCHANGE": "MCX",
    "TITAN COMPANY LIMITED": "TITAN", "VOLTAS LTD": "VOLTAS",
    "SRF LTD": "SRF", "PB FINTECH LIMITED": "POLICYBZR",
    "POLYCAB INDIA LIMITED": "POLYCAB", "BHARAT ELECTRONICS LTD": "BEL",
    "BANK OF INDIA": "BANKINDIA", "BHARAT DYNAMICS LIMITED": "BDL",
    "POWER GRID CORP. LTD.": "POWERGRID", "GODREJ PROPERTIES LTD": "GODREJPROP",
    "GMR AIRPORTS LIMITED": "GMRAIRPORT", "GODREJ CONSUMER PRODUCTS": "GODREJCP",
    "INDIAN BANK": "INDIANB", "PIRAMAL PHARMA LIMITED": "PPLPHARMA",
    "OIL AND NATURAL GAS CORP.": "ONGC", "LAURUS LABS LIMITED": "LAURUSLABS",
    "STATE BANK OF INDIA": "SBIN", "SHREE CEMENT LIMITED": "SHREECEM",
    "SHRIRAM FINANCE LIMITED": "SHRIRAMFIN", "PUNJAB NATIONAL BANK": "PNB",
    "UNITED SPIRITS LIMITED": "UNITDSPR", "UPL LIMITED": "UPL",
    "VARUN BEVERAGES LIMITED": "VBL", "VEDANTA LIMITED": "VEDL",
    "KFIN TECHNOLOGIES LIMITED": "KFINTECH", "NUVAMA WEALTH MANAGE LTD": "NUVAMA",
    "MAX HEALTHCARE INS LTD": "MAXHEALTH", "MAZAGON DOCK SHIPBUIL LTD": "MAZDOCK",
    "TATA CHEMICALS LTD": "TATACHEM", "PG ELECTROPLAST LIMITED": "PGEL",
    "BAJAJ HOLDINGS & INVS LTD": "BAJAJHLDNG", "WAAREE ENERGIES LIMITED": "WAAREEENER",
    "SWIGGY LIMITED": "SWIGGY", "FSN E COMMERCE VENTURES LTD": "NYKAA",
    "FSN E COMMERCE VENTURES LIMITED": "NYKAA", "FSN E-COMMERCE VENTURES LTD": "NYKAA"
}

# --- HELPER FUNCTIONS ---
def convert_decimal(obj):
    if isinstance(obj, list): return [convert_decimal(i) for i in obj]
    elif isinstance(obj, dict): return {k: convert_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal): return float(obj)
    return obj

@st.cache_data(ttl=60)
def load_data_from_dynamodb(target_date, signal_type=None):
    try:
        dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
        table = dynamodb.Table(DYNAMODB_TABLE)
        target_date_str = target_date.isoformat()
        response = table.scan(FilterExpression=Attr("Date").eq(target_date_str))
        items = response.get('Items', [])
        while 'LastEvaluatedKey' in response:
            response = table.scan(
                FilterExpression=Attr("Date").eq(target_date_str),
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response.get('Items', []))
    except Exception as e:
        st.error(f"Error querying DynamoDB: {e}")
        return pd.DataFrame()

    if not items: return pd.DataFrame()

    df = pd.DataFrame([convert_decimal(item) for item in items])

    # Filter by Signal Type if provided
    if signal_type:
        if 'Signal' in df.columns:
            df = df[df['Signal'] == signal_type].copy()
        elif signal_type == "ALPHA_HUNTER": # Fallback for old alerts
             df = df[~df['Signal'].isin(["INTRADAY_BOOST"])].copy()

    # Standardize
    if 'Price' in df.columns and 'SignalPrice' not in df.columns:
        df.rename(columns={'Price': 'SignalPrice'}, inplace=True)
    if 'Side' in df.columns:
        df['Direction'] = df['Side'].map({'Bullish': 'LONG', 'Bearish': 'SHORT'})
    elif 'Signal' in df.columns and 'Side' not in df.columns:
        df['Direction'] = df['Signal'].map({'LONG': 'LONG', 'SHORT': 'SHORT'})

    # Numeric Conversion
    numeric_cols = ['SignalPrice', 'TargetPrice', 'TargetPct', 'RVOL', 'NetMovePct', 'SuperScore', 'RS_Score', 'OI_Change']
    for col in numeric_cols:
        if col not in df.columns: df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
    
    if 'Rating' not in df.columns: df['Rating'] = "N/A"
    if 'Alignment' not in df.columns: df['Alignment'] = "N/A"
    if 'Time' not in df.columns: df['Time'] = ""
    
    return df

def fetch_live_updates(df, target_date):
    if df.empty or 'Name' not in df.columns: return df

    df['Cleaned_Name'] = df['Name'].replace(TICKER_CORRECTIONS)
    unique_tickers = df['Cleaned_Name'].unique().tolist()
    yahoo_tickers = [f"{t}.NS" for t in unique_tickers]
     
    india_tz = pytz.timezone('Asia/Kolkata')
    is_today = target_date == datetime.now(india_tz).date()
     
    try:
        if is_today:
            data = yf.download(tickers=yahoo_tickers, period="1d", interval="1m", progress=False, threads=False)
            if not data.empty and 'Close' in data:
                 final_prices = data['Close'].iloc[-1]
            else: return df
        else:
            start_dt = target_date
            end_dt = target_date + timedelta(days=1)
            data = yf.download(tickers=yahoo_tickers, start=start_dt, end=end_dt, interval="1d", progress=False, threads=False)
            if data.empty or 'Close' not in data: return df
            final_prices = data['Close'].iloc[-1]

        def get_price(ticker, price_series):
            yf_ticker = f"{ticker}.NS"
            try:
                if isinstance(price_series, pd.Series):
                    if yf_ticker in price_series.index: return float(price_series[yf_ticker])
                if len(unique_tickers) == 1:
                    if isinstance(price_series, pd.Series): return float(price_series.iloc[0])
                    return float(price_series)
                return 0.0
            except: return 0.0

        df['Live_Price'] = df['Cleaned_Name'].apply(lambda x: get_price(x, final_prices))

    except Exception as e:
        print(f"Error fetching data: {e}")
     
    df['Live_Price'] = df['Live_Price'] if 'Live_Price' in df.columns else 0.0
    return df

def metric_card(title, value, subtitle=None, color="#e5e7eb", glow=False):
     return f"""
    <div style="padding:18px;border-radius:14px;background:linear-gradient(145deg,#0f1320,#0c101a);
    border:1px solid #222634;box-shadow:{'0 0 12px rgba(34,197,94,.35)' if glow else 'none'};">
        <div style="color:#9ca3af;font-size:14px;margin-bottom:6px;">{title}</div>
        <div style="font-size:32px;font-weight:800;color:white;">{value}</div>
        <div style="color:{color};font-size:14px;">{subtitle or ""}</div>
    </div>
    """

# =========================================================
# PAGE 1: SIGNAL X (ORIGINAL)
# =========================================================
def render_signalx(selected_date):
    # CSS & AUTO REFRESH
    count = st_autorefresh(interval=300 * 1000, key="datarefresh")
    st.markdown("""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap');
        .stApp { background-color: #080a0e; font-family: 'Inter', sans-serif; }
        [data-testid="stVerticalBlockBorderWrapper"] > div { background-color: #131722; border-radius: 12px; border: 1px solid #2a2e3a; box-shadow: 0 0 18px rgba(0,0,0,.35); }
        tbody tr:hover { background-color: rgba(255,255,255,0.04) !important; }
        .table-header { color: #FFFFFF; background: linear-gradient(90deg, rgba(255,255,255,0.05) 0%, rgba(0,0,0,0) 100%); border-left: 4px solid #00FF7F; padding: 10px 15px; margin-bottom: 15px; font-size: 1.2rem; font-weight: 800; letter-spacing: 1px; text-transform: uppercase; border-radius: 4px; }
        .block-container { padding-top: 2rem; padding-bottom: 3rem; }
        [data-testid="stSidebar"] { background-color: #0b0e14; border-right: 1px solid #2a2e3a; }
    </style>
    """, unsafe_allow_html=True)

    # --- FILTERS ---
    st.sidebar.subheader("🔍 Filters")
    rating_filter = st.sidebar.multiselect("Quality", ["⭐⭐⭐⭐⭐ (ELITE)", "⭐⭐⭐⭐ (HIGH)", "⭐⭐⭐ (MED)", "⭐ (LOW)"], default=["⭐⭐⭐⭐⭐ (ELITE)", "⭐⭐⭐⭐ (HIGH)"])
    align_filter = st.sidebar.multiselect("Market Alignment", ["✅ WITH TREND", "⚠️ CONTRA", "NEUTRAL"], default=["✅ WITH TREND", "⚠️ CONTRA"])

    # --- DATA ---
    df = load_data_from_dynamodb(selected_date, "ALPHA_HUNTER")
    if df.empty:
        st.info(f"No Alpha Hunter alerts found for {selected_date}")
        return

    # Apply Filters
    if rating_filter: df = df[df['Rating'].isin(rating_filter)]
    if align_filter: df = df[df['Alignment'].isin(align_filter)]

    # Fetch Live
    loading_placeholder = st.empty()
    loading_placeholder.text(f'⚡ Fetching Prices for {selected_date}...')
    df = fetch_live_updates(df, selected_date)
    loading_placeholder.empty()

    # Calculations
    df['SignalPrice'] = pd.to_numeric(df['SignalPrice'], errors='coerce')
    df['Live_Price'] = pd.to_numeric(df['Live_Price'], errors='coerce')
    df['Live_Move_Pct'] = ((df['Live_Price'] - df['SignalPrice']) / df['SignalPrice']) * 100
    df['Live_Move_Pct'] = df['Live_Move_Pct'].fillna(0.0)
    df['Visual_Side'] = df['Direction'].map({'LONG': '🟢 LONG', 'SHORT': '🔴 SHORT'})

    # --- METRICS ---
    bull_count = len(df[df['Direction'] == 'LONG'])
    bear_count = len(df[df['Direction'] == 'SHORT'])
    elite_count = len(df[df['SuperScore'] >= 100])
    avg_score = int(df['SuperScore'].mean()) if not df.empty else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(metric_card("Elite Setups", elite_count, "Score 100+", "#eab308", glow=True), unsafe_allow_html=True)
    c2.markdown(metric_card("Avg Quality", avg_score, "SuperScore", "#60a5fa"), unsafe_allow_html=True)
    c3.markdown(metric_card("Bullish", bull_count, "Longs", "#22c55e"), unsafe_allow_html=True)
    c4.markdown(metric_card("Bearish", bear_count, "Shorts", "#ef4444"), unsafe_allow_html=True)

    st.divider()

    # --- TABLE ---
    if 'Name' in df.columns:
        df['Cleaned_Name'] = df['Name'].replace(TICKER_CORRECTIONS)
        df['TV_Symbol'] = DEFAULT_EXCHANGE + ":" + df['Cleaned_Name'].str.replace('&', '_').str.replace(' ', '')
        df['Chart'] = "https://www.tradingview.com/chart/?symbol=" + df['TV_Symbol']

    with st.container(border=True):
        st.markdown('<div class="table-header">🚨 Active Market Signals</div>', unsafe_allow_html=True)
        if not df.empty:
            df_sorted = df.sort_values(by='SuperScore', ascending=False)
            st.data_editor(
                df_sorted[['Chart', 'Time', 'Name', 'Visual_Side', 'SuperScore', 'Alignment', 'Live_Move_Pct', 'RS_Score', 'RVOL']], 
                column_config={
                    "Chart": st.column_config.LinkColumn("View", display_text="📊", width="small"),
                    "Time": st.column_config.TextColumn("Entry", width="small"),
                    "Name": st.column_config.TextColumn("Ticker", width="medium"),
                    "Visual_Side": st.column_config.TextColumn("Side", width="small"),
                    "SuperScore": st.column_config.NumberColumn("Score", format="%d"),
                    "Alignment": st.column_config.TextColumn("Context", width="small"),
                    "Live_Move_Pct": st.column_config.NumberColumn("PnL %", format="%.2f%%"),
                    "RS_Score": st.column_config.NumberColumn("RS", format="%.2f"),
                    "RVOL": st.column_config.NumberColumn("Vol", format="%.1fx"),
                },
                hide_index=True, use_container_width=True, disabled=True, key="unified_table"
            )

# =========================================================
# PAGE 2: INTRADAY BOOST (NEW)
# =========================================================
def render_intraday_boost(selected_date):
    st.header("🚀 Intraday Boost")
    st.info("Top Gainers/Losers accumulated till 12 PM + NSE OI Spurts.")

    df = load_data_from_dynamodb(selected_date, "INTRADAY_BOOST")
    if df.empty:
        st.warning("No Boost alerts found.")
        return

    # Process Data
    if 'Name' in df.columns:
        df['Cleaned_Name'] = df['Name'].replace(TICKER_CORRECTIONS)
        df['TV_Symbol'] = DEFAULT_EXCHANGE + ":" + df['Cleaned_Name'].str.replace(' ', '')
        df['Chart'] = "https://www.tradingview.com/chart/?symbol=" + df['TV_Symbol']

    # Sort
    if 'SignalPrice' in df.columns: df['SignalPrice'] = pd.to_numeric(df['SignalPrice'], errors='coerce')
    
    st.markdown("### 🔥 Broken Levels Watchlist")

    # Split into Bullish (LONG) and Bearish (SHORT)
    df_bull = df[df['Direction'] == 'LONG'].copy()
    df_bear = df[df['Direction'] == 'SHORT'].copy()

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### 🟢 Bullish Scans")
        if not df_bull.empty:
            st.data_editor(
                df_bull[['Chart', 'Time', 'Name', 'BreakType', 'SignalPrice', 'OI_Change']],
                column_config={
                    "Chart": st.column_config.LinkColumn("Chart", display_text="📈"),
                    "SignalPrice": st.column_config.NumberColumn("Price", format="%.2f"),
                    "OI_Change": st.column_config.NumberColumn("OI Chg %", format="%.2f%%"),
                    "BreakType": st.column_config.TextColumn("Level", help="PDH: Prev Day High")
                },
                use_container_width=True, hide_index=True, disabled=True, key="bull_table"
            )
        else:
            st.caption("No Bullish setups found.")

    with col2:
        st.markdown("#### 🔴 Bearish Scans")
        if not df_bear.empty:
            st.data_editor(
                df_bear[['Chart', 'Time', 'Name', 'BreakType', 'SignalPrice', 'OI_Change']],
                column_config={
                    "Chart": st.column_config.LinkColumn("Chart", display_text="📉"),
                    "SignalPrice": st.column_config.NumberColumn("Price", format="%.2f"),
                    "OI_Change": st.column_config.NumberColumn("OI Chg %", format="%.2f%%"),
                    "BreakType": st.column_config.TextColumn("Level", help="PDL: Prev Day Low")
                },
                use_container_width=True, hide_index=True, disabled=True, key="bear_table"
            )
        else:
            st.caption("No Bearish setups found.")


# =========================================================
# MAIN NAVIGATION
# =========================================================
with st.sidebar:
    # --- RESTORED ORIGINAL SVG LOGO ---
    st.markdown("""
<div style="text-align: left; margin-bottom: 25px; padding-left: 10px;">
<svg width="250" height="60" viewBox="0 0 400 70" fill="none" xmlns="http://www.w3.org/2000/svg">
<defs>
<linearGradient id="bullGradient" x1="0%" y1="100%" x2="100%" y2="0%">
<stop offset="0%" style="stop-color:#00F260;stop-opacity:1" />
<stop offset="100%" style="stop-color:#0575E6;stop-opacity:1" />
</linearGradient>
<linearGradient id="bearGradient" x1="0%" y1="0%" x2="100%" y2="100%">
<stop offset="0%" style="stop-color:#FF416C;stop-opacity:1" />
<stop offset="100%" style="stop-color:#FF4B2B;stop-opacity:1" />
</linearGradient>
<filter id="glow" x="-20%" y="-20%" width="140%" height="140%">
<feGaussianBlur stdDeviation="2" result="coloredBlur"/>
<feMerge>
<feMergeNode in="coloredBlur"/>
<feMergeNode in="SourceGraphic"/>
</feMerge>
</filter>
</defs>
<g filter="url(#glow)">
<path d="M15 15 C 30 15, 45 55, 65 65" stroke="url(#bearGradient)" stroke-width="6" stroke-linecap="round" stroke-linejoin="round"/>
<path d="M65 65 L 50 63 M 65 65 L 63 50" stroke="url(#bearGradient)" stroke-width="6" stroke-linecap="round" stroke-linejoin="round"/>
<path d="M15 65 C 30 65, 45 25, 65 15" stroke="url(#bullGradient)" stroke-width="6" stroke-linecap="round" stroke-linejoin="round"/>
<path d="M65 15 L 50 17 M 65 15 L 63 30" stroke="url(#bullGradient)" stroke-width="6" stroke-linecap="round" stroke-linejoin="round"/>
</g>
<text x="85" y="52" fill="#FFFFFF" font-family="'Inter', sans-serif" font-weight="700" font-size="42" letter-spacing="-1">Signal</text>
<text x="215" y="52" fill="url(#bullGradient)" font-family="'Inter', sans-serif" font-weight="700" font-size="42" letter-spacing="-1">X</text>
</svg>
</div>
""", unsafe_allow_html=True)
    
    st.divider()
    page = st.radio("Navigate", ["SignalX (Original)", "Intraday Boost"])
    st.divider()
    india_tz = pytz.timezone('Asia/Kolkata')
    selected_date = st.date_input("📅 Select Date", datetime.now(india_tz).date())

if page == "SignalX (Original)":
    render_signalx(selected_date)
elif page == "Intraday Boost":
    render_intraday_boost(selected_date)
