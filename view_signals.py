import streamlit as st
import pandas as pd
import boto3
from boto3.dynamodb.conditions import Attr
import os
from decimal import Decimal
from datetime import datetime
import pytz
import yfinance as yf
import altair as alt
import re  # <--- NEW: For smart text cleaning
from streamlit_autorefresh import st_autorefresh
from streamlit_option_menu import option_menu

# --- 1. PAGE CONFIG & AUTO REFRESH ---
st.set_page_config(page_title="Scanner Dashboard", layout="wide", page_icon="⚡")
count = st_autorefresh(interval=300 * 1000, key="datarefresh")

# --- 2. CUSTOM CSS ---
st.markdown("""
<style>
    .dashboard-card {
        background-color: #1E1E1E;
        padding: 20px;
        border-radius: 10px;
        border: 1px solid #333;
        margin-bottom: 20px;
    }
</style>
""", unsafe_allow_html=True)

# --- CONFIG ---
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1") 
DYNAMODB_TABLE = os.getenv("DYNAMODB_TABLE", "SentAlerts")
DEFAULT_EXCHANGE = "NSE"

# --- 3. SMART TICKER ENGINE (THE FIX) ---
def get_yahoo_symbol(raw_name):
    """
    Smartly converts any company name to its NSE Yahoo Symbol.
    1. Checks explicit map for tricky ones.
    2. Applies rules to clean up standard names.
    """
    raw_name = str(raw_name).strip().upper()

    # A. THE MASTER MAP (For stocks that don't match their names)
    # Add any stock here that still shows 0.00
    EXPLICIT_MAP = {
        "NIFTY 50": "^NSEI",
        "NIFTY BANK": "^NSEBANK",
        "BAJAJ FINANCE": "BAJFINANCE",
        "BAJAJ FINSERV": "BAJAJFINSV",
        "M&M": "M&M",
        "MAHINDRA & MAHINDRA": "M&M",
        "L&T": "LT",
        "LARSEN & TOUBRO": "LT",
        "HINDUSTAN UNILEVER": "HINDUNILVR",
        "HIND UNILEVER": "HINDUNILVR",
        "TITAN COMPANY": "TITAN",
        "SUN PHARMA": "SUNPHARMA",
        "BHARAT PETROLEUM": "BPCL",
        "HINDUSTAN PETROLEUM": "HINDPETRO",
        "INDIAN OIL": "IOC",
        "POWER GRID": "POWERGRID",
        "HERO MOTOCORP": "HEROMOTOCO",
        "TATA CONSUMER": "TATACONSUM",
        "TATA MOTORS": "TATAMOTORS",
        "TATA STEEL": "TATASTEEL",
        "TATA POWER": "TATAPOWER",
        "JIO FINANCIAL": "JIOFIN",
        "LIC HOUSING": "LICHSGFIN",
        "NAM-INDIA": "NAM-INDIA",
        "MCDOWELL-N": "MCDOWELL-N",
        "UNITED SPIRITS": "MCDOWELL-N",
        "BRITANNIA": "BRITANNIA",
        "NESTLE INDIA": "NESTLEIND",
        "ASIAN PAINTS": "ASIANPAINT",
        "INDUSIND BANK": "INDUSINDBK",
        "KOTAK MAHINDRA BANK": "KOTAKBANK",
        "AU SMALL FINANCE": "AUBANK",
        "CHOLAMANDALAM INV": "CHOLAFIN",
        "MUTHOOT FINANCE": "MUTHOOTFIN",
        "SHRIRAM FINANCE": "SHRIRAMFIN",
        "HDFC LIFE": "HDFCLIFE",
        "SBI LIFE": "SBILIFE",
        "ICICI PRU": "ICICIPRULI",
        "ICICI LOMBARD": "ICICIGI",
        "BAJAJ AUTO": "BAJAJ-AUTO",
        "DIVIS LAB": "DIVISLAB",
        "DR REDDY": "DRREDDY",
        "APOLLO HOSPITALS": "APOLLOHOSP",
        "MAX HEALTHCARE": "MAXHEALTH",
        "SYNGENE INTL": "SYNGENE",
        "GODREJ PROPERTIES": "GODREJPROP",
        "GODREJ CONSUMER": "GODREJCP",
        "BHARAT FORGE": "BHARATFORG",
        "BALKRISHNA IND": "BALKRISIND",
        "SAMVARDHANA MOTHERSON": "MOTHERSON",
        "MOTHERSON SUMI": "MOTHERSON",
        "EICHER MOTORS": "EICHERMOT",
        "TVS MOTOR": "TVSMOTOR",
        "HINDALCO": "HINDALCO",
        "JINDAL STEEL": "JINDALSTEL",
        "JSW STEEL": "JSWSTEEL",
        "SAIL": "SAIL",
        "COAL INDIA": "COALINDIA",
        "ADANI ENTERPRISES": "ADANIENT",
        "ADANI PORTS": "ADANIPORTS",
        "AMBUJA CEMENTS": "AMBUJACEM",
        "ULTRATECH CEMENT": "ULTRACEMCO",
        "SHREE CEMENT": "SHREECEM",
        "GRASIM IND": "GRASIM",
        "PIDILITE IND": "PIDILITIND",
        "BERGER PAINTS": "BERGEPAINT",
        "HAVELLS INDIA": "HAVELLS",
        "SIEMENS": "SIEMENS",
        "ABB INDIA": "ABB",
        "CUMMINS INDIA": "CUMMINSIND",
        "BEL": "BEL",
        "HAL": "HAL",
        "BHEL": "BHEL",
        "DLF": "DLF",
        "OBEROI REALTY": "OBEROIRLTY",
        "PHOENIX MILLS": "PHOENIXLTD",
        "PRESTIGE ESTATES": "PRESTIGE",
        "IRCTC": "IRCTC",
        "INFO EDGE": "NAUKRI",
        "ZOMATO": "ZOMATO",
        "PAYTM": "PAYTM",
        "PB FINTECH": "POLICYBZR",
        "FSN E-COMMERCE": "NYKAA",
        "LTIMINDTREE": "LTIM",
        "TECH MAHINDRA": "TECHM",
        "WIPRO": "WIPRO",
        "INFOSYS": "INFY",
        "TCS": "TCS",
        "HCL TECH": "HCLTECH",
        "PERSISTENT SYS": "PERSISTENT",
        "COFORGE": "COFORGE",
        "MPHASIS": "MPHASIS",
        "INTERGLOBE AVIATION": "INDIGO",
        "PI INDUSTRIES": "PIIND",
        "UPL": "UPL",
        "COROMANDEL INTL": "COROMANDEL",
        "SRF": "SRF",
        "NAVIN FLUORINE": "NAVINFLUOR",
        "AARTI IND": "AARTIIND",
        "TATA CHEMICALS": "TATACHEM",
        "TRENT": "TRENT",
        "ABFRL": "ABFRL",
        "PAGE INDUSTRIES": "PAGEIND",
        "BOSCH": "BOSCHLTD",
        "MRF": "MRF",
        "APOLLO TYRES": "APOLLOTYRE",
        "EXIDE IND": "EXIDEIND",
        "VOLTAS": "VOLTAS",
        "CROMPTON GREAVES": "CROMPTON",
        "WHIRLPOOL": "WHIRLPOOL",
        "BLUE STAR": "BLUESTARCO",
        "DIXON TECH": "DIXON",
        "POLYCAB": "POLYCAB",
        "KEI INDUSTRIES": "KEI",
        "ASTRAL": "ASTRAL",
        "SUPREME IND": "SUPREMEIND",
        "BALRAMPUR CHINI": "BALRAMCHIN",
        "JUBILANT FOOD": "JUBLFOOD",
        "ZEE ENT": "ZEEL",
        "PVR INOX": "PVRINOX",
        "SUN TV": "SUNTV",
        "CONCOR": "CONCOR",
        "DELHIVERY": "DELHIVERY",
        "TATA ELXSI": "TATAELXSI",
        "KPIT TECH": "KPITTECH",
        "TATA TECHNOLOGIES": "TATATECH",
        "J&K BANK": "J&KBANK",
        "CANARA BANK": "CANBK",
        "BANK OF BARODA": "BANKBARODA",
        "PUNJAB NATIONAL": "PNB",
        "UNION BANK": "UNIONBANK",
        "INDIAN BANK": "INDIANB",
        "IDFC FIRST": "IDFCFIRSTB",
        "FEDERAL BANK": "FEDERALBNK",
        "RBL BANK": "RBLBANK",
        "BANDHAN BANK": "BANDHANBNK",
        "CITY UNION BANK": "CUB",
    }
    
    # 1. Check Exact Match in Dict
    for key, val in EXPLICIT_MAP.items():
        if key in raw_name:
            return f"{val}.NS"

    # B. SMART CLEANER (For regular names like 'INOX WIND LIMITED')
    # Remove standard suffix junk
    clean = raw_name
    remove_words = [
        "LIMITED", "LTD", "LTD.", "(INDIA)", "INDIA", "INDUSTRIES", 
        "ENTERPRISES", "SERVICES", "FINANCE", "HOLDINGS", "SYSTEMS",
        "TECHNOLOGIES", "CORP", "CORPORATION", "COMPANY", "BANK"
    ]
    for word in remove_words:
        clean = clean.replace(word, "")
    
    # Remove special chars and spaces
    clean = re.sub(r'[^A-Z0-9&]', '', clean)
    
    return f"{clean}.NS"

# --- 4. LOAD DATA ---
@st.cache_data(ttl=60)
def load_data_from_dynamodb(target_date):
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

    def convert_decimal(obj):
        if isinstance(obj, list): return [convert_decimal(i) for i in obj]
        elif isinstance(obj, dict): return {k: convert_decimal(v) for k, v in obj.items()}
        elif isinstance(obj, Decimal): return float(obj)
        return obj

    df = pd.DataFrame([convert_decimal(item) for item in items])

    # Standardize
    if 'Price' in df.columns and 'SignalPrice' not in df.columns:
        df.rename(columns={'Price': 'SignalPrice'}, inplace=True)
    if 'Side' in df.columns:
        df['Direction'] = df['Side'].map({'Bullish': 'LONG', 'Bearish': 'SHORT'})
    elif 'Signal' in df.columns:
        df['Direction'] = df['Signal'].map({'LONG': 'LONG', 'SHORT': 'SHORT'})

    numeric_cols = ['SignalPrice', 'TargetPrice', 'TargetPct', 'RVOL', 'NetMovePct', 'RangeSoFarPct']
    for col in numeric_cols:
        if col not in df.columns: df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
    
    return df

# --- 5. LIVE DATA ENGINE ---
def fetch_live_updates(df):
    if df.empty or 'Name' not in df.columns: return df

    # Generate Yahoo Tickers using the Smart Engine
    df['Yahoo_Ticker'] = df['Name'].apply(get_yahoo_symbol)
    
    unique_tickers = df['Yahoo_Ticker'].unique().tolist()
    
    try:
        live_data = yf.download(tickers=unique_tickers, period="1d", interval="1m", progress=False)['Close'].iloc[-1]
        
        price_map = {}
        for ticker in unique_tickers:
            try:
                if isinstance(live_data, pd.Series):
                    price = live_data.get(ticker, 0)
                else:
                    price = live_data if len(unique_tickers) == 1 else 0
                price_map[ticker] = float(price) if price > 0 else 0
            except:
                price_map[ticker] = 0

        df['Live_Price'] = df['Yahoo_Ticker'].map(price_map)
        df['Live_Move_Pct'] = ((df['Live_Price'] - df['SignalPrice']) / df['SignalPrice']) * 100
        df['Live_Move_Pct'] = df['Live_Move_Pct'].fillna(0.0)
        
    except Exception as e:
        df['Live_Price'] = 0.0
        df['Live_Move_Pct'] = 0.0
        
    return df

# --- SECTOR FETCHING HELPER ---
@st.cache_data(show_spinner=False)
def get_sector_map(ticker_list):
    sector_map = {}
    for raw_name in ticker_list:
        try:
            yf_ticker = get_yahoo_symbol(raw_name) # Use Smart Engine here too
            info = yf.Ticker(yf_ticker).info
            sector = info.get('sector', 'Others')
            
            if sector == 'Financial Services': sector = 'FIN SERVICE'
            if sector == 'Technology': sector = 'IT'
            if sector == 'Consumer Cyclical': sector = 'AUTO' 
            if sector == 'Basic Materials': sector = 'METAL'
            
            sector_map[raw_name] = sector.upper()
        except:
            sector_map[raw_name] = 'OTHERS'
    return sector_map

# --- MAIN APP UI ---

with st.sidebar:
    selected = option_menu(
        menu_title="TradeFinder",
        options=["Market Pulse", "Sector Scope"], 
        icons=["activity", "grid"],
        menu_icon="cast",
        default_index=0,
    )
    st.divider()
    india_tz = pytz.timezone('Asia/Kolkata')
    today_india = datetime.now(india_tz).date()
    selected_date = st.date_input("📅 Select Date", today_india)

if selected == "Market Pulse":
    st.title("🚀 Market Pulse")
    df = load_data_from_dynamodb(selected_date)
    
    if df.empty:
        st.info(f"No alerts found for {selected_date}")
        st.stop()

    if selected_date == today_india:
        with st.spinner('⚡ Fetching Live Market Rates...'):
            df = fetch_live_updates(df)
    else:
        df['Live_Price'] = 0.0
        df['Live_Move_Pct'] = 0.0
        
    # Generate Chart Links using Smart Tickers
    # We strip .NS for TradingView because TV doesn't use it
    df['TV_Symbol'] = DEFAULT_EXCHANGE + ":" + df['Name'].apply(get_yahoo_symbol).str.replace('.NS','').str.replace('&', '_') # TV uses _ for & sometimes
    df['Chart'] = "https://www.tradingview.com/chart/?symbol=" + df['TV_Symbol']

    col1, col2 = st.columns(2)

    with col1:
        st.markdown('<div class="dashboard-card">', unsafe_allow_html=True)
        st.subheader("🟢 BULLISH BEACONS")
        bull_df = df[df['Direction'] == 'LONG'].copy()
        if not bull_df.empty:
            bull_df = bull_df.sort_values(by='Live_Move_Pct', ascending=False)
            st.data_editor(
                bull_df[['Chart', 'Name', 'Live_Move_Pct', 'Time', 'RVOL']], 
                column_config={
                    "Chart": st.column_config.LinkColumn("View", display_text="📈"),
                    "Live_Move_Pct": st.column_config.ProgressColumn("%", format="%.2f%%", min_value=-5, max_value=5),
                    "Time": st.column_config.TextColumn("Entry Time"), 
                    "RVOL": st.column_config.NumberColumn("RVOL", format="%.2fx"),
                },
                hide_index=True, use_container_width=True, disabled=True, key="bull_table"
            )
        else:
            st.caption("No Bullish Signals yet.")
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="dashboard-card">', unsafe_allow_html=True)
        st.subheader("🔴 BEARISH DRAGONS")
        bear_df = df[df['Direction'] == 'SHORT'].copy()
        if not bear_df.empty:
            bear_df = bear_df.sort_values(by='Live_Move_Pct', ascending=True)
            st.data_editor(
                bear_df[['Chart', 'Name', 'Live_Move_Pct', 'Time', 'RVOL']], 
                column_config={
                    "Chart": st.column_config.LinkColumn("View", display_text="📉"),
                    "Live_Move_Pct": st.column_config.ProgressColumn("%", format="%.2f%%", min_value=-5, max_value=5),
                    "Time": st.column_config.TextColumn("Entry Time"),
                    "RVOL": st.column_config.NumberColumn("RVOL", format="%.2fx"),
                },
                hide_index=True, use_container_width=True, disabled=True, key="bear_table"
            )
        else:
            st.caption("No Bearish Signals yet.")
        st.markdown('</div>', unsafe_allow_html=True)

elif selected == "Sector Scope":
    st.title("🏙️ Sector Scope")
    st.caption("Active Sectors based on alerts")

    df = load_data_from_dynamodb(selected_date)
    if df.empty:
        st.info(f"No alerts found for {selected_date}")
        st.stop()

    if 'Name' in df.columns:
        unique_stocks = df['Name'].unique().tolist()
        
        with st.spinner(f"Mapping sectors for {len(unique_stocks)} stocks..."):
            sector_mapping = get_sector_map(unique_stocks)
        
        df['Sector'] = df['Name'].map(sector_mapping)
        sector_counts = df['Sector'].value_counts().reset_index()
        sector_counts.columns = ['Sector', 'Count']

        chart = alt.Chart(sector_counts).mark_bar(
            color='#2ecc71',
            cornerRadiusTopLeft=5,
            cornerRadiusTopRight=5
        ).encode(
            x=alt.X('Sector', sort='-y', axis=alt.Axis(labelAngle=-90, title=None)),
            y=alt.Y('Count', axis=alt.Axis(title=None, tickMinStep=1)),
            tooltip=['Sector', 'Count']
        ).configure_axis(
            grid=False, labelColor='#eee', domainColor='#333'
        ).configure_view(strokeWidth=0).properties(height=400)

        st.altair_chart(chart, use_container_width=True)

        st.divider()
        st.subheader("Sector Details")
        st.dataframe(
            df[['Name', 'Sector', 'Direction', 'SignalPrice']].sort_values(by='Sector'),
            use_container_width=True, hide_index=True
        )

else:
    st.write("Page under construction")
