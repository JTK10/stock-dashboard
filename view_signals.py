import streamlit as st
import pandas as pd
import boto3
from boto3.dynamodb.conditions import Attr
import os
from decimal import Decimal
from datetime import datetime
import pytz
import yfinance as yf
from streamlit_autorefresh import st_autorefresh
from streamlit_option_menu import option_menu  # <--- NEW LIBRARY

# --- 1. PAGE CONFIG & AUTO REFRESH ---
st.set_page_config(page_title="Scanner Dashboard", layout="wide", page_icon="⚡")

# Auto-refresh every 5 minutes (300 seconds) to get fresh prices
count = st_autorefresh(interval=300 * 1000, key="datarefresh")

# --- 2. CUSTOM CSS (FOR THE DARK CARD LOOK) ---
st.markdown("""
<style>
    /* Card Container Style */
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

# === TRADINGVIEW MAPPING (KEPT EXACTLY AS IS) ===
TICKER_CORRECTIONS = {
    "PATANJALI FOODS LIMITED": "PATANJALI",
    "INDUSIND BANK LIMITED": "INDUSINDBK",
    "COAL INDIA LTD": "COALINDIA",
    "TATA MOTORS LIMITED": "TATAMOTORS",
    "INDIAN ENERGY EXC LTD": "IEX",
    "RELIANCE INDUSTRIES LTD": "RELIANCE",
    "CENTRAL DEPO SER (I) LTD": "CDSL",
    "GRASIM INDUSTRIES LTD": "GRASIM",
    "INDIAN RENEWABLE ENERGY": "IREDA",
    "LIFE INSURA CORP OF INDIA": "LICI",
    "FEDERAL BANK LTD": "FEDERALBNK",
    "JSW STEEL LIMITED": "JSWSTEEL",
    "RAIL VIKAS NIGAM LIMITED": "RVNL",
    "PIDILITE INDUSTRIES LTD": "PIDILITIND",
    "MANAPPURAM FINANCE LTD": "MANAPPURAM",
    "TORRENT PHARMACEUTICALS L": "TORNTPHARM",
    "BAJAJ AUTO LIMITED": "BAJAJ-AUTO",
    "LIC HOUSING FINANCE LTD": "LICHSGFIN",
    "DR. REDDY S LABORATORIES": "DRREDDY",
    "CIPLA LTD": "CIPLA",
    "MANKIND PHARMA LIMITED": "MANKIND",
    "NATIONAL ALUMINIUM CO LTD": "NATIONALUM",
    "CANARA BANK": "CANBK",
    "AVENUE SUPERMARTS LIMITED": "DMART",
    "STEEL AUTHORITY OF INDIA": "SAIL",
    "INDIAN RAIL TOUR CORP LTD": "IRCTC",
    "TORRENT POWER LTD": "TORNTPOWER",
    "JUBILANT FOODWORKS LTD": "JUBLFOOD",
    "INFO EDGE (I) LTD": "NAUKRI",
    "INTERGLOBE AVIATION LTD": "INDIGO",
    "UNION BANK OF INDIA": "UNIONBANK",
    "BAJAJ FINSERV LTD.": "BAJAJFINSV",
    "SUN PHARMACEUTICAL IND L": "SUNPHARMA",
    "EXIDE INDUSTRIES LTD": "EXIDEIND",
    "INDUS TOWERS LIMITED": "INDUSTOWER",
    "BHARAT PETROLEUM CORP  LT": "BPCL",
    "360 ONE WAM LIMITED": "360ONE",
    "SOLAR INDUSTRIES (I) LTD": "SOLARINDS",
    "TATA POWER CO LTD": "TATAPOWER",
    "SUPREME INDUSTRIES LTD": "SUPREMEIND",
    "ONE 97 COMMUNICATIONS LTD": "PAYTM",
    "TATA TECHNOLOGIES LIMITED": "TATATECH",
    "HERO MOTOCORP LIMITED": "HEROMOTOCO",
    "MARICO LIMITED": "MARICO",
    "VODAFONE IDEA LIMITED": "IDEA",
    "BRITANNIA INDUSTRIES LTD": "BRITANNIA",
    "ICICI PRU LIFE INS CO LTD": "ICICIPRULI",
    "AU SMALL FINANCE BANK LTD": "AUBANK",
    "INDIAN RAILWAY FIN CORP L": "IRFC",
    "FORTIS HEALTHCARE LTD": "FORTIS",
    "HINDUSTAN AERONAUTICS LTD": "HAL",
    "BHARAT FORGE LTD": "BHARATFORG",
    "BHARTI AIRTEL LIMITED": "BHARTIARTL",
    "TRENT LTD": "TRENT",
    "KALYAN JEWELLERS IND LTD": "KALYANKJIL",
    "AXIS BANK LIMITED": "AXISBANK",
    "BOSCH LIMITED": "BOSCHLTD",
    "ZYDUS LIFESCIENCES LTD": "ZYDUSLIFE",
    "PI INDUSTRIES LTD": "PIIND",
    "HINDUSTAN PETROLEUM CORP": "HINDPETRO",
    "MARUTI SUZUKI INDIA LTD.": "MARUTI",
    "THE PHOENIX MILLS LTD": "PHOENIXLTD",
    "NBCC (INDIA) LIMITED": "NBCC",
    "BSE LIMITED": "BSE",
    "HINDUSTAN ZINC LIMITED": "HINDZINC",
    "IDFC FIRST BANK LIMITED": "IDFCFIRSTB",
    "HSG & URBAN DEV CORPN LTD": "HUDCO",
    "ITC LTD": "ITC",
    "AUROBINDO PHARMA LTD": "AUROPHARMA",
    "KAYNES TECHNOLOGY IND LTD": "KAYNES",
    "TVS MOTOR COMPANY  LTD": "TVSMOTOR",
    "BHEL": "BHEL",
    "EICHER MOTORS LTD": "EICHERMOT",
    "BAJAJ FINANCE LIMITED": "BAJFINANCE",
    "TATA ELXSI LIMITED": "TATAELXSI",
    "NCC LIMITED": "NCC",
    "OBEROI REALTY LIMITED": "OBEROIRLTY",
    "HAVELLS INDIA LIMITED": "HAVELLS",
    "TATA CONSULTANCY SERV LT": "TCS",
    "JINDAL STEEL LIMITED": "JINDALSTEL",
    "CROMPT GREA CON ELEC LTD": "CROMPTON",
    "ALKEM LABORATORIES LTD.": "ALKEM",
    "ICICI BANK LTD.": "ICICIBANK",
    "DLF LIMITED": "DLF",
    "NESTLE INDIA LIMITED": "NESTLEIND",
    "AMBER ENTERPRISES (I) LTD": "AMBER",
    "SYNGENE INTERNATIONAL LTD": "SYNGENE",
    "DIXON TECHNO (INDIA) LTD": "DIXON",
    "COMPUTER AGE MNGT SER LTD": "CAMS",
    "BIOCON LIMITED.": "BIOCON",
    "DABUR INDIA LTD": "DABUR",
    "PAGE INDUSTRIES LTD": "PAGEIND",
    "ADANI PORT & SEZ LTD": "ADANIPORTS",
    "SONA BLW PRECISION FRGS L": "SONACOMS",
    "CYIENT LIMITED": "CYIENT",
    "ADANI GREEN ENERGY LTD": "ADANIGREEN",
    "CONTAINER CORP OF IND LTD": "CONCOR",
    "COFORGE LIMITED": "COFORGE",
    "HDFC AMC LIMITED": "HDFCAMC",
    "HDFC LIFE INS CO LTD": "HDFCLIFE",
    "DALMIA BHARAT LIMITED": "DALBHARAT",
    "ASTRAL LIMITED": "ASTRAL",
    "CUMMINS INDIA LTD": "CUMMINSIND",
    "ANGEL ONE LIMITED": "ANGELONE",
    "SUZLON ENERGY LIMITED": "SUZLON",
    "ETERNAL LIMITED": "ETERNAL",
    "APL APOLLO TUBES LTD": "APLAPOLLO",
    "HCL TECHNOLOGIES LTD": "HCLTECH",
    "AMBUJA CEMENTS LTD": "AMBUJACEM",
    "PG ELECTROPLAST LTD": "PGEL",
    "COLGATE PALMOLIVE LTD.": "COLPAL",
    "TATA CONSUMER PRODUCT LTD": "TATACONSUM",
    "KOTAK MAHINDRA BANK LTD": "KOTAKBANK",
    "NHPC LTD": "NHPC",
    "SBI CARDS & PAY SER LTD": "SBICARD",
    "ICICI LOMBARD GIC LIMITED": "ICICIGI",
    "HDFC BANK LTD": "HDFCBANK",
    "LUPIN LIMITED": "LUPIN",
    "SBI LIFE INSURANCE CO LTD": "SBILIFE",
    "NMDC LTD.": "NMDC",
    "APOLLO HOSPITALS ENTER. L": "APOLLOHOSP",
    "LODHA DEVELOPERS LIMITED": "LODHA",
    "NTPC LTD": "NTPC",
    "CG POWER AND IND SOL LTD": "CGPOWER",
    "MAHINDRA & MAHINDRA LTD": "M_M",
    "INDIAN OIL CORP LTD": "IOC",
    "HFCL LIMITED": "HFCL",
    "KPIT TECHNOLOGIES LIMITED": "KPITTECH",
    "SAMMAAN CAPITAL LIMITED": "SAMMAANCAP",
    "HINDALCO  INDUSTRIES  LTD": "HINDALCO",
    "PERSISTENT SYSTEMS LTD": "PERSISTENT",
    "CHOLAMANDALAM IN & FIN CO": "CHOLAFIN",
    "INOX WIND LIMITED": "INOXWIND",
    "BLUE STAR LIMITED": "BLUESTARCO",
    "ABB INDIA LIMITED": "ABB",
    "LARSEN & TOUBRO LTD.": "LT",
    "ULTRATECH CEMENT LIMITED": "ULTRACEMCO",
    "INFOSYS LIMITED": "INFY",
    "GAIL (INDIA) LTD": "GAIL",
    "PETRONET LNG LIMITED": "PETRONET",
    "POWER FIN CORP LTD.": "PFC",
    "PNB HOUSING FIN LTD.": "PNBHOUSING",
    "WIPRO LTD": "WIPRO",
    "REC LIMITED": "RECLTD",
    "PRESTIGE ESTATE LTD": "PRESTIGE",
    "FSN E COMMERCE VENTURES": "NYKAA",
    "ADANI ENERGY SOLUTION LTD": "ADANIENSOL",
    "ADANI ENTERPRISES LIMITED": "ADANIENT",
    "TATA STEEL LIMITED": "TATASTEEL",
    "RBL BANK LIMITED": "RBLBANK",
    "INDRAPRASTHA GAS LTD": "IGL",
    "YES BANK LIMITED": "YESBANK",
    "SAMVRDHNA MTHRSN INTL LTD": "MOTHERSON",
    "GLENMARK PHARMACEUTICALS": "GLENMARK",
    "ADITYA BIRLA CAPITAL LTD.": "ABCAPITAL",
    "L&T FINANCE LIMITED": "LTF",
    "UNO MINDA LIMITED": "UNOMINDA",
    "ORACLE FIN SERV SOFT LTD.": "OFSS",
    "BANDHAN BANK LIMITED": "BANDHANBNK",
    "TECH MAHINDRA LIMITED": "TECHM",
    "IIFL FINANCE LIMITED": "IIFL",
    "JIO FIN SERVICES LTD": "JIOFIN",
    "MPHASIS LIMITED": "MPHASIS",
    "SIEMENS LTD": "SIEMENS",
    "OIL INDIA LTD": "OIL",
    "JSW ENERGY LIMITED": "JSWENERGY",
    "TUBE INVEST OF INDIA LTD": "TIINDIA",
    "LTIMINDTREE LIMITED": "LTIM",
    "ASHOK LEYLAND LTD": "ASHOKLEY",
    "THE INDIAN HOTELS CO. LTD": "INDHOTEL",
    "DELHIVERY LIMITED": "DELHIVERY",
    "DIVI S LABORATORIES LTD": "DIVISLAB",
    "TITAGARH RAIL SYSTEMS LTD": "TITAGARH",
    "BANK OF BARODA": "BANKBARODA",
    "MAX FINANCIAL SERV LTD": "MFSL",
    "MULTI COMMODITY EXCHANGE": "MCX",
    "TITAN COMPANY LIMITED": "TITAN",
    "VOLTAS LTD": "VOLTAS",
    "SRF LTD": "SRF",
    "ASIAN PAINTS LIMITED": "ASIANPAINT",
    "MUTHOOT FINANCE LIMITED": "MUTHOOTFIN",
    "PB FINTECH LIMITED": "POLICYBZR",
    "POLYCAB INDIA LIMITED": "POLYCAB",
    "BHARAT ELECTRONICS LTD": "BEL",
    "BANK OF INDIA": "BANKINDIA",
    "BHARAT DYNAMICS LIMITED": "BDL",
    "HINDUSTAN UNILEVER LTD.": "HINDUNILVR",
    "POWER GRID CORP. LTD.": "POWERGRID",
    "GODREJ PROPERTIES LTD": "GODREJPROP",
    "GMR AIRPORTS LIMITED": "GMRAIRPORT",
    "GODREJ CONSUMER PRODUCTS": "GODREJCP",
    "INDIAN BANK": "INDIANB",
    "PIRAMAL PHARMA LIMITED": "PPLPHARMA",
    "OIL AND NATURAL GAS CORP.": "ONGC",
    "KEI INDUSTRIES LTD.": "KEI",
    "LAURUS LABS LIMITED": "LAURUSLABS",
    "STATE BANK OF INDIA": "SBIN",
    "SHREE CEMENT LIMITED": "SHREECEM",
    "SHRIRAM FINANCE LIMITED": "SHRIRAMFIN",
    "PUNJAB NATIONAL BANK": "PNB",
    "UNITED SPIRITS LIMITED": "UNITDSPR",
    "UPL LIMITED": "UPL",
    "VARUN BEVERAGES LIMITED": "VBL",
    "VEDANTA LIMITED": "VEDL",
    "KFIN TECHNOLOGIES LIMITED": "KFINTECH",
    "NUVAMA WEALTH MANAGE LTD": "NUVAMA",
    "MAX HEALTHCARE INS LTD": "MAXHEALTH",
    "MAZAGON DOCK SHIPBUIL LTD": "MAZDOCK",
    "TATA CHEMICALS LTD": "TATACHEM"
}

# --- LOAD DATA FROM DYNAMODB (KEPT EXACTLY AS IS) ---
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

    data = [convert_decimal(item) for item in items]
    df = pd.DataFrame(data)

    if 'Price' in df.columns and 'SignalPrice' not in df.columns:
        df.rename(columns={'Price': 'SignalPrice'}, inplace=True)
    if 'Side' in df.columns:
        df['Direction'] = df['Side'].map({'Bullish': 'LONG', 'Bearish': 'SHORT'})
    elif 'Signal' in df.columns:
        df['Direction'] = df['Signal'].map({'LONG': 'LONG', 'SHORT': 'SHORT'})
    
    # Ensure numeric cols exist
    numeric_cols = ['SignalPrice', 'TargetPrice', 'TargetPct', 'RVOL', 'NetMovePct', 'RangeSoFarPct']
    for col in numeric_cols:
        if col not in df.columns: df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
        
    return df

# --- LIVE DATA ENGINE (REQUIRED FOR PNL %) ---
def fetch_live_updates(df):
    if df.empty or 'Name' not in df.columns: return df

    if 'Cleaned_Name' not in df.columns:
        df['Cleaned_Name'] = df['Name'].replace(TICKER_CORRECTIONS)
    
    unique_tickers = df['Cleaned_Name'].unique().tolist()
    yahoo_tickers = [f"{t}.NS" for t in unique_tickers]
    
    try:
        # Fast batch download
        live_data = yf.download(tickers=yahoo_tickers, period="1d", interval="1m", progress=False)['Close'].iloc[-1]
        
        price_map = {}
        for ticker in unique_tickers:
            yf_ticker = f"{ticker}.NS"
            try:
                if isinstance(live_data, pd.Series):
                    price = live_data.get(yf_ticker, 0)
                else:
                    price = live_data if len(unique_tickers) == 1 else 0
                price_map[ticker] = float(price) if price > 0 else 0
            except:
                price_map[ticker] = 0

        df['Live_Price'] = df['Cleaned_Name'].map(price_map)
        df['Live_Move_Pct'] = ((df['Live_Price'] - df['SignalPrice']) / df['SignalPrice']) * 100
        df['Live_Move_Pct'] = df['Live_Move_Pct'].fillna(0.0)
        
    except Exception as e:
        df['Live_Price'] = 0.0
        df['Live_Move_Pct'] = 0.0
        
    return df

# --- MAIN APP UI ---

# 1. SIDEBAR (NEW)
with st.sidebar:
    selected = option_menu(
        menu_title="TradeFinder",
        options=["Market Pulse", "Insider Strategy", "Sector Scope"],
        icons=["activity", "eye", "grid"],
        menu_icon="cast",
        default_index=0,
    )
    st.divider()
    
    # Date Selection (Moved to Sidebar)
    india_tz = pytz.timezone('Asia/Kolkata')
    today_india = datetime.now(india_tz).date()
    selected_date = st.date_input("📅 Select Date", today_india)

# 2. MARKET PULSE PAGE (THE DASHBOARD)
if selected == "Market Pulse":
    st.title("🚀 Market Pulse")

    # Load Data
    df = load_data_from_dynamodb(selected_date)
    
    if df.empty:
        st.info(f"No alerts found for {selected_date}")
        st.stop()

    # Fetch Live Updates (Only for Today)
    if selected_date == today_india:
        with st.spinner('⚡ Fetching Live Market Rates...'):
            df = fetch_live_updates(df)
    else:
        df['Live_Price'] = 0.0
        df['Live_Move_Pct'] = 0.0

    # Chart Link Prep
    if 'Name' in df.columns:
        df['Cleaned_Name'] = df['Name'].replace(TICKER_CORRECTIONS)
        df['TV_Symbol'] = DEFAULT_EXCHANGE + ":" + df['Cleaned_Name'].str.replace(' ', '')
        df['Chart'] = "https://www.tradingview.com/chart/?symbol=" + df['TV_Symbol']

    # --- SPLIT LAYOUT (BULLISH vs BEARISH) ---
    col1, col2 = st.columns(2)

    # LEFT COLUMN: BULLISH
    with col1:
        st.markdown('<div class="dashboard-card">', unsafe_allow_html=True)
        st.subheader("🟢 BULLISH BEACONS")
        
        bull_df = df[df['Direction'] == 'LONG'].copy()
        if not bull_df.empty:
            bull_df = bull_df.sort_values(by='Live_Move_Pct', ascending=False)
            
            st.data_editor(
                bull_df[['Chart', 'Name', 'Live_Move_Pct', 'SignalPrice', 'Live_Price']],
                column_config={
                    "Chart": st.column_config.LinkColumn("View", display_text="📈"),
                    "Live_Move_Pct": st.column_config.ProgressColumn("PnL %", format="%.2f%%", min_value=-5, max_value=5),
                    "Live_Price": st.column_config.NumberColumn("CMP", format="%.2f"),
                    "SignalPrice": st.column_config.NumberColumn("Entry", format="%.2f"),
                },
                hide_index=True,
                use_container_width=True,
                disabled=True,
                key="bull_table"
            )
        else:
            st.caption("No Bullish Signals yet.")
        st.markdown('</div>', unsafe_allow_html=True)

    # RIGHT COLUMN: BEARISH
    with col2:
        st.markdown('<div class="dashboard-card">', unsafe_allow_html=True)
        st.subheader("🔴 BEARISH DRAGONS")
        
        bear_df = df[df['Direction'] == 'SHORT'].copy()
        if not bear_df.empty:
            bear_df = bear_df.sort_values(by='Live_Move_Pct', ascending=True) # Sort losers to top
            
            st.data_editor(
                bear_df[['Chart', 'Name', 'Live_Move_Pct', 'SignalPrice', 'Live_Price']],
                column_config={
                    "Chart": st.column_config.LinkColumn("View", display_text="📉"),
                    "Live_Move_Pct": st.column_config.ProgressColumn("PnL %", format="%.2f%%", min_value=-5, max_value=5),
                    "Live_Price": st.column_config.NumberColumn("CMP", format="%.2f"),
                    "SignalPrice": st.column_config.NumberColumn("Entry", format="%.2f"),
                },
                hide_index=True,
                use_container_width=True,
                disabled=True,
                key="bear_table"
            )
        else:
            st.caption("No Bearish Signals yet.")
        st.markdown('</div>', unsafe_allow_html=True)

else:
    st.write("Work in progress page...")
