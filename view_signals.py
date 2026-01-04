import streamlit as st
import pandas as pd
import boto3
from boto3.dynamodb.conditions import Attr
import os
from decimal import Decimal
from datetime import datetime, timedelta
import pytz
import yfinance as yf
import altair as alt
from streamlit_autorefresh import st_autorefresh
from streamlit_option_menu import option_menu

# --- 1. PAGE CONFIG ---
st.set_page_config(page_title="SignalX", layout="wide", page_icon="✖️")

# --- USER AUTHENTICATION (DISABLED) ---
USERS = {"admin": "admin123", "trader": "signalx"} 

if 'authenticated' not in st.session_state:
    st.session_state.authenticated = True 

# --- AUTO REFRESH ---
count = st_autorefresh(interval=300 * 1000, key="datarefresh")

# --- 2. CUSTOM CSS (UPGRADED) ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap');

    .stApp {
        background-color: #080a0e;
        font-family: 'Inter', sans-serif;
    }

    /* Part 3 & Final Touch: Hover & Shadows */
    [data-testid="stVerticalBlockBorderWrapper"] > div {
        background-color: #131722;
        border-radius: 12px;
        border: 1px solid #2a2e3a;
        box-shadow: 0 0 18px rgba(0,0,0,.35);
    }
    
    tbody tr:hover {
        background-color: rgba(255,255,255,0.04) !important;
    }

    /* Part 6: Gradient Progress Bars */
    [data-testid="stProgress"] div div {
        background: linear-gradient(90deg,#16a34a,#22c55e);
    }

    .header-bullish {
        color: #00FF7F;
        background: linear-gradient(90deg, rgba(0,255,127,0.1) 0%, rgba(0,0,0,0) 100%);
        border-left: 4px solid #00FF7F;
        padding: 10px 15px;
        margin-bottom: 15px;
        font-size: 1.2rem;
        font-weight: 800;
        letter-spacing: 1px;
        text-transform: uppercase;
        border-radius: 4px;
    }

    .header-bearish {
        color: #FF4B4B;
        background: linear-gradient(90deg, rgba(255,75,75,0.1) 0%, rgba(0,0,0,0) 100%);
        border-left: 4px solid #FF4B4B;
        padding: 10px 15px;
        margin-bottom: 15px;
        font-size: 1.2rem;
        font-weight: 800;
        letter-spacing: 1px;
        text-transform: uppercase;
        border-radius: 4px;
    }

    .block-container {
        padding-top: 2rem;
        padding-bottom: 3rem;
    }
     
    [data-testid="stSidebar"] {
        background-color: #0b0e14;
        border-right: 1px solid #2a2e3a;
    }
</style>
""", unsafe_allow_html=True)

# --- CONFIG ---
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1") 
DYNAMODB_TABLE = os.getenv("DYNAMODB_TABLE", "SentAlerts")
DEFAULT_EXCHANGE = "NSE"

# === TRADINGVIEW & YAHOO MAPPING ===
TICKER_CORRECTIONS = {
    "LIC HOUSING FINANCE LTD": "LICHSGFIN",
    "INOX WIND LIMITED": "INOXWIND",
    "HINDUSTAN ZINC LIMITED": "HINDZINC",
    "HINDUSTAN UNILEVER LTD.": "HINDUNILVR",
    "TATA TECHNOLOGIES LIMITED": "TATATECH",
    "SYNGENE INTERNATIONAL LTD": "SYNGENE",
    "MARUTI SUZUKI INDIA LTD.": "MARUTI",
    "KEI INDUSTRIES LTD.": "KEI",
    "JINDAL STEEL LIMITED": "JINDALSTEL",
    "CENTRAL DEPO SER (I) LTD": "CDSL",
    "BAJAJ FINANCE LIMITED": "BAJFINANCE",
    "MAHINDRA & MAHINDRA LTD": "M&M", 
    "DABUR INDIA LTD": "DABUR",
    "TRENT LTD": "TRENT",
    "JIO FIN SERVICES LTD": "JIOFIN",
    "IIFL FINANCE LIMITED": "IIFL",
    "MUTHOOT FINANCE LIMITED": "MUTHOOTFIN",
    "BOSCH LIMITED": "BOSCHLTD",
    "HDFC LIFE INS CO LTD": "HDFCLIFE",
    "ASIAN PAINTS LIMITED": "ASIANPAINT",
    "DALMIA BHARAT LIMITED": "DALBHARAT",
    "BLUE STAR LIMITED": "BLUESTARCO",
    "HINDALCO INDUSTRIES LTD": "HINDALCO",
    "360 ONE WAM LIMITED": "360ONE",
    "HINDALCO INDUSTRIES LIMITED": "HINDALCO",
    "HINDALCO": "HINDALCO",
    "HINDALCO  INDUSTRIES  LTD": "HINDALCO",
    "PATANJALI FOODS LIMITED": "PATANJALI",
    "INDUSIND BANK LIMITED": "INDUSINDBK",
    "COAL INDIA LTD": "COALINDIA",
    "TATA MOTORS LIMITED": "TATAMOTORS",
    "INDIAN ENERGY EXC LTD": "IEX",
    "RELIANCE INDUSTRIES LTD": "RELIANCE",
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
    "SOLAR INDUSTRIES (I) LTD": "SOLARINDS",
    "TATA POWER CO LTD": "TATAPOWER",
    "SUPREME INDUSTRIES LTD": "SUPREMEIND",
    "ONE 97 COMMUNICATIONS LTD": "PAYTM",
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
    "KALYAN JEWELLERS IND LTD": "KALYANKJIL",
    "AXIS BANK LIMITED": "AXISBANK",
    "ZYDUS LIFESCIENCES LTD": "ZYDUSLIFE",
    "PI INDUSTRIES LTD": "PIIND",
    "HINDUSTAN PETROLEUM CORP": "HINDPETRO",
    "THE PHOENIX MILLS LTD": "PHOENIXLTD",
    "NBCC (INDIA) LIMITED": "NBCC",
    "BSE LIMITED": "BSE",
    "IDFC FIRST BANK LIMITED": "IDFCFIRSTB",
    "HSG & URBAN DEV CORPN LTD": "HUDCO",
    "ITC LTD": "ITC",
    "AUROBINDO PHARMA LTD": "AUROPHARMA",
    "KAYNES TECHNOLOGY IND LTD": "KAYNES",
    "TVS MOTOR COMPANY  LTD": "TVSMOTOR",
    "BHEL": "BHEL",
    "EICHER MOTORS LTD": "EICHERMOT",
    "TATA ELXSI LIMITED": "TATAELXSI",
    "NCC LIMITED": "NCC",
    "OBEROI REALTY LIMITED": "OBEROIRLTY",
    "HAVELLS INDIA LIMITED": "HAVELLS",
    "TATA CONSULTANCY SERV LT": "TCS",
    "CROMPT GREA CON ELEC LTD": "CROMPTON",
    "ALKEM LABORATORIES LTD.": "ALKEM",
    "ICICI BANK LTD.": "ICICIBANK",
    "DLF LIMITED": "DLF",
    "NESTLE INDIA LIMITED": "NESTLEIND",
    "AMBER ENTERPRISES (I) LTD": "AMBER",
    "DIXON TECHNO (INDIA) LTD": "DIXON",
    "COMPUTER AGE MNGT SER LTD": "CAMS",
    "BIOCON LIMITED.": "BIOCON",
    "PAGE INDUSTRIES LTD": "PAGEIND",
    "ADANI PORT & SEZ LTD": "ADANIPORTS",
    "SONA BLW PRECISION FRGS L": "SONACOMS",
    "CYIENT LIMITED": "CYIENT",
    "ADANI GREEN ENERGY LTD": "ADANIGREEN",
    "CONTAINER CORP OF IND LTD": "CONCOR",
    "COFORGE LIMITED": "COFORGE",
    "HDFC AMC LIMITED": "HDFCAMC",
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
    "INDIAN OIL CORP LTD": "IOC",
    "HFCL LIMITED": "HFCL",
    "KPIT TECHNOLOGIES LIMITED": "KPITTECH",
    "SAMMAAN CAPITAL LIMITED": "SAMMAANCAP",
    "PERSISTENT SYSTEMS LTD": "PERSISTENT",
    "CHOLAMANDALAM IN & FIN CO": "CHOLAFIN",
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
    "PB FINTECH LIMITED": "POLICYBZR",
    "POLYCAB INDIA LIMITED": "POLYCAB",
    "BHARAT ELECTRONICS LTD": "BEL",
    "BANK OF INDIA": "BANKINDIA",
    "BHARAT DYNAMICS LIMITED": "BDL",
    "POWER GRID CORP. LTD.": "POWERGRID",
    "GODREJ PROPERTIES LTD": "GODREJPROP",
    "GMR AIRPORTS LIMITED": "GMRAIRPORT",
    "GODREJ CONSUMER PRODUCTS": "GODREJCP",
    "INDIAN BANK": "INDIANB",
    "PIRAMAL PHARMA LIMITED": "PPLPHARMA",
    "OIL AND NATURAL GAS CORP.": "ONGC",
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
    "TATA CHEMICALS LTD": "TATACHEM",
    "PG ELECTROPLAST LIMITED": "PGEL",
    "BAJAJ HOLDINGS & INVS LTD": "BAJAJHLDNG",
    "WAAREE ENERGIES LIMITED": "WAAREEENER",
    "SWIGGY LIMITED": "SWIGGY",
    "FSN E COMMERCE VENTURES LTD": "NYKAA",
    "FSN E COMMERCE VENTURES LIMITED": "NYKAA",
    "FSN E-COMMERCE VENTURES LTD": "NYKAA"
}

# === STATIC SECTOR MAPPING ===
STATIC_SECTORS = {
    # FIN SERVICE
    "HDFCBANK": "FIN SERVICE", "ICICIBANK": "FIN SERVICE", "AXISBANK": "FIN SERVICE", "KOTAKBANK": "FIN SERVICE",
    "SBIN": "FIN SERVICE", "BAJFINANCE": "FIN SERVICE", "BAJAJFINSV": "FIN SERVICE", "LICHSGFIN": "FIN SERVICE",
    "PFC": "FIN SERVICE", "RECLTD": "FIN SERVICE", "SBILIFE": "FIN SERVICE", "HDFCLIFE": "FIN SERVICE",
    "ICICIPRULI": "FIN SERVICE", "CHOLAFIN": "FIN SERVICE", "MUTHOOTFIN": "FIN SERVICE", "LICI": "FIN SERVICE",
    "JIOFIN": "FIN SERVICE", "ABCAPITAL": "FIN SERVICE", "LTF": "FIN SERVICE", "BANDHANBNK": "FIN SERVICE",
    "BANKBARODA": "FIN SERVICE", "PNB": "FIN SERVICE", "CANBK": "FIN SERVICE", "INDUSINDBK": "FIN SERVICE",
    "IDFCFIRSTB": "FIN SERVICE", "AUBANK": "FIN SERVICE", "SHRIRAMFIN": "FIN SERVICE", "CDSL": "FIN SERVICE",
    "CAMS": "FIN SERVICE", "ANGELONE": "FIN SERVICE", "MCX": "FIN SERVICE", "BSE": "FIN SERVICE",
    "PAYTM": "FIN SERVICE", "POLICYBZR": "FIN SERVICE", "KFINTECH": "FIN SERVICE", "NUVAMA": "FIN SERVICE",
    "MFSL": "FIN SERVICE", "ICICIGI": "FIN SERVICE", "SBICARD": "FIN SERVICE", "MANAPPURAM": "FIN SERVICE",
    "IIFL": "FIN SERVICE", "RBLBANK": "FIN SERVICE", "YESBANK": "FIN SERVICE", "INDIANB": "FIN SERVICE",
    "BANKINDIA": "FIN SERVICE", "HUDCO": "FIN SERVICE", "IRFC": "FIN SERVICE", "SAMMAANCAP": "FIN SERVICE",

    # IT
    "TCS": "IT", "INFY": "IT", "HCLTECH": "IT", "WIPRO": "IT", "TECHM": "IT", "LTIM": "IT",
    "PERSISTENT": "IT", "COFORGE": "IT", "MPHASIS": "IT", "LTTS": "IT", "TATAELXSI": "IT",
    "OFSS": "IT", "KPITTECH": "IT", "CYIENT": "IT", "SONACOMS": "IT", "NAUKRI": "IT",

    # AUTO
    "TATAMOTORS": "AUTO", "MARUTI": "AUTO", "M&M": "AUTO", "BAJAJ-AUTO": "AUTO", "EICHERMOT": "AUTO",
    "HEROMOTOCO": "AUTO", "TVSMOTOR": "AUTO", "ASHOKLEY": "AUTO", "BHARATFORG": "AUTO", "BALKRISIND": "AUTO",
    "MRF": "AUTO", "APOLLOTYRE": "AUTO", "MOTHERSON": "AUTO", "BOSCHLTD": "AUTO", "UNOMINDA": "AUTO",
    "TIINDIA": "AUTO", "EXIDEIND": "AUTO",

    # METAL & MINING
    "TATASTEEL": "METAL", "HINDALCO": "METAL", "JSWSTEEL": "METAL", "VEDL": "METAL", "COALINDIA": "METAL",
    "HINDZINC": "METAL", "NMDC": "METAL", "SAIL": "METAL", "JINDALSTEL": "METAL", "NATIONALUM": "METAL",
    "APLAPOLLO": "METAL", "RATNAMANI": "METAL",

    # ENERGY & POWER
    "RELIANCE": "ENERGY", "NTPC": "ENERGY", "POWERGRID": "ENERGY", "ONGC": "ENERGY", "BPCL": "ENERGY",
    "IOC": "ENERGY", "TATAPOWER": "ENERGY", "ADANIGREEN": "ENERGY", "ADANIENT": "ENERGY", "ADANIPORTS": "ENERGY",
    "ADANIENSOL": "ENERGY", "JSWENERGY": "ENERGY", "NHPC": "ENERGY", "SJVN": "ENERGY", "TORNTPOWER": "ENERGY",
    "SUZLON": "ENERGY", "INOXWIND": "ENERGY", "IEX": "ENERGY", "IREDA": "ENERGY", "OIL": "ENERGY",
    "GAIL": "ENERGY", "PETRONET": "ENERGY", "IGL": "ENERGY", "MGL": "ENERGY", "SOLARINDS": "ENERGY",

    # PHARMA
    "SUNPHARMA": "PHARMA", "CIPLA": "PHARMA", "DRREDDY": "PHARMA", "DIVISLAB": "PHARMA", "APOLLOHOSP": "PHARMA",
    "MAXHEALTH": "PHARMA", "TORNTPHARM": "PHARMA", "MANKIND": "PHARMA", "LUPIN": "PHARMA", "ZYDUSLIFE": "PHARMA",
    "AUROPHARMA": "PHARMA", "ALKEM": "PHARMA", "BIOCON": "PHARMA", "SYNGENE": "PHARMA", "LAURUSLABS": "PHARMA",
    "GLENMARK": "PHARMA", "PPLPHARMA": "PHARMA", "FORTIS": "PHARMA",

    # FMCG & CONSUMER
    "ITC": "FMCG", "HINDUNILVR": "FMCG", "NESTLEIND": "FMCG", "BRITANNIA": "FMCG", "TATACONSUM": "FMCG",
    "TITAN": "FMCG", "ASIANPAINT": "FMCG", "BERGEPAINT": "FMCG", "PIDILITIND": "FMCG", "GODREJCP": "FMCG",
    "DABUR": "FMCG", "MARICO": "FMCG", "COLPAL": "FMCG", "VBL": "FMCG", "UNITDSPR": "FMCG", "UBL": "FMCG",
    "PATANJALI": "FMCG", "PAGEIND": "FMCG", "TRENT": "FMCG", "DMART": "FMCG", "NYKAA": "FMCG",
    "ZOMATO": "FMCG", "SWIGGY": "FMCG", "HAVELLS": "FMCG", "VOLTAS": "FMCG", "BLUESTARCO": "FMCG",
    "CROMPTON": "FMCG", "DIXON": "FMCG", "AMBER": "FMCG", "PGEL": "FMCG", "KALYANKJIL": "FMCG",

    # INFRA & REALTY
    "LT": "INFRA", "DLF": "REALTY", "LODHA": "REALTY", "GODREJPROP": "REALTY", "OBEROIRLTY": "REALTY",
    "PHOENIXLTD": "REALTY", "PRESTIGE": "REALTY", "NBCC": "INFRA", "NCC": "INFRA", "RVNL": "INFRA",
    "TITAGARH": "INFRA", "CONCOR": "INFRA", "IRCTC": "INFRA", "GMRINFRA": "INFRA", "GMRAIRPORT": "INFRA",
    "INDHOTEL": "INFRA", "ULTRACEMCO": "INFRA", "GRASIM": "INFRA", "AMBUJACEM": "INFRA", "SHREECEM": "INFRA",
    "DALBHARAT": "INFRA", "ACC": "INFRA", "ASTRAL": "INFRA", "POLYCAB": "INFRA", "SUPREMEIND": "INFRA",
    "KEI": "INFRA",

    # DEFENCE & OTHERS
    "HAL": "DEFENCE", "BEL": "DEFENCE", "MAZDOCK": "DEFENCE", "COCHINSHIP": "DEFENCE", "BDL": "DEFENCE",
    "BHEL": "INFRA", "ABB": "INFRA", "SIEMENS": "INFRA", "CGPOWER": "INFRA", "CUMMINSIND": "INFRA",
    "PIIND": "CHEM", "SRF": "CHEM", "TATACHEM": "CHEM", "UPL": "CHEM", "COROMANDEL": "CHEM",
    "INDUSTOWER": "TELECOM", "BHARTIARTL": "TELECOM", "IDEA": "TELECOM",
}

# --- 3. HELPER FUNCTIONS ---

def metric_card(title, value, subtitle=None, color="#e5e7eb", glow=False):
    return f"""
    <div style="
        padding:18px;
        border-radius:14px;
        background:linear-gradient(145deg,#0f1320,#0c101a);
        border:1px solid #222634;
        box-shadow:{'0 0 12px rgba(34,197,94,.35)' if glow else 'none'};
    ">
        <div style="color:#9ca3af;font-size:14px;margin-bottom:6px;">{title}</div>
        <div style="font-size:32px;font-weight:800;color:white;">{value}</div>
        <div style="color:{color};font-size:14px;">{subtitle or ""}</div>
    </div>
    """

def classify_strength(rvol):
    if rvol >= 2:
        return "🚀 Strong"
    if rvol >= 1:
        return "⚠ Moderate"
    return "💤 Weak"

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

    # Standardize Column Names
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

# --- 4. LIVE & HISTORICAL DATA ENGINE ---
def fetch_live_updates(df, target_date):
    if df.empty or 'Name' not in df.columns: 
        return df

    # 1. Clean Names & Prepare Tickers
    df['Cleaned_Name'] = df['Name'].replace(TICKER_CORRECTIONS)
    unique_tickers = df['Cleaned_Name'].unique().tolist()
    yahoo_tickers = [f"{t}.NS" for t in unique_tickers]
    
    # 2. Determine Date Mode
    india_tz = pytz.timezone('Asia/Kolkata')
    is_today = target_date == datetime.now(india_tz).date()
    
    try:
        if is_today:
            # LIVE MODE
            data = yf.download(tickers=yahoo_tickers, period="1d", interval="1m", progress=False)
            if not data.empty and 'Close' in data:
                final_prices = data['Close'].iloc[-1]
            else:
                return df
        else:
            # HISTORY MODE
            start_dt = target_date
            end_dt = target_date + timedelta(days=1)
            data = yf.download(tickers=yahoo_tickers, start=start_dt, end=end_dt, interval="1d", progress=False)
            
            if data.empty or 'Close' not in data:
                return df
            
            final_prices = data['Close'].iloc[-1]

        # 3. Map Prices
        def get_price(ticker, price_series):
            yf_ticker = f"{ticker}.NS"
            try:
                if isinstance(price_series, pd.Series):
                    if yf_ticker in price_series.index:
                        return float(price_series[yf_ticker])
                
                if len(unique_tickers) == 1:
                    if isinstance(price_series, pd.Series):
                        return float(price_series.iloc[0])
                    return float(price_series)
                    
                return 0.0
            except:
                return 0.0

        df['Live_Price'] = df['Cleaned_Name'].apply(lambda x: get_price(x, final_prices))

    except Exception as e:
        print(f"Error fetching data: {e}")
        df['Live_Price'] = df['Live_Price'] if 'Live_Price' in df.columns else 0.0
        
    return df

# --- 5. SECTOR FETCHING HELPER ---
@st.cache_data(show_spinner=False)
def get_sector_map(clean_ticker_list):
    sector_map = {}
    for ticker in clean_ticker_list:
        # 1. CHECK STATIC MAP FIRST (Instant)
        if ticker in STATIC_SECTORS:
            sector_map[ticker] = STATIC_SECTORS[ticker]
            continue
            
        # 2. FALLBACK TO YAHOO (Only if unknown)
        try:
            yf_ticker = f"{ticker}.NS"
            info = yf.Ticker(yf_ticker).info
            sector = info.get('sector', 'Others')
            
            if sector == 'Financial Services': sector = 'FIN SERVICE'
            if sector == 'Technology': sector = 'IT'
            if sector == 'Consumer Cyclical': sector = 'AUTO' 
            if sector == 'Basic Materials': sector = 'METAL'
            
            sector_map[ticker] = sector.upper()
        except:
            sector_map[ticker] = 'OTHERS'
    return sector_map

# --- MAIN APP UI ---

# Sidebar
with st.sidebar:
    selected = option_menu(
        menu_title="MENU",
        options=["SignalX", "Sector Scope"], 
        icons=["activity", "grid"],
        menu_icon="cast",
        default_index=0,
        styles={
            "container": {"padding": "5!important", "background-color": "#0b0e14"},
            "nav-link": {"font-size": "16px", "text-align": "left", "margin":"0px", "--hover-color": "#1e232e"},
            "nav-link-selected": {"background-color": "#00FF7F", "color": "black"},
        }
    )
    st.divider()
    india_tz = pytz.timezone('Asia/Kolkata')
    today_india = datetime.now(india_tz).date()
    selected_date = st.date_input("📅 Select Date", today_india)
    
    st.divider()

# 1. SignalX
if selected == "SignalX":
    # --- CUSTOM SVG LOGO ---
    st.markdown("""
<div style="text-align: left; margin-bottom: 25px; padding-left: 10px;">
<svg width="400" height="70" viewBox="0 0 400 70" fill="none" xmlns="http://www.w3.org/2000/svg">
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

    df = load_data_from_dynamodb(selected_date)
    
    if df.empty:
        st.info(f"No alerts found for {selected_date}")
        st.stop()

    # --- MAIN DATA FETCHING LOOP ---
    with st.spinner(f'⚡ Fetching Prices for {selected_date}...'):
        df = fetch_live_updates(df, selected_date)
        
        # Ensure columns are numeric
        df['SignalPrice'] = pd.to_numeric(df['SignalPrice'], errors='coerce')
        df['Live_Price'] = pd.to_numeric(df['Live_Price'], errors='coerce')
        
        # FORCE PnL Calculation
        df['Live_Move_Pct'] = ((df['Live_Price'] - df['SignalPrice']) / df['SignalPrice']) * 100
        df['Live_Move_Pct'] = df['Live_Move_Pct'].fillna(0.0)

    # --- Part 7: GLOBAL SEARCH ---
    search = st.text_input("🔎 Search ticker...", placeholder="Type symbol e.g. TATA")
    if search:
        df = df[df["Name"].str.contains(search, case=False)]

    # --- TRADINGVIEW LINK GENERATOR & STRENGTH ---
    if 'Name' in df.columns:
        df['Cleaned_Name'] = df['Name'].replace(TICKER_CORRECTIONS)
        df['TV_Symbol'] = DEFAULT_EXCHANGE + ":" + df['Cleaned_Name'].str.replace('&', '_').str.replace(' ', '')
        df['Chart'] = "https://www.tradingview.com/chart/?symbol=" + df['TV_Symbol']
        
        # Part 4: Signal Strength
        if 'RVOL' in df.columns:
             df["Strength"] = df["RVOL"].apply(classify_strength)
        else:
             df["Strength"] = "💤 Weak"

    # --- DATA SPLIT ---
    bull_count = len(df[df['Direction'] == 'LONG'])
    bear_count = len(df[df['Direction'] == 'SHORT'])

    # --- Part 1: PREMIUM KPI CARDS ---
    net_bias = bull_count - bear_count
    bias_label = "Bullish" if net_bias > 0 else "Bearish" if net_bias < 0 else "Neutral"
    bias_color = "#22c55e" if net_bias > 0 else "#ef4444" if net_bias < 0 else "#eab308"

    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(metric_card("Bullish Signals", bull_count, "Long Bias", "#22c55e", glow=True), unsafe_allow_html=True)
    c2.markdown(metric_card("Bearish Signals", bear_count, "Short Bias", "#ef4444"), unsafe_allow_html=True)
    c3.markdown(metric_card("Total Active", bull_count + bear_count, "All Signals"), unsafe_allow_html=True)
    c4.markdown(metric_card("Net Market Bias", bias_label, f"{net_bias:+}", bias_color), unsafe_allow_html=True)

    # --- Part 2: NET MARKET BIAS GAUGE ---
    st.write("") # Spacer
    bias_score = (bull_count - bear_count) / max(bull_count + bear_count, 1)

    gauge_df = pd.DataFrame({
        "x": ["Bias"],
        "score": [bias_score]
    })

    gauge = alt.Chart(gauge_df).mark_bar(
        cornerRadiusTopLeft=10,
        cornerRadiusTopRight=10
    ).encode(
        x=alt.X("x:N", axis=None),
        y=alt.Y("score:Q", scale=alt.Scale(domain=[-1,1], nice=False), axis=None), # axis=None for cleaner look
        color=alt.value("#22c55e" if bias_score > 0 else "#ef4444"),
        tooltip=[alt.Tooltip("score", format=".2f")]
    ).properties(
        height=30
    )

    st.caption("📊 Net Market Bias (−1 = Fully Short, +1 = Fully Long)")
    st.altair_chart(gauge, use_container_width=True)

    st.divider()

    col1, col2 = st.columns(2)

    # --- LEFT COL: BULLISH ---
    with col1:
        with st.container(border=True):
            st.markdown('<div class="header-bullish">🟢 Bullish Beacons</div>', unsafe_allow_html=True)
            bull_df = df[df['Direction'] == 'LONG'].copy()
            if not bull_df.empty:
                bull_df = bull_df.sort_values(by='Live_Move_Pct', ascending=False)
                st.data_editor(
                    bull_df[['Chart', 'Name', 'Live_Move_Pct', 'Strength', 'Time', 'RVOL']], 
                    column_config={
                        "Chart": st.column_config.LinkColumn("View", display_text="📈", width="small"),
                        "Name": st.column_config.TextColumn("Ticker", width="medium"),
                        "Live_Move_Pct": st.column_config.ProgressColumn(
                            "PnL %", 
                            format="%.2f%%", 
                            min_value=-5, 
                            max_value=5,
                            width="medium"
                        ),
                        "Strength": st.column_config.TextColumn("Strength", width="small"),
                        "Time": st.column_config.TextColumn("Time", width="small"), 
                        "RVOL": st.column_config.NumberColumn("RVOL", format="%.1fx", width="small"),
                    },
                    hide_index=True, use_container_width=True, disabled=True, key="bull_table"
                )
            else:
                st.caption("No Bullish Signals yet.")

    # --- RIGHT COL: BEARISH ---
    with col2:
        with st.container(border=True):
            st.markdown('<div class="header-bearish">🔴 Bearish Dragons</div>', unsafe_allow_html=True)
            bear_df = df[df['Direction'] == 'SHORT'].copy()
            if not bear_df.empty:
                bear_df = bear_df.sort_values(by='Live_Move_Pct', ascending=True)
                st.data_editor(
                    bear_df[['Chart', 'Name', 'Live_Move_Pct', 'Strength', 'Time', 'RVOL']], 
                    column_config={
                        "Chart": st.column_config.LinkColumn("View", display_text="📉", width="small"),
                        "Name": st.column_config.TextColumn("Ticker", width="medium"),
                        "Live_Move_Pct": st.column_config.ProgressColumn(
                            "PnL %", 
                            format="%.2f%%", 
                            min_value=-5, 
                            max_value=5,
                            width="medium"
                        ),
                        "Strength": st.column_config.TextColumn("Strength", width="small"),
                        "Time": st.column_config.TextColumn("Time", width="small"),
                        "RVOL": st.column_config.NumberColumn("RVOL", format="%.1fx", width="small"),
                    },
                    hide_index=True, use_container_width=True, disabled=True, key="bear_table"
                )
            else:
                st.caption("No Bearish Signals yet.")

# 2. Sector Scope
elif selected == "Sector Scope":
    st.title("🏙️ Sector Scope")
    st.caption("Active Sectors based on alerts")

    df = load_data_from_dynamodb(selected_date)
    if df.empty:
        st.info(f"No alerts found for {selected_date}")
        st.stop()

    if 'Name' in df.columns:
        # 1. Prepare Data
        df['Cleaned_Name'] = df['Name'].replace(TICKER_CORRECTIONS)
        unique_stocks = df['Cleaned_Name'].unique().tolist()
        
        # 2. Map Sectors (Using optimized static map)
        with st.spinner(f"Mapping sectors for {len(unique_stocks)} stocks..."):
            sector_mapping = get_sector_map(unique_stocks)
        
        df['Sector'] = df['Cleaned_Name'].map(sector_mapping)

        # 3. Calculate Sector Stats (Total Count & Bearish Count)
        sector_stats = df.groupby('Sector').size().reset_index(name='Total_Count')
        bearish_counts = df[df['Direction'] == 'SHORT'].groupby('Sector').size().reset_index(name='Bearish_Count')
        
        sector_stats = pd.merge(sector_stats, bearish_counts, on='Sector', how='left').fillna(0)
        sector_stats['Bearish_Pct'] = (sector_stats['Bearish_Count'] / sector_stats['Total_Count']) * 100
        
        # Bonus: Heat Score
        def heat(score):
            if score >= 70: return "🔥🔥🔥"
            if score >= 40: return "🔥🔥"
            if score >= 10: return "🔥"
            return "❄"

        sector_stats["Heat"] = sector_stats["Bearish_Pct"].apply(heat)

        # 4. Color Logic
        sector_stats['Color'] = sector_stats['Bearish_Pct'].apply(lambda x: '#FF4B4B' if x > 50 else '#00FF7F')

        # 5. Create the Chart
        chart = alt.Chart(sector_stats).mark_bar(
            cornerRadiusTopLeft=5,
            cornerRadiusTopRight=5
        ).encode(
            x=alt.X('Sector', sort='-y', axis=alt.Axis(labelAngle=-45, title=None)),
            y=alt.Y('Total_Count', axis=alt.Axis(title=None, tickMinStep=1)),
            color=alt.Color('Color', scale=None), 
            tooltip=[
                alt.Tooltip('Sector', title='Sector'),
                alt.Tooltip('Total_Count', title='Total Alerts'),
                alt.Tooltip('Bearish_Pct', title='Bearish %', format='.0f')
            ]
        ).configure_axis(
            grid=False, labelColor='#eee', domainColor='#333'
        ).configure_view(strokeWidth=0).properties(height=400)
        
        with st.container(border=True):
            st.altair_chart(chart, use_container_width=True)

        st.divider()
        st.subheader("Sector Details")
        
        # Updated DataFrame view with Heat
        st.dataframe(
            sector_stats[["Sector", "Total_Count", "Bearish_Pct", "Heat"]],
            use_container_width=True,
            hide_index=True
        )
