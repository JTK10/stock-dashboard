import streamlit as st
import pandas as pd
import boto3
from boto3.dynamodb.conditions import Attr, Key
import os
import json
from decimal import Decimal
from datetime import datetime, timedelta
import pytz
from streamlit_autorefresh import st_autorefresh
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import gc 

# --- 0. MEMORY CLEANUP ---
gc.collect()

# --- 1. PAGE CONFIG ---
st.set_page_config(page_title="Market Radar", layout="wide", page_icon="📡")

# --- CONFIG ---
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1") 
DYNAMODB_TABLE = os.getenv("DYNAMODB_TABLE", "SentAlerts")
NSE_OI_TABLE = "NSE_OI_DATA" 
DEFAULT_EXCHANGE = "NSE"

# === OBFUSCATION MAPPING (THE "MASK") ===
# (Kept for compatibility, though we use direct values now)
SIGNAL_MASK = {
    "LONG_BUILDUP": "ACCUMULATION",
    "SHORT_BUILDUP": "DISTRIBUTION",
    "SHORT_COVERING": "RECOVERY",
    "LONG_UNWINDING": "LIQUIDATION",
    "NEUTRAL": "STABLE",
    "N/A": "-"
}

# === TRADINGVIEW MAPPING ===
TICKER_CORRECTIONS = {
    # ... (Your existing TICKER_CORRECTIONS dictionary - Keeping it as is) ...
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

# --- SECTOR MAPPING FOR NSE DATA ---
SECTOR_MAP = {
    "HDFCBANK": "Banking", "ICICIBANK": "Banking", "SBIN": "Banking", "AXISBANK": "Banking",
    "KOTAKBANK": "Banking", "INDUSINDBK": "Banking", "BANKBARODA": "Banking", "PNB": "Banking",
    "AUBANK": "Banking", "BANDHANBNK": "Banking", "FEDERALBNK": "Banking", "IDFCFIRSTB": "Banking",
    "RBLBANK": "Banking", "BAJFINANCE": "Finance", "BAJAJFINSV": "Finance", "CHOLAFIN": "Finance",
    "SHRIRAMFIN": "Finance", "MUTHOOTFIN": "Finance", "SBICARD": "Finance", "PEL": "Finance",
    "MANAPPURAM": "Finance", "L&TFH": "Finance", "M&MFIN": "Finance", "PFC": "Finance", "RECLTD": "Finance",
    "TCS": "IT", "INFY": "IT", "HCLTECH": "IT", "WIPRO": "IT", "TECHM": "IT", "LTIM": "IT",
    "LTTS": "IT", "PERSISTENT": "IT", "COFORGE": "IT", "MPHASIS": "IT", "TATAELXSI": "IT",
    "OFSS": "IT", "KPITTECH": "IT",
    "MARUTI": "Auto", "TATAMOTORS": "Auto", "M&M": "Auto", "BAJAJ-AUTO": "Auto", "EICHERMOT": "Auto",
    "HEROMOTOCO": "Auto", "TVSMOTOR": "Auto", "ASHOKLEY": "Auto", "BHARATFORG": "Auto",
    "BALKRISIND": "Auto", "MRF": "Auto", "APOLLOTYRE": "Auto", "MOTHERSON": "Auto", "BOSCHLTD": "Auto",
    "RELIANCE": "Oil & Gas", "ONGC": "Oil & Gas", "BPCL": "Oil & Gas", "IOC": "Oil & Gas", "HPCL": "Oil & Gas",
    "GAIL": "Oil & Gas", "PETRONET": "Oil & Gas", "NTPC": "Power", "POWERGRID": "Power", "TATAPOWER": "Power",
    "ADANIGREEN": "Power", "ADANIENSOL": "Power", "JSWENERGY": "Power", "NHPC": "Power",
    "ITC": "FMCG", "HINDUNILVR": "FMCG", "NESTLEIND": "FMCG", "BRITANNIA": "FMCG", "TATACONSUM": "FMCG",
    "DABUR": "FMCG", "GODREJCP": "FMCG", "MARICO": "FMCG", "COLPAL": "FMCG", "VBL": "FMCG",
    "ASIANPAINT": "Consumer", "BERGEPAINT": "Consumer", "PIDILITIND": "Consumer", "TITAN": "Consumer",
    "HAVELLS": "Consumer", "VOLTAS": "Consumer", "WHIRLPOOL": "Consumer", "PAGEIND": "Consumer", "TRENT": "Consumer",
    "SUNPHARMA": "Pharma", "CIPLA": "Pharma", "DRREDDY": "Pharma", "DIVISLAB": "Pharma", "TORNTPHARM": "Pharma",
    "LUPIN": "Pharma", "AUROPHARMA": "Pharma", "ALKEM": "Pharma", "BIOCON": "Pharma", "SYNGENE": "Pharma",
    "GLENMARK": "Pharma", "GRANULES": "Pharma", "LAURUSLABS": "Pharma", "APOLLOHOSP": "Healthcare", 
    "METROPOLIS": "Healthcare", "LALPATHLAB": "Healthcare",
    "TATASTEEL": "Metals", "JSWSTEEL": "Metals", "HINDALCO": "Metals", "VEDL": "Metals", "JINDALSTEL": "Metals",
    "SAIL": "Metals", "NMDC": "Metals", "NATIONALUM": "Metals", "COALINDIA": "Metals", "HINDZINC": "Metals",
    "DLF": "Realty", "GODREJPROP": "Realty", "OBEROIRLTY": "Realty", "PHOENIXLTD": "Realty", "PRESTIGE": "Realty",
    "LODHA": "Realty", "LT": "Infra", "HAL": "Defence", "BEL": "Defence", "MAZDOCK": "Defence", "COCHINSHIP": "Defence",
    "BDL": "Defence", "IRCTC": "Railways", "CONCOR": "Logistics", "INDIGO": "Aviation",
    "ADANIENT": "Diversified", "ADANIPORTS": "Infra"
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
        
        # Scan for INTRADAY_BOOST signals for the selected date
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

    # Standardize Column Names
    if 'Price' in df.columns and 'SignalPrice' not in df.columns:
        df.rename(columns={'Price': 'SignalPrice'}, inplace=True)
    
    # Ensure Score exists (for backward compatibility if old data is mixed)
    if 'Score' not in df.columns: df['Score'] = 0.0
    if 'Staircase' not in df.columns: df['Staircase'] = 'N/A'
    if 'OI_Change' not in df.columns: df['OI_Change'] = 0.0
    if 'BreakType' not in df.columns: df['BreakType'] = 'INSIDE'

    # Numeric Conversion
    df['Score'] = pd.to_numeric(df['Score'], errors='coerce').fillna(0)
    df['SignalPrice'] = pd.to_numeric(df['SignalPrice'], errors='coerce')
    df['OI_Change'] = pd.to_numeric(df['OI_Change'], errors='coerce').fillna(0)

    return df

@st.cache_data(ttl=60)
def load_history_data(target_date):
    """Fetches full timeline history for Deep Dive"""
    try:
        dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
        table = dynamodb.Table(DYNAMODB_TABLE)
        pk = f"HISTORY#BOOST#{target_date.isoformat()}"
        response = table.query(KeyConditionExpression=Key('PK').eq(pk))
        items = response.get('Items', [])
        while 'LastEvaluatedKey' in response:
            response = table.query(
                KeyConditionExpression=Key('PK').eq(pk),
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response.get('Items', []))
        return items
    except Exception as e:
        st.error(f"Error fetching history: {e}")
        return []

def get_stock_time_series(target_date, stock_name):
    items = load_history_data(target_date)
    data_points = []
    
    for item in items:
        time_str = item.get('SK')
        try:
            raw_data = item.get('Data')
            if isinstance(raw_data, str):
                snapshot = json.loads(raw_data)
                if isinstance(snapshot, str): snapshot = json.loads(snapshot)
                
                if isinstance(snapshot, list):
                    for stock in snapshot:
                        if stock.get('Name') == stock_name:
                            data_points.append({
                                'Time': time_str,
                                'OI_Change': float(stock.get('OI_Change', 0)),
                                'SignalPrice': float(stock.get('SignalPrice', 0)),
                                'NetMovePct': float(stock.get('NetMovePct', 0))
                            })
        except:
            continue
    
    if not data_points: return pd.DataFrame()
    df = pd.DataFrame(data_points)
    return df.sort_values('Time')

@st.cache_data(ttl=60)
def load_nse_sector_data():
    try:
        dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
        table = dynamodb.Table(NSE_OI_TABLE) 
        response = table.get_item(Key={"PK": "NSE#OI", "SK": "LATEST"})
        
        if "Item" in response and "data" in response["Item"]:
            json_str = response["Item"]["data"]
            data = json.loads(json_str)
            return pd.DataFrame(data)
    except Exception as e:
        st.error(f"Error loading NSE Data: {e}")
    return pd.DataFrame()

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
# PAGE 1: SMART MONEY RADAR (Top 15 Sorted by Score)
# =========================================================
def render_live_alerts(selected_date):
    # CSS & AUTO REFRESH
    count = st_autorefresh(interval=60 * 1000, key="datarefresh") # Refresh every 60s
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

    st.subheader("🚀 Smart Money Radar")
    st.caption("Top Opportunities sorted by Structure + Momentum + Low Risk Entry")
    
    # --- LOAD DATA ---
    df = load_data_from_dynamodb(selected_date, "INTRADAY_BOOST")
    
    if df.empty:
        st.info(f"No signals found for {selected_date}")
        return

    # --- GET LATEST SNAPSHOT ---
    if 'Time' in df.columns:
        latest_time = df['Time'].max()
        latest_df = df[df['Time'] == latest_time].copy()
    else:
        latest_df = df.copy() # Fallback if no Time column

    if latest_df.empty:
        st.warning("Data loaded but no recent snapshot found.")
        return

    # --- SORT BY SCORE ---
    # Convert Score to numeric just in case
    latest_df['Score'] = pd.to_numeric(latest_df['Score'], errors='coerce').fillna(0)
    
    # Filter: Show top 15 based on Score
    ranked_df = latest_df.sort_values(by='Score', ascending=False).head(15)

    # --- METRICS ---
    # Calculate some summary stats from the Top 15
    top_score_stock = ranked_df.iloc[0]['Name'] if not ranked_df.empty else "N/A"
    avg_score = ranked_df['Score'].mean() if not ranked_df.empty else 0
    staircase_count = len(ranked_df[ranked_df['Staircase'].astype(str).str.contains("✅")])
    
    c1, c2, c3 = st.columns(3) 
    c1.markdown(metric_card("Last Updated", latest_time, "Market Time", "#eab308", glow=True), unsafe_allow_html=True)
    c2.markdown(metric_card("Top Pick", top_score_stock, "Highest Conviction", "#60a5fa"), unsafe_allow_html=True)
    c3.markdown(metric_card("Strong Structures", f"{staircase_count}/15", "Staircase Confirmed", "#00FF7F"), unsafe_allow_html=True)

    st.divider()

    # --- PREPARE TABLE DATA ---
    # Add chart links
    if 'Name' in ranked_df.columns:
        ranked_df['Cleaned_Name'] = ranked_df['Name'].replace(TICKER_CORRECTIONS)
        ranked_df['TV_Symbol'] = DEFAULT_EXCHANGE + ":" + ranked_df['Cleaned_Name'].str.replace('&', '_').str.replace(' ', '')
        ranked_df['Chart'] = "https://www.tradingview.com/chart/?symbol=" + ranked_df['TV_Symbol']

    # --- DISPLAY TABLE ---
    with st.container(border=True):
        st.markdown(f'<div class="table-header">🏆 Top 15 Opportunities ({latest_time})</div>', unsafe_allow_html=True)
        
        st.data_editor(
            ranked_df[['Chart', 'Name', 'Score', 'Staircase', 'BreakType', 'OI_Change', 'SignalPrice']], 
            column_config={
                "Chart": st.column_config.LinkColumn("View", display_text="📊", width="small"),
                "Name": st.column_config.TextColumn("Stock", width="medium"),
                "Score": st.column_config.ProgressColumn("Smart Score", min_value=0, max_value=50, format="%.1f"),
                "Staircase": st.column_config.TextColumn("Structure", width="small"),
                "BreakType": st.column_config.TextColumn("Level Break", width="small"),
                "OI_Change": st.column_config.NumberColumn("OI Change %", format="%.2f%%"),
                "SignalPrice": st.column_config.NumberColumn("Price", format="%.2f")
            },
            hide_index=True, use_container_width=True, disabled=True, key="radar_table"
        )

# =========================================================
# PAGE 2: VELOCITY LEADERBOARD (Kept for broad view)
# =========================================================
def render_intraday_boost(selected_date):
    st.header("📈 Market Velocity (Broad View)")
    st.info("Broader list of market movers (Gainers/Losers).")

    df = load_data_from_dynamodb(selected_date, "INTRADAY_BOOST")
    if df.empty:
        st.warning("No data found.")
        return

    # Filter for latest time to show current state
    if 'Time' in df.columns:
        latest_time = df['Time'].max()
        df = df[df['Time'] == latest_time].copy()

    # Prepare Data
    if 'Name' in df.columns:
        df['Cleaned_Name'] = df['Name'].replace(TICKER_CORRECTIONS)
        df['TV_Symbol'] = DEFAULT_EXCHANGE + ":" + df['Cleaned_Name'].str.replace(' ', '')
        df['Chart'] = "https://www.tradingview.com/chart/?symbol=" + df['TV_Symbol']

    # Classification
    def classify_side(row):
        # Use new 'Side' column if available, else derive
        if 'Side' in row and pd.notnull(row['Side']): return str(row['Side']).upper()
        # Fallback
        r_type = str(row.get('RankType', '')).upper()
        if 'TOP GAINER' in r_type: return 'BULLISH'
        if 'TOP LOSER' in r_type: return 'BEARISH'
        return 'NEUTRAL'

    df['View_Side'] = df.apply(classify_side, axis=1)
    
    df_bull = df[df['View_Side'] == 'BULLISH'].sort_values('OI_Change', ascending=False).head(20)
    df_bear = df[df['View_Side'] == 'BEARISH'].sort_values('OI_Change', ascending=False).head(20)

    col1, col2 = st.columns(2)

    display_cols = ['Chart', 'Name', 'SignalPrice', 'BreakType', 'OI_Change']

    with col1:
        st.markdown("#### 🟢 Bullish Momentum")
        st.data_editor(
            df_bull[display_cols],
            column_config={
                "Chart": st.column_config.LinkColumn("View", display_text="📈", width="small"),
                "Name": st.column_config.TextColumn("Name", width="medium"),
                "SignalPrice": st.column_config.NumberColumn("Price", format="%.2f"),
                "BreakType": st.column_config.TextColumn("Status"),
                "OI_Change": st.column_config.NumberColumn("OI %", format="%.2f")
            },
            use_container_width=True, hide_index=True, disabled=True
        )

    with col2:
        st.markdown("#### 🔴 Bearish Momentum")
        st.data_editor(
            df_bear[display_cols],
            column_config={
                "Chart": st.column_config.LinkColumn("View", display_text="📉", width="small"),
                "Name": st.column_config.TextColumn("Name", width="medium"),
                "SignalPrice": st.column_config.NumberColumn("Price", format="%.2f"),
                "BreakType": st.column_config.TextColumn("Status"),
                "OI_Change": st.column_config.NumberColumn("OI %", format="%.2f")
            },
            use_container_width=True, hide_index=True, disabled=True
        )

# =========================================================
# PAGE 3: SECTOR VIEW
# =========================================================
def render_sector_view():
    st.header("📊 Sector Heatmap")
    
    df = load_nse_sector_data()
    if df.empty:
        st.warning("No Data found.")
        return

    df['Sector'] = df['symbol'].map(SECTOR_MAP).fillna('Others')
    cols_to_fix = ['pChangeInOpenInterest', 'lastPrice', 'changeinOpenInterest']
    for col in cols_to_fix:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    sector_stats = df.groupby('Sector')['pChangeInOpenInterest'].mean().reset_index()
    sector_stats = sector_stats.sort_values('pChangeInOpenInterest', ascending=False)
    
    sector_counts = df['Sector'].value_counts().reset_index()
    sector_counts.columns = ['Sector', 'Stock_Count']
    
    final_sector = pd.merge(sector_stats, sector_counts, on='Sector')
    final_sector = final_sector[final_sector['Sector'] != 'Others']

    st.subheader("🔥 Sector Activity")
    st.bar_chart(final_sector.set_index('Sector')['pChangeInOpenInterest'])

    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.markdown("### 🏆 Leading Sectors")
        st.dataframe(
            final_sector.style.format({"pChangeInOpenInterest": "{:.2f}"}),
            column_order=("Sector", "pChangeInOpenInterest", "Stock_Count"),
            column_config={
                "pChangeInOpenInterest": "Activity Score"
            },
            hide_index=True,
            use_container_width=True
        )

    with col2:
        st.markdown("### 🔍 Drilldown")
        selected_sector = st.selectbox("Select Sector", final_sector['Sector'].unique())
        
        subset = df[df['Sector'] == selected_sector].copy()
        subset = subset.sort_values('pChangeInOpenInterest', ascending=False)
        
        st.dataframe(
            subset[['symbol', 'pChangeInOpenInterest', 'openInterest', 'lastPrice']],
            column_config={
                "symbol": "Stock",
                "pChangeInOpenInterest": st.column_config.NumberColumn("Activity", format="%.2f"),
                "openInterest": st.column_config.NumberColumn("Volume Depth", format="%d"),
                "lastPrice": st.column_config.NumberColumn("Price", format="%.2f"),
            },
            hide_index=True,
            use_container_width=True
        )

# =========================================================
# PAGE 4: DEEP DIVE (STRUCTURAL ANALYSIS)
# =========================================================
def render_deep_dive_view(selected_date):
    st.header("🔬 Deep Dive: Structural Analysis")
    st.info("Visualizing the 'Staircase' accumulation patterns and Price vs. OI correlation.")
    
    # 1. Get List of Stocks Active Today (from Live Data)
    df_live = load_data_from_dynamodb(selected_date, "INTRADAY_BOOST")
    if df_live.empty:
        st.info("No data available for this date.")
        return
        
    if 'Name' not in df_live.columns: return
    
    # Create a list of stocks labeled with their OI Change for easier selection
    summary = df_live.groupby('Name')['OI_Change'].max().reset_index().sort_values('OI_Change', ascending=False)
    summary['Label'] = summary['Name'] + " (" + summary['OI_Change'].astype(str) + "%)"
    
    col_sel, col_blank = st.columns([1, 2])
    with col_sel:
        selected_label = st.selectbox("Select Stock to Analyze:", summary['Label'].tolist())
    
    if not selected_label: return
    
    selected_stock = summary.loc[summary['Label'] == selected_label, 'Name'].iloc[0]

    # 2. Fetch History (Time Series)
    history_df = get_stock_time_series(selected_date, selected_stock)
    
    if history_df.empty:
        st.error(f"Could not load detailed history for {selected_stock}.")
        st.caption("This might happen if the backend history snapshot (HISTORY#BOOST#...) is missing.")
        return

    # 3. CONSTRUCT THE "STAIRCASE" CHART
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Trace 1: Open Interest (The Stairs)
    fig.add_trace(
        go.Scatter(
            x=history_df['Time'], 
            y=history_df['OI_Change'], 
            mode='lines', 
            name='OI Structure',
            line=dict(shape='hv', color='#00FF7F', width=3), # <--- THIS CREATES THE STAIRS
            fill='tozeroy',
            fillcolor='rgba(0, 255, 127, 0.15)'
        ),
        secondary_y=False
    )

    # Trace 2: Price Action (The Yellow Line)
    fig.add_trace(
        go.Scatter(
            x=history_df['Time'], 
            y=history_df['SignalPrice'], 
            name='Price Action',
            line=dict(color='#FBBF24', width=2),
            mode='lines+markers'
        ),
        secondary_y=True
    )

    # Layout Styling
    fig.update_layout(
        title=f"<b>{selected_stock}</b>: Price vs. OI Structure",
        template="plotly_dark",
        paper_bgcolor="#0e1117",
        plot_bgcolor="#0e1117",
        hovermode="x unified",
        height=500,
        legend=dict(orientation="h", y=1.02, x=1, xanchor="right", yanchor="bottom")
    )
    
    # Axes Styling
    fig.update_yaxes(title_text="OI Change %", color="#00FF7F", secondary_y=False, showgrid=False)
    fig.update_yaxes(title_text="Price", color="#FBBF24", secondary_y=True, showgrid=True, gridcolor="#222")
    fig.update_xaxes(title_text="Time (Intraday)", showgrid=False)

    st.plotly_chart(fig, use_container_width=True)

    # 4. MOMENTUM BAR (Step Size)
    history_df['Step_Size'] = history_df['OI_Change'].diff().fillna(0)
    
    fig2 = go.Figure()
    fig2.add_trace(go.Bar(
        x=history_df['Time'],
        y=history_df['Step_Size'],
        marker_color=history_df['Step_Size'].apply(lambda x: '#22c55e' if x>=0 else '#ef4444'),
        name="Step Intensity"
    ))
    fig2.update_layout(
        title="Momentum Intensity (Size of Each Step)",
        template="plotly_dark",
        height=300,
        paper_bgcolor="#0e1117",
        plot_bgcolor="#0e1117"
    )
    st.plotly_chart(fig2, use_container_width=True)

# =========================================================
# MAIN NAVIGATION
# =========================================================
with st.sidebar:
    st.markdown("""
<div style="text-align: left; margin-bottom: 25px; padding-left: 10px;">
<svg width="250" height="60" viewBox="0 0 400 70" fill="none" xmlns="http://www.w3.org/2000/svg">
<defs>
<linearGradient id="bullGradient" x1="0%" y1="100%" x2="100%" y2="0%">
<stop offset="0%" style="stop-color:#00F260;stop-opacity:1" />
<stop offset="100%" style="stop-color:#0575E6;stop-opacity:1" />
</linearGradient>
<filter id="glow" x="-20%" y="-20%" width="140%" height="140%">
<feGaussianBlur stdDeviation="2" result="coloredBlur"/>
<feMerge>
<feMergeNode in="coloredBlur"/>
<feMergeNode in="SourceGraphic"/>
</feMerge>
</filter>
</defs>
<text x="10" y="52" fill="url(#bullGradient)" font-family="'Inter', sans-serif" font-weight="700" font-size="42" letter-spacing="-1">QUANT</text>
<text x="170" y="52" fill="#FFFFFF" font-family="'Inter', sans-serif" font-weight="700" font-size="42" letter-spacing="-1">RADAR</text>
</svg>
</div>
""", unsafe_allow_html=True)
    
    st.divider()
    page = st.radio("Navigate", ["🚀 Live Radar", "📈 Market Velocity", "📊 Sector Heatmap", "🔬 Deep Dive"])
    st.divider()
    india_tz = pytz.timezone('Asia/Kolkata')
    selected_date = st.date_input("📅 Select Date", datetime.now(india_tz).date())
    
    if st.button("🔄 Clear Cache (Fix Blanks)"):
        st.cache_data.clear()
        st.rerun()

if page == "🚀 Live Radar":
    render_live_alerts(selected_date)
elif page == "📈 Market Velocity":
    render_intraday_boost(selected_date)
elif page == "📊 Sector Heatmap":
    render_sector_view()
elif page == "🔬 Deep Dive":
    render_deep_dive_view(selected_date)
