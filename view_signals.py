import streamlit as st
import pandas as pd
import boto3
from boto3.dynamodb.conditions import Key, Attr
import os
import json
from decimal import Decimal
from datetime import datetime, timedelta
import pytz
from streamlit_autorefresh import st_autorefresh
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
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

# --- SECTOR MAPPING ---
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

# --- 1. OPTIMIZED DATA LOADING ---
@st.cache_data(ttl=60)
def load_todays_history_optimized(target_date):
    """
    Fetches the entire day's history in one go via Query (Fast).
    """
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

def calculate_staircase_locally(history_ois):
    """
    FAIL-SAFE: Logic to check Staircase if backend returned '?'
    """
    if len(history_ois) < 2: return False
    
    # Growth Check (> 2%)
    if (history_ois[-1] - history_ois[0]) < 2.0: return False
    
    # Glitch Fix & Steps
    cleaned = []
    last_val = 0
    for v in history_ois:
        if v == 0 and abs(last_val) > 1.0: cleaned.append(last_val)
        else: 
            cleaned.append(v)
            last_val = v
            
    steps = np.diff(cleaned)
    if len(steps) == 0: return False
    
    # Spike Filter (Relaxed for simplicity in UI check, or strict)
    # Using strict rules:
    if max(steps) > 5.0 and len(steps) > 0:
        return False
        
    if min(steps) < -0.5: return False
    
    # Consistency
    good_steps = [s for s in steps if s > 0.2]
    return len(good_steps) >= 2

def process_radar_data(history_items):
    """
    Processes raw history blobs to calculate Metrics & Force-Check Scores.
    """
    if not history_items: return pd.DataFrame()
    
    # 1. Flatten all snapshots
    all_snapshots = []
    for record in history_items:
        time_str = record.get('SK')
        try:
            raw_data = record.get('Data')
            if isinstance(raw_data, str):
                if raw_data.startswith('"'): raw_data = raw_data[1:-1].replace('""', '"')
                data = json.loads(raw_data)
                if isinstance(data, str): data = json.loads(data)
                
                if isinstance(data, list):
                    for stock in data:
                        stock['SnapshotTime'] = time_str
                        stock['SignalPrice'] = float(stock.get('SignalPrice', 0))
                        stock['OI_Change'] = float(stock.get('OI_Change', 0))
                        all_snapshots.append(stock)
        except: continue
            
    df_full = pd.DataFrame(all_snapshots)
    if df_full.empty: return pd.DataFrame()
    
    # 2. Group by Stock
    stats = []
    for stock_name, group in df_full.groupby('Name'):
        group = group.sort_values('SnapshotTime')
        latest = group.iloc[-1]
        
        # --- RE-CALCULATE SCORE & STAIRCASE (FAIL-SAFE) ---
        # Get history of OI for this stock
        history_ois = group['OI_Change'].tolist()
        
        # Check Staircase Locally
        is_staircase = calculate_staircase_locally(history_ois)
        
        # Check Break
        break_type = str(latest.get('BreakType', 'INSIDE')).upper()
        is_break = ("PDH" in break_type) or ("PDL" in break_type)
        
        # Recalculate Score
        score = abs(latest['OI_Change'])
        if is_staircase: score += 15
        if is_break: score += 10
        if "PWH" in break_type or "PWL" in break_type: score += 10
        
        # 3. Entry Logic - Use the Recalculated Score for filtering
        if score > 20:
            # It's valid NOW. Find when it started being interesting.
            # Look for the first row where calculated criteria were met
            # Since calculating row-by-row is slow, we use a vectorized approximation
            # Assume entry when OI > 3 and Break occurred
            
            potential_entries = group[(group['OI_Change'].abs() > 3.0) & (group['BreakType'].astype(str).str.contains("BROKE", na=False))]
            
            if not potential_entries.empty:
                first_entry = potential_entries.iloc[0]
                entry_price = float(first_entry['SignalPrice'])
                entry_time = first_entry['SnapshotTime']
                
                # Calculate Moves
                is_bullish = latest['OI_Change'] > 0
                post_entry = group[group['SnapshotTime'] >= entry_time]
                
                if is_bullish:
                    max_price = post_entry['SignalPrice'].max()
                    max_move = ((max_price - entry_price) / entry_price) * 100
                    curr_move = ((latest['SignalPrice'] - entry_price) / entry_price) * 100
                else:
                    min_price = post_entry['SignalPrice'].min()
                    max_move = ((entry_price - min_price) / entry_price) * 100
                    curr_move = ((entry_price - latest['SignalPrice']) / entry_price) * 100
            else:
                entry_time, entry_price, max_move, curr_move = "-", 0, 0, 0
        else:
             entry_time, entry_price, max_move, curr_move = "-", 0, 0, 0

        stats.append({
            'Name': stock_name,
            'Latest Score': round(score, 1), # Use the Calculated Score
            'Staircase': "✅" if is_staircase else "❌",
            'Break': latest.get('BreakType', 'INSIDE'),
            'Entry Time': entry_time,
            'Entry Price': entry_price,
            'Current Price': latest['SignalPrice'],
            'Max Move %': max_move,
            'Current Move %': curr_move,
            'OI %': latest['OI_Change']
        })
        
    return pd.DataFrame(stats).sort_values('Latest Score', ascending=False)

@st.cache_data(ttl=60)
def load_data_from_dynamodb(target_date, signal_type=None):
    # LEGACY: For other tabs
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
    except: return pd.DataFrame()

    if not items: return pd.DataFrame()
    df = pd.DataFrame([convert_decimal(item) for item in items])
    if signal_type and 'Signal' in df.columns:
        df = df[df['Signal'] == signal_type].copy()
    
    numeric = ['SignalPrice', 'OI_Change']
    for c in numeric: 
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
    return df

@st.cache_data(ttl=60)
def load_nse_sector_data():
    try:
        dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
        table = dynamodb.Table(NSE_OI_TABLE) 
        response = table.get_item(Key={"PK": "NSE#OI", "SK": "LATEST"})
        if "Item" in response:
            return pd.DataFrame(json.loads(response["Item"]["data"]))
    except: pass
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
# PAGE 1: SMART RADAR
# =========================================================
def render_live_alerts(selected_date):
    st_autorefresh(interval=60 * 1000, key="datarefresh")
    st.markdown("""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap');
        .stApp { background-color: #080a0e; font-family: 'Inter', sans-serif; }
        [data-testid="stVerticalBlockBorderWrapper"] > div { background-color: #131722; border-radius: 12px; border: 1px solid #2a2e3a; box-shadow: 0 0 18px rgba(0,0,0,.35); }
        .table-header { color: #FFFFFF; background: linear-gradient(90deg, rgba(255,255,255,0.05) 0%, rgba(0,0,0,0) 100%); border-left: 4px solid #00FF7F; padding: 10px 15px; margin-bottom: 15px; font-size: 1.2rem; font-weight: 800; letter-spacing: 1px; text-transform: uppercase; border-radius: 4px; }
    </style>
    """, unsafe_allow_html=True)

    st.subheader("🚀 Smart Money Radar")
    
    # 1. Load History
    history_items = load_todays_history_optimized(selected_date)
    
    if not history_items:
        st.info("No data found for this date.")
        return

    # 2. Process
    radar_df = process_radar_data(history_items)
    if radar_df.empty:
        st.warning("No valid stocks found.")
        return

    # 3. Filter Top 20
    display_df = radar_df.head(20).copy()
    
    # 4. Links
    display_df['Cleaned_Name'] = display_df['Name'].replace(TICKER_CORRECTIONS)
    display_df['TV_Symbol'] = DEFAULT_EXCHANGE + ":" + display_df['Cleaned_Name'].str.replace('&', '_').str.replace(' ', '')
    display_df['Chart'] = "https://www.tradingview.com/chart/?symbol=" + display_df['TV_Symbol']

    # 5. Metrics
    latest_time_val = max([x.get('SK', '00:00') for x in history_items]) if history_items else "N/A"
    top_stock = display_df.iloc[0]['Name'] if not display_df.empty else "-"
    
    c1, c2, c3 = st.columns(3)
    c1.markdown(metric_card("Last Update", latest_time_val, "Time", "#eab308", glow=True), unsafe_allow_html=True)
    c2.markdown(metric_card("Top Pick", top_stock, "Highest Score", "#00FF7F"), unsafe_allow_html=True)
    
    st.divider()
    st.markdown('<div class="table-header">🏆 Top Opportunities</div>', unsafe_allow_html=True)
    
    st.data_editor(
        display_df[[
            'Chart', 'Name', 'Latest Score', 'Staircase', 'Break', 
            'Entry Time', 'Entry Price', 'Current Price', 'Max Move %', 'Current Move %', 'OI %'
        ]],
        column_config={
            "Chart": st.column_config.LinkColumn("View", display_text="📊", width="small"),
            "Name": st.column_config.TextColumn("Stock", width="medium"),
            "Latest Score": st.column_config.ProgressColumn("Smart Score", min_value=0, max_value=60, format="%.1f"),
            "Staircase": st.column_config.TextColumn("Struct", width="small"),
            "Break": st.column_config.TextColumn("Lvl Break", width="small"),
            "Entry Time": st.column_config.TextColumn("Signal", width="small"),
            "Entry Price": st.column_config.NumberColumn("Entry ₹", format="%.1f"),
            "Current Price": st.column_config.NumberColumn("CMP ₹", format="%.1f"),
            "Max Move %": st.column_config.NumberColumn("Max Run", format="%.2f%%"),
            "Current Move %": st.column_config.NumberColumn("Run Now", format="%.2f%%"),
            "OI %": st.column_config.NumberColumn("OI", format="%.1f%%")
        },
        hide_index=True,
        use_container_width=True,
        height=800
    )

# =========================================================
# PAGE 2: MARKET VELOCITY
# =========================================================
def render_intraday_boost(selected_date):
    st.header("📈 Market Velocity")
    df = load_data_from_dynamodb(selected_date, "INTRADAY_BOOST")
    if df.empty: return
    
    if 'Name' in df.columns:
        df['TV_Symbol'] = DEFAULT_EXCHANGE + ":" + df['Name'].replace(TICKER_CORRECTIONS).str.replace(' ', '')
        df['Chart'] = "https://www.tradingview.com/chart/?symbol=" + df['TV_Symbol']
        
    df = df.sort_values('OI_Change', ascending=False).head(20)
    
    st.data_editor(
        df[['Chart', 'Name', 'SignalPrice', 'OI_Change', 'BreakType']],
        column_config={
            "Chart": st.column_config.LinkColumn("View", display_text="📈", width="small"),
            "SignalPrice": st.column_config.NumberColumn("Price", format="%.2f"),
            "OI_Change": st.column_config.NumberColumn("OI %", format="%.2f")
        },
        hide_index=True, use_container_width=True
    )

# =========================================================
# PAGE 3: SECTOR VIEW
# =========================================================
def render_sector_view():
    st.header("📊 Sector Heatmap")
    df = load_nse_sector_data()
    if df.empty: return

    df['Sector'] = df['symbol'].map(SECTOR_MAP).fillna('Others')
    cols = ['pChangeInOpenInterest', 'lastPrice']
    for c in cols: 
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)

    stats = df.groupby('Sector')['pChangeInOpenInterest'].mean().reset_index().sort_values('pChangeInOpenInterest', ascending=False)
    st.bar_chart(stats.set_index('Sector')['pChangeInOpenInterest'])
    st.dataframe(stats, hide_index=True, use_container_width=True)

# =========================================================
# PAGE 4: DEEP DIVE
# =========================================================
def render_deep_dive_view(selected_date):
    st.header("🔬 Deep Dive")
    history_items = load_todays_history_optimized(selected_date)
    if not history_items: return
    
    names = set()
    for item in history_items:
        try:
            data = json.loads(item.get('Data', '[]'))
            if isinstance(data, str): data = json.loads(data)
            for s in data: names.add(s.get('Name'))
        except: pass
        
    selected = st.selectbox("Select Stock", sorted(list(names)))
    
    if selected:
        ts_data = []
        for item in history_items:
            time = item.get('SK')
            try:
                data = json.loads(item.get('Data', '[]'))
                if isinstance(data, str): data = json.loads(data)
                for s in data:
                    if s.get('Name') == selected:
                        ts_data.append({
                            'Time': time, 
                            'OI': float(s.get('OI_Change',0)), 
                            'Price': float(s.get('SignalPrice',0))
                        })
            except: pass
            
        if ts_data:
            df = pd.DataFrame(ts_data).sort_values('Time')
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(go.Scatter(x=df['Time'], y=df['OI'], mode='lines', name='OI', line=dict(shape='hv', color='#00FF7F')), secondary_y=False)
            fig.add_trace(go.Scatter(x=df['Time'], y=df['Price'], name='Price', line=dict(color='#FBBF24')), secondary_y=True)
            fig.update_layout(title=f"{selected} Structure", template="plotly_dark", height=500)
            st.plotly_chart(fig, use_container_width=True)

# =========================================================
# MAIN
# =========================================================
with st.sidebar:
    st.title("QUANT RADAR")
    page = st.radio("Navigate", ["🚀 Smart Radar", "📈 Market Velocity", "📊 Sector Heatmap", "🔬 Deep Dive"])
    india_tz = pytz.timezone('Asia/Kolkata')
    selected_date = st.date_input("📅 Select Date", datetime.now(india_tz).date())
    if st.button("🔄 Refresh"): st.cache_data.clear(); st.rerun()

if page == "🚀 Smart Radar":
    render_live_alerts(selected_date)
elif page == "📈 Market Velocity":
    render_intraday_boost(selected_date)
elif page == "📊 Sector Heatmap":
    render_sector_view()
elif page == "🔬 Deep Dive":
    render_deep_dive_view(selected_date)
