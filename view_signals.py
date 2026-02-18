import streamlit as st
import pandas as pd
import boto3
from boto3.dynamodb.conditions import Key, Attr
import os
import json
from decimal import Decimal
from datetime import datetime
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

def calculate_staircase_locally(history_ois, break_type):
    """
    FAIL-SAFE: Logic to check Staircase if backend returned '?'
    Allows Spikes if 'BreakType' indicates a breakout.
    """
    if len(history_ois) < 2: return False
    
    # Growth Check (> 2%)
    if (history_ois[-1] - history_ois[0]) < 2.0: return False
    
    # Glitch Fix
    cleaned = []
    last_val = 0
    for v in history_ois:
        if v == 0 and abs(last_val) > 1.0: cleaned.append(last_val)
        else: 
            cleaned.append(v)
            last_val = v
            
    steps = np.diff(cleaned)
    if len(steps) == 0: return False
    
    # Spike Filter Logic
    is_break = ("PDH" in str(break_type)) or ("PDL" in str(break_type))
    
    # If Breaking: Allow huge steps. If Inside: Restrict steps.
    spike_limit = 50.0 if is_break else 5.0
    
    if max(steps) > spike_limit: return False
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
                        
                        # --- NEW: Extract AI Fields ---
                        stock['AI_Decision'] = stock.get('AI_Decision', 'N/A')
                        stock['AI_Reason'] = stock.get('AI_Reason', '')
                        stock['Option_PCR'] = stock.get('Option_PCR', '0')
                        stock['Option_MaxPain'] = stock.get('Option_MaxPain', '0')
                        
                        all_snapshots.append(stock)
        except: continue
            
    df_full = pd.DataFrame(all_snapshots)
    if df_full.empty: return pd.DataFrame()
    
    # 2. Group by Stock
    stats = []
    for stock_name, group in df_full.groupby('Name'):
        group = group.sort_values('SnapshotTime')
        latest = group.iloc[-1]
        latest_score = float(latest.get("Score", latest.get("Best_Score", 0)))
        break_type = str(latest.get('BreakType', 'INSIDE')).upper()
        
        # 3. Entry Logic
        if latest_score > 20:
            potential_entries = group[(group['OI_Change'].abs() > 1.5) & (group['BreakType'].astype(str).str.contains("BROKE", na=False))]
            
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

        # --- FIX: LOOK FOR LAST VALID AI VERDICT ---
        # Instead of just checking 'latest', we look for the last row where AI_Decision is NOT N/A
        valid_ai_rows = group[group['AI_Decision'].isin(['AI_SELECTED', 'FALLBACK_SELECTED'])]
        
        if not valid_ai_rows.empty:
            last_valid_ai = valid_ai_rows.iloc[-1]
            ai_decision = last_valid_ai['AI_Decision']
            ai_reason = last_valid_ai['AI_Reason']
            ai_time = last_valid_ai['SnapshotTime'] # Capture Time
        else:
            ai_decision = "N/A"
            ai_reason = "-"
            ai_time = "-"

        stats.append({
            'Name': stock_name,
            'Latest Score': round(latest_score, 1),
            'Break': latest.get('BreakType', 'INSIDE'),
            'Entry Time': entry_time,
            'Entry Price': entry_price,
            'Current Price': latest['SignalPrice'],
            'Max Move %': max_move,
            'Current Move %': curr_move,
            'OI %': latest['OI_Change'],
            # --- PERSISTENT AI Fields ---
            'AI_Decision': ai_decision,
            'AI_Reason': ai_reason,
            'AI_Time': ai_time,
            'Option_PCR': latest.get('Option_PCR', '-'),
            'Option_MaxPain': latest.get('Option_MaxPain', '-')
        })
        
    return pd.DataFrame(stats).sort_values('Latest Score', ascending=False)

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

@st.cache_data(ttl=60)
def load_lock_data(target_date):
    try:
        dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
        table = dynamodb.Table(DYNAMODB_TABLE)
        pk = f"DAILY_UNIQUE_LOCK#{target_date.isoformat()}"
        response = table.query(KeyConditionExpression=Key('PK').eq(pk))
        return response.get("Items", [])
    except:
        return []

@st.cache_data(ttl=60)
def load_daily_ai_registry(target_date):
    """
    Fetches the persistent daily AI selection list.
    This helps recover signals that may have been overwritten in the live table.
    """
    try:
        dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
        table = dynamodb.Table(DYNAMODB_TABLE)
        pk = f"AI_DAILY_ALERT#{target_date.isoformat()}"
        response = table.query(KeyConditionExpression=Key('PK').eq(pk))
        return {item['SK'] for item in response.get('Items', [])}
    except:
        return set()

@st.cache_data(ttl=60)
def load_swing_candidates(selected_date):
    try:
        dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
        table = dynamodb.Table(DYNAMODB_TABLE)

        pk = f"SWING_CANDIDATE#{selected_date.isoformat()}"
        response = table.query(KeyConditionExpression=Key('PK').eq(pk))
        items = response.get("Items", [])
        return items

    except Exception as e:
        st.error(f"Error loading swing data: {e}")
        return []

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
    locks = load_lock_data(selected_date)
    lock_map = {x["Stock"]: x for x in locks}

    radar_df["Locked"] = radar_df["Name"].apply(lambda x: "🔒" if x in lock_map else "")
    radar_df["Lock Time"] = radar_df["Name"].apply(lambda x: lock_map[x]["Lock_Time"] if x in lock_map else "-")
    radar_df["Reentry"] = radar_df["Name"].apply(lambda x: lock_map[x]["Reentry_Time"] if x in lock_map and lock_map[x].get("Reentry_Time") else "-")
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
    latest_time_val = max([x.get('SK', '00:00') or "00:00" for x in history_items]) if history_items else "N/A"
    top_stock = display_df.iloc[0]['Name'] if not display_df.empty else "-"
    
    c1, c2, c3 = st.columns(3)
    c1.markdown(metric_card("Last Update", latest_time_val, "Time", "#eab308", glow=True), unsafe_allow_html=True)
    c2.markdown(metric_card("Top Pick", top_stock, "Highest Score", "#00FF7F"), unsafe_allow_html=True)
    c3.markdown(metric_card("Locked Today", len(lock_map), "Unique Stocks", "#38bdf8"), unsafe_allow_html=True)

    
    st.divider()
    st.markdown('<div class="table-header">🏆 Top Opportunities</div>', unsafe_allow_html=True)
    
    st.data_editor(
        display_df[[
            'Chart', 'Name', 'Latest Score', 'Locked', 'Lock Time', 'Reentry', 'Break',
            'Entry Time', 'Entry Price', 'Current Price', 'Max Move %', 'Current Move %', 'OI %'
        ]],
        column_config={
            "Chart": st.column_config.LinkColumn("View", display_text="📊", width="small"),
            "Name": st.column_config.TextColumn("Stock", width="medium"),
            "Latest Score": st.column_config.ProgressColumn("Smart Score", min_value=0, max_value=60, format="%.1f"),
            "Locked": st.column_config.TextColumn("Locked", width="small"),
            "Lock Time": st.column_config.TextColumn("Lock Time", width="small"),
            "Reentry": st.column_config.TextColumn("Reentry", width="small"),
            "Break": st.column_config.TextColumn("Lvl Break", width="small"),
            "Entry Time": st.column_config.TextColumn("1st Signal", width="small"),
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
# PAGE 2: MARKET VELOCITY (RESTORED)
# =========================================================
def render_intraday_boost(selected_date):
    st.header("📈 Market Velocity")
    st.info("Real-time momentum (Original View).")

    df = load_data_from_dynamodb(selected_date, "INTRADAY_BOOST")
    if df.empty:
        st.warning("No data found.")
        return

    # Filter for latest time
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
    if df.empty: return

    df['Sector'] = df['symbol'].map(SECTOR_MAP).fillna('Others')
    cols = ['pChangeInOpenInterest', 'lastPrice']
    for c in cols: 
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)

    stats = df.groupby('Sector')['pChangeInOpenInterest'].mean().reset_index().sort_values('pChangeInOpenInterest', ascending=False)
    st.bar_chart(stats.set_index('Sector')['pChangeInOpenInterest'])
    st.dataframe(stats, hide_index=True, use_container_width=True)

# =========================================================
# PAGE 4: AI SIGNAL DASHBOARD (UPDATED)
# =========================================================
def render_ai_signals_view(selected_date):
    import traceback
    try:
        st_autorefresh(interval=60 * 1000, key="datarefresh_ai")
        st.header("🧠 AI Verdicts")
        st.info("Live AI analysis with Option Chain & Trading Plan")
        
        # 1. LOAD REGISTRY (The Source of Truth)
        registry = load_daily_ai_registry(selected_date)
        
        # 2. SMART FETCH STRATEGY
        # Attempt 1: Standard Scan
        ai_df = load_data_from_dynamodb(selected_date)
        
        # Attempt 2: Direct Fetch (Fallback if Scan fails but Registry exists)
        if (ai_df.empty or 'AI_Decision' not in ai_df.columns) and registry:
            # We need Instrument Keys to fetch specific items. 
            # We can get them from the History which we know works.
            history = load_todays_history_optimized(selected_date)
            name_to_key = {}
            
            # extract mapping from history
            for record in history:
                try:
                    raw = json.loads(record.get('Data', '[]'))
                    if isinstance(raw, str): raw = json.loads(raw)
                    for item in raw:
                        if 'Name' in item and 'InstrumentKey' in item:
                            name_to_key[item['Name']] = item['InstrumentKey']
                except: continue
            
            # Now fetch specific items for registered stocks
            dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
            table = dynamodb.Table(DYNAMODB_TABLE)
            direct_items = []
            
            for stock_name in registry:
                if stock_name in name_to_key:
                    key = name_to_key[stock_name]
                    pk = f"INST#{key}#{selected_date.isoformat()}"
                    sk = "SIGNAL#INTRADAY_BOOST#LIVE"
                    try:
                        resp = table.get_item(Key={"PK": pk, "SK": sk})
                        if 'Item' in resp:
                            direct_items.append(convert_decimal(resp['Item']))
                    except: pass
            
            if direct_items:
                ai_df = pd.DataFrame(direct_items)

        if ai_df.empty:
            if registry:
                st.warning(f"⚠️ AI has selected stocks ({', '.join(registry)}), but live details are syncing. Please wait.")
            else:
                st.info("⏳ Waiting for AI Signals... (No active Verdicts yet)")
            return
            
        # 3. Filter Logic
        # Filter 1: Must be the Live Signal SK
        if 'SK' in ai_df.columns:
            ai_df = ai_df[ai_df['SK'] == 'SIGNAL#INTRADAY_BOOST#LIVE'].copy()
            
        # Filter 2: Must be in Registry OR Explicitly Selected
        # This handles cases where Scan returns many items but we only want the AI ones
        if 'Name' in ai_df.columns:
            is_in_registry = ai_df['Name'].isin(registry)
            is_selected = False
            if 'AI_Decision' in ai_df.columns:
                is_selected = ai_df['AI_Decision'].isin(['AI_SELECTED', 'FALLBACK_SELECTED'])
            
            ai_df = ai_df[is_in_registry | is_selected].copy()
        
        if ai_df.empty:
            st.info("⏳ Waiting for AI Signals... (No matching data found)")
            return
            
        # 4. Load Locks & Sort
        locks = load_lock_data(selected_date)
        lock_map = {x.get("Stock", ""): x for x in locks}
        
        if 'Time' in ai_df.columns:
            ai_df = ai_df.sort_values(by='Time', ascending=False)
            
        # 5. Render Cards
        for _, row in ai_df.iterrows():
            # ... (Rest of rendering logic remains mostly same, safe access) ...
            decision = str(row.get('AI_Decision', 'N/A'))
            stock_name = str(row.get('Name', row.get('InstrumentKey', 'Unknown')))
            
            # Recovery Label
            if decision == 'N/A' and stock_name in registry:
                decision = "AI_SELECTED (Recall)"
            
            ai_time = str(row.get('Signal_Generated_At', row.get('Time', '-')))
            if pd.isna(ai_time) or ai_time.strip() in ["", "nan"]: ai_time = str(row.get('Time', '-'))
                
            # Extract New Fields
            target = row.get('Target', 'N/A')
            stoploss = row.get('StopLoss', 'N/A')
            risk_reward = row.get('RiskReward', 'N/A')
            
            if target == 'N/A' and "Recall" in decision:
                target = "See Telegram" # Fallback if data was overwritten
            
            # Options
            pcr = row.get('Option_PCR', '-')
            max_pain = row.get('Option_MaxPain', '-')
            res = row.get('Option_Res', '-')
            sup = row.get('Option_Sup', '-')
            
            # Colors
            if "AI_SELECTED" in decision:
               color, bg_color = "#00FF7F", "rgba(0,255,127,0.1)"
            elif "FALLBACK_SELECTED" in decision:
                 color, bg_color = "#FBBF24", "rgba(251,191,36,0.1)"
            else:
                color, bg_color = "#9ca3af", "rgba(156,163,175,0.1)"

            st.markdown(f"""
            <div style="padding: 20px; border-radius: 12px; border: 1px solid {color}; background-color: {bg_color}; margin-bottom: 15px;">
                <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
                    <h3 style="margin:0; color:white;">{stock_name}</h3>
                    <div style="display:flex; align-items:center; gap:10px;">
                        <span style="color:#9ca3af; font-size:12px; font-family:monospace;">🕒 {ai_time}</span>
                        <span style="background:{color}; color:black; padding:4px 12px; border-radius:4px; font-weight:800;">{decision}</span>
                    </div>
                </div>
                <div style="color: #e5e7eb; font-size: 16px; margin-bottom: 15px;">
                    <i>" {row.get('AI_Reason', '-')} "</i>
                </div>
                <div style="margin-bottom: 15px; padding: 10px; background: rgba(0,0,0,0.3); border-radius: 8px;">
                    <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; text-align: center;">
                        <div>
                            <div style="color:#9ca3af; font-size:11px; letter-spacing:1px;">TARGET</div>
                            <div style="color:#00FF7F; font-weight:bold; font-size:16px;">{target}</div>
                        </div>
                        <div style="border-left: 1px solid rgba(255,255,255,0.1); border-right: 1px solid rgba(255,255,255,0.1);">
                            <div style="color:#9ca3af; font-size:11px; letter-spacing:1px;">STOP LOSS</div>
                            <div style="color:#EF4444; font-weight:bold; font-size:16px;">{stoploss}</div>
                        </div>
                        <div>
                            <div style="color:#9ca3af; font-size:11px; letter-spacing:1px;">RISK/REWARD</div>
                            <div style="color:white; font-weight:bold; font-size:16px;">{risk_reward}</div>
                        </div>
                    </div>
                </div>
                <div style="display:flex; justify-content: space-between; font-size: 13px; color: #9ca3af; flex-wrap: wrap; gap: 10px;">
                    <div style="display:flex; gap: 15px;">
                        <div>Price: <span style="color:white;">{row.get('SignalPrice', '-')}</span></div>
                        <div>OI Chg: <span style="color:white;">{row.get('OI_Change', '-')}%</span></div>
                    </div>
                    <div style="display:flex; gap: 15px;">
                        <div>PCR: <span style="color:#38bdf8;">{pcr}</span></div>
                        <div>Max Pain: <span style="color:#38bdf8;">{max_pain}</span></div>
                        <div>Walls: <span style="color:#38bdf8;">{res} / {sup}</span></div>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)

    except Exception as e:
        st.error("🚨 CRITICAL ERROR: The AI Signal page crashed.")
        st.code(traceback.format_exc(), language="python")

def render_swing_dashboard(selected_date):

    st.header("📊 Swing Trading Engine")
    st.info("Daily Hybrid Adaptive Swing Model")

    swing_items = load_swing_candidates(selected_date)

    if not swing_items:
        st.warning("No swing candidate stored for this date.")
        return

    df = pd.DataFrame(swing_items)
    if "Symbol" not in df.columns:
        df["Symbol"] = df["Name"] if "Name" in df.columns else "-"
    if "Direction" not in df.columns:
        df["Direction"] = "-"
    if "Confidence" not in df.columns:
        df["Confidence"] = 0
    if "Setup" not in df.columns:
        df["Setup"] = "-"
    if "Status" not in df.columns:
        df["Status"] = "-"
    if "Close" not in df.columns:
        df["Close"] = np.nan

    if "Triggered_At" in df.columns:
        df["Entry Trigger Time"] = df["Triggered_At"]
    elif "Entry_Trigger_Time" in df.columns:
        df["Entry Trigger Time"] = df["Entry_Trigger_Time"]
    elif "Trigger_Time" in df.columns:
        df["Entry Trigger Time"] = df["Trigger_Time"]
    else:
        df["Entry Trigger Time"] = "-"

    df["Entry Trigger Time"] = df["Entry Trigger Time"].fillna("-")
    df["Confidence"] = pd.to_numeric(df["Confidence"], errors="coerce").fillna(0).clip(0, 100)
    df["Close"] = pd.to_numeric(df["Close"], errors="coerce")

    # TradingView link
    df["Cleaned_Name"] = df["Symbol"].replace(TICKER_CORRECTIONS)
    df["TV_Symbol"] = DEFAULT_EXCHANGE + ":" + df["Cleaned_Name"].str.replace(" ", "")
    df["Chart"] = "https://www.tradingview.com/chart/?symbol=" + df["TV_Symbol"]

    st.data_editor(
        df[[
            "Chart",
            "Symbol",
            "Direction",
            "Confidence",
            "Setup",
            "Status",
            "Entry Trigger Time",
            "Close"
        ]],
        column_config={
            "Chart": st.column_config.LinkColumn("Chart", display_text="📊", width="small"),
            "Symbol": st.column_config.TextColumn("Stock"),
            "Direction": st.column_config.TextColumn("Side"),
            "Confidence": st.column_config.ProgressColumn("Confidence", min_value=0, max_value=100),
            "Setup": st.column_config.TextColumn("Setup"),
            "Status": st.column_config.TextColumn("Status"),
            "Entry Trigger Time": st.column_config.TextColumn("Entry Trigger"),
            "Close": st.column_config.NumberColumn("Close Price", format="%.2f")
        },
        use_container_width=True,
        hide_index=True
    )

# =========================================================
# MAIN
# =========================================================
with st.sidebar:
    st.title("QUANT RADAR")
    # UPDATED MENU
    page = st.radio(
        "Navigate",
        [
            "🚀 Smart Radar",
            "📊 Swing Trading",
            "🧠 AI SIGNAL",
            "📈 Market Velocity",
            "📊 Sector Heatmap"
        ]
    )
    
    india_tz = pytz.timezone('Asia/Kolkata')
    selected_date = st.date_input("📅 Select Date", datetime.now(india_tz).date())
    if st.button("🔄 Refresh"): st.cache_data.clear(); st.rerun()

if page == "🚀 Smart Radar":
    render_live_alerts(selected_date)
elif page == "📊 Swing Trading":
    render_swing_dashboard(selected_date)
elif page == "🧠 AI SIGNAL":
    render_ai_signals_view(selected_date)
elif page == "📈 Market Velocity":
    render_intraday_boost(selected_date)
elif page == "📊 Sector Heatmap":
    render_sector_view()

