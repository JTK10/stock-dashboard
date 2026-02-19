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

# --- Global Elite Theme ---
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800;900&display=swap');

html, body, [class*="css"]  {
    font-family: 'Inter', sans-serif;
}

.stApp {
    background: radial-gradient(circle at 20% 10%, #0f172a 0%, #070a13 50%, #04060b 100%);
}

/* Card Layer */
[data-testid="stVerticalBlockBorderWrapper"] > div {
    background: linear-gradient(145deg, #111827, #0b1220);
    border-radius: 18px;
    border: 1px solid rgba(255,255,255,0.06);
    box-shadow:
        0 10px 30px rgba(0,0,0,.45),
        inset 0 0 0 1px rgba(255,255,255,0.02);
}

/* Header Styling */
h1, h2, h3 {
    font-weight: 900 !important;
    letter-spacing: 0.5px;
}

/* Section Divider */
hr {
    border: none;
    height: 1px;
    background: linear-gradient(90deg, transparent, rgba(255,255,255,.15), transparent);
}

/* Table Glow */
div[data-testid="stDataFrame"] {
    border-radius: 14px;
    overflow: hidden;
    border: 1px solid rgba(255,255,255,.05);
}

/* Metric Cards */
.stMetric {
    background: linear-gradient(145deg, #111827, #0b1220);
    border-radius: 16px;
    padding: 14px;
    border: 1px solid rgba(255,255,255,.06);
}

/* Sidebar */
section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0f172a, #0a0f1c);
}
</style>
""", unsafe_allow_html=True)


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

def process_radar_data(history_items, cumulative_scores_df):
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
        
        signal_gen_score = 0
        
        # 3. Entry Logic
        if latest_score > 20:
            potential_entries = group[(group['OI_Change'].abs() > 1.5) & (group['BreakType'].astype(str).str.contains("BROKE", na=False))]
            
            if not potential_entries.empty:
                first_entry = potential_entries.iloc[0]
                entry_price = float(first_entry['SignalPrice'])
                entry_time = first_entry['SnapshotTime']
                signal_gen_score = float(first_entry.get("Score", first_entry.get("Best_Score", 0)))
                
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
            'Signal_Generated_Score': round(signal_gen_score, 1),
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
    
    if not stats:
        return pd.DataFrame()

    radar_df = pd.DataFrame(stats)

    if not cumulative_scores_df.empty:
        radar_df = pd.merge(radar_df, cumulative_scores_df, on="Name", how="left")
        radar_df['Peak_Score'] = radar_df['Peak_Score'].fillna(0)
    else:
        radar_df['Peak_Score'] = 0

    radar_df['Peak_Score'] = radar_df[['Peak_Score', 'Latest Score']].max(axis=1)

    radar_df["SmartRank"] = (
        0.5 * radar_df['Peak_Score'] +
        0.3 * radar_df['Latest Score'] +
        0.2 * radar_df['Signal_Generated_Score']
    )
    
    return radar_df.sort_values('SmartRank', ascending=False)

@st.cache_data(ttl=60)
def load_data_from_dynamodb(target_date, signal_type=None):
    try:
        dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
        table = dynamodb.Table(DYNAMODB_TABLE)
        date_str = target_date.isoformat()

        # Step 1: Get instrument keys from history (FAST partition query)
        history_pk = f"HISTORY#BOOST#{date_str}"
        history_response = table.query(
            KeyConditionExpression=Key('PK').eq(history_pk)
        )
        history_items = history_response.get("Items", [])

        instrument_keys = set()

        for record in history_items:
            try:
                raw = json.loads(record.get("Data", "[]"))
                if isinstance(raw, str):
                    raw = json.loads(raw)
                for item in raw:
                    if "InstrumentKey" in item:
                        instrument_keys.add(item["InstrumentKey"])
            except:
                continue

        # Step 2: Direct get_item for each instrument (NO SCAN)
        items = []
        for key in instrument_keys:
            pk = f"INST#{key}#{date_str}"
            sk = "SIGNAL#INTRADAY_BOOST#LIVE"

            try:
                resp = table.get_item(Key={"PK": pk, "SK": sk})
                if "Item" in resp:
                    items.append(convert_decimal(resp["Item"]))
            except:
                continue

        if not items:
            return pd.DataFrame()

        df = pd.DataFrame(items)

        if signal_type and 'Signal' in df.columns:
            df = df[df['Signal'] == signal_type].copy()

        numeric = ['SignalPrice', 'OI_Change']
        for c in numeric:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)

        return df

    except Exception as e:
        st.error(f"DynamoDB optimized fetch error: {e}")
        return pd.DataFrame()

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
def load_cumulative_scores(target_date):
    """
    Fetches the cumulative best scores for all stocks for a given day.
    """
    try:
        dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
        table = dynamodb.Table(DYNAMODB_TABLE)
        pk = f"CUMULATIVE_SCORE#{target_date.isoformat()}"
        response = table.query(KeyConditionExpression=Key('PK').eq(pk))
        items = response.get("Items", [])
        
        while 'LastEvaluatedKey' in response:
            response = table.query(
                KeyConditionExpression=Key('PK').eq(pk),
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response.get('Items', []))

        if not items:
            return pd.DataFrame(columns=['Name', 'Peak_Score'])
        
        df = pd.DataFrame(convert_decimal(items))
        if df.empty:
            return pd.DataFrame(columns=['Name', 'Peak_Score'])
        
        df = df.rename(columns={'SK': 'Name', 'Best_Score': 'Peak_Score'})
        
        if 'Peak_Score' in df.columns:
            df['Peak_Score'] = pd.to_numeric(df['Peak_Score'], errors='coerce').fillna(0)
        
        return df[['Name', 'Peak_Score']]
        
    except Exception as e:
        st.error(f"Error loading cumulative scores: {e}")
        return pd.DataFrame(columns=['Name', 'Peak_Score'])

@st.cache_data(ttl=60)
def load_swing_candidates(selected_date):
    try:
        dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
        table = dynamodb.Table(DYNAMODB_TABLE)

        response = table.query(
            KeyConditionExpression=Key('PK').eq("SWING_ACTIVE")
        )
        return response.get("Items", [])
    except:
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
    
    st.subheader("🚀 Smart Money Radar")
    
    # 1. Load History
    history_items = load_todays_history_optimized(selected_date)
    cumulative_scores_df = load_cumulative_scores(selected_date)
    
    if not history_items:
        return

    # 2. Process
    radar_df = process_radar_data(history_items, cumulative_scores_df)
    locks = load_lock_data(selected_date)
    lock_map = {x["Stock"]: x for x in locks}

    radar_df["Locked"] = radar_df["Name"].apply(lambda x: "🔒" if x in lock_map else "")
    radar_df["Lock Time"] = radar_df["Name"].apply(lambda x: lock_map[x]["Lock_Time"] if x in lock_map else "-")
    radar_df["Reentry"] = radar_df["Name"].apply(lambda x: lock_map[x]["Reentry_Time"] if x in lock_map and lock_map[x].get("Reentry_Time") else "-")
    if radar_df.empty:
        return

    # 3. Filter Top 20
    display_df = radar_df.head(20).copy()
    
    # 4. Links
    display_df['Cleaned_Name'] = display_df['Name'].replace(TICKER_CORRECTIONS)
    display_df['TV_Symbol'] = DEFAULT_EXCHANGE + ":" + display_df['Cleaned_Name'].str.replace('&', '_').str.replace(' ', '')
    display_df['Chart'] = "https://www.tradingview.com/chart/?symbol=" + display_df['TV_Symbol']

    # 5. Smart Radar = Top 3 Spotlight
    st.markdown("### 🏆 Top 3 Opportunities")
    top3 = display_df.head(3)

    if len(top3) >= 3:
        cols = st.columns(3)
        for i in range(3):
            row = top3.iloc[i]
            cols[i].markdown(f"""
            <div style="
            background:linear-gradient(145deg,#0f172a,#0b1220);
            border:1px solid rgba(0,255,200,.4);
            border-radius:18px;
            padding:20px;
            text-align:center;
            box-shadow:0 0 25px rgba(0,255,200,.15);
            ">
                <h3 style="margin:0;color:white;">{row['Name']}</h3>
                <div style="font-size:32px;color:#00ffcc;font-weight:900;">
                    {row['SmartRank']:.1f}
                </div>
                <div style="color:#9ca3af;">Smart Rank</div>
            </div>
            """, unsafe_allow_html=True)
    st.divider()

    st.data_editor(
        display_df[[
            'Chart', 'Name', 'SmartRank', 'Latest Score', 'Peak_Score', 'Locked', 'Lock Time', 'Reentry', 'Break',
            'Entry Time', 'Entry Price', 'Current Price', 'Max Move %', 'Current Move %', 'OI %'
        ]],
        column_config={
            "Chart": st.column_config.LinkColumn("View", display_text="📊", width="small"),
            "Name": st.column_config.TextColumn("Stock", width="medium"),
            "SmartRank": st.column_config.ProgressColumn("Smart Rank", min_value=0, max_value=100, format="%.1f"),
            "Latest Score": st.column_config.NumberColumn("Latest", format="%.1f"),
            "Peak_Score": st.column_config.NumberColumn("Peak", format="%.1f"),
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

    df = load_data_from_dynamodb(selected_date, "INTRADAY_BOOST")
    if df.empty:
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

    # Market Bias Engine
    bull = len(df_bull)
    bear = len(df_bear)

    if (bull + bear) > 0:
        bias = "BULLISH" if bull > bear else "BEARISH"
        bias_color = "#00ffcc" if bull > bear else "#ff4d4d"

        st.markdown(f"""
        <div style="
        background:linear-gradient(145deg,#0f172a,#0b1220);
        border:1px solid {bias_color};
        border-radius:18px;
        padding:20px;
        text-align:center;
        box-shadow:0 0 30px {bias_color}20;
        margin-bottom: 20px;
        ">
        <h2 style="margin:0;color:{bias_color};">
        Market Bias: {bias}
        </h2>
        <p style="margin:0;color:#9ca3af;">
        Bull: {bull} | Bear: {bear}
        </p>
        </div>
        """, unsafe_allow_html=True)


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
        st.markdown("### 🧠 AI Verdicts Dashboard")
        
        # 1. LOAD REGISTRY (The Source of Truth)
        registry = load_daily_ai_registry(selected_date)
        
        # 2. SMART FETCH STRATEGY
        ai_df = load_data_from_dynamodb(selected_date)

        if ai_df.empty:
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
            return
            
        # 4. Load Locks & Sort
        locks = load_lock_data(selected_date)
        lock_map = {x.get("Stock", ""): x for x in locks}
        
        if 'Time' in ai_df.columns:
            ai_df = ai_df.sort_values(by='Time', ascending=False)
            
        # 5. Render Cards
        for _, row in ai_df.iterrows():
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
            
            # AI Card
            confidence = row.get("AI_Confidence", 82)
            live_move = row.get("Live_Move", 0.0)

            glow_color = "#00ffcc" if confidence > 75 else "#fbbf24"
            
            try:
                oi_change = float(row.get('OI_Change', 0))
            except (ValueError, TypeError):
                oi_change = 0


            st.markdown(f"""
            <div style="
            border:1px solid {glow_color};
            background:linear-gradient(145deg,#0f172a,#0b1220);
            border-radius:20px;
            padding:28px;
            margin-bottom:30px;
            box-shadow:0 0 40px {glow_color}20;
            ">

            <div style="display:flex;justify-content:space-between;align-items:center;">
                <div>
                    <h2 style="margin:0;color:white;font-size:26px;font-weight:900;">
                        {stock_name}
                    </h2>
                    <span style="color:#9ca3af;font-size:13px;">
                        Signal Time: {ai_time}
                    </span>
                </div>
                <div style="
                    background:{glow_color};
                    padding:8px 18px;
                    border-radius:10px;
                    font-weight:800;
                    color:#0a0f1c;
                ">
                    {decision.replace('_',' ')}
                </div>
            </div>

            <hr>

            <div style="
            background:rgba(255,255,255,0.04);
            padding:16px;
            border-radius:10px;
            margin-bottom:20px;
            color:#e5e7eb;
            font-style:italic;
            ">
            🧠 {row.get('AI_Reason')}
            </div>

            <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:20px;">
                <div><b style="color:#00ffcc;">TARGET</b><br>{target}</div>
                <div><b style="color:#ff4d4d;">STOP</b><br>{stoploss}</div>
                <div><b>R/R</b><br>{risk_reward}</div>
                <div>
                    <b>CONFIDENCE</b>
                    <div style="background:#1f2937;border-radius:6px;height:8px;margin-top:4px;">
                        <div style="width:{confidence}%;background:{glow_color};height:8px;border-radius:6px;"></div>
                    </div>
                    {confidence}%
                </div>
            </div>

            <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:16px;font-size:14px;">
                <div>Live Move: <span style="color:{'#00ffcc' if live_move>0 else '#ff4d4d'};">{live_move:.2f}%</span></div>
                <div>OI: {oi_change}%</div>
                <div>PCR: {pcr}</div>
                <div>Walls: {res}/{sup}</div>
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

def render_swing_analytics():

    st.header("📈 Swing Performance Analytics")

    try:
        dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
        sent_alerts_table = dynamodb.Table(DYNAMODB_TABLE)
        resp = sent_alerts_table.query(
            KeyConditionExpression=Key('PK').eq("SWING_HISTORY")
        )
        items = resp.get("Items", [])
    except Exception as e:
        st.error(f"Error loading swing analytics: {e}")
        items = []

    if not items:
        st.warning("No closed swing trades yet.")
        return

    df = pd.DataFrame(items)

    df["ReturnPct"] = pd.to_numeric(df["ReturnPct"])
    df["Holding_Days"] = pd.to_numeric(df["Holding_Days"])

    total_trades = len(df)
    win_rate = (df["ReturnPct"] > 0).mean() * 100
    avg_return = df["ReturnPct"].mean()
    avg_hold = df["Holding_Days"].mean()
    max_win = df["ReturnPct"].max()
    max_loss = df["ReturnPct"].min()

    c1,c2,c3,c4,c5 = st.columns(5)

    c1.metric("Total Trades", total_trades)
    c2.metric("Win Rate", f"{win_rate:.1f}%")
    c3.metric("Avg Return", f"{avg_return:.2f}%")
    c4.metric("Avg Hold Days", f"{avg_hold:.1f}")
    c5.metric("Best Trade", f"{max_win:.2f}%")

    st.divider()

    st.subheader("📊 Return Distribution")
    st.bar_chart(df["ReturnPct"])

    st.subheader("📈 Equity Curve")

    df = df.sort_values("Entry_Date")
    df["Equity"] = (1 + df["ReturnPct"]/100).cumprod()

    st.line_chart(df.set_index("Entry_Date")["Equity"])

    st.subheader("📋 Trade Log")

    st.dataframe(df[
        [
            "Symbol",
            "Direction",
            "Entry_Date",
            "Exit_Date",
            "ReturnPct",
            "Holding_Days",
            "Exit_Reason"
        ]
    ])

# =========================================================
# MAIN
# =========================================================
with st.sidebar:
    st.markdown("""
    <div style="
    background: linear-gradient(135deg,#00ffcc,#00b8ff);
    border-radius:18px;
    padding:2rem;
    text-align:center;
    box-shadow:0 15px 40px rgba(0,255,200,.25);
    ">
    <h1 style="margin:0;color:#0a0f1c;font-size:2.8rem;font-weight:900;">
    QUANT RADAR
    </h1>
    <p style="margin:0;color:#0a0f1c;font-weight:600;">
    AI-Powered Institutional Intelligence
    </p>
    </div>
    """, unsafe_allow_html=True)
    
    # UPDATED MENU
    page = st.radio(
        "Navigate",
        [
            "🚀 Smart Radar",
            "📊 Swing Trading",
            "📈 Swing Analytics",
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
elif page == "📈 Swing Analytics":
    render_swing_analytics()
elif page == "🧠 AI SIGNAL":
    render_ai_signals_view(selected_date)
elif page == "📈 Market Velocity":
    render_intraday_boost(selected_date)
elif page == "📊 Sector Heatmap":
    render_sector_view()
