import streamlit as st
import pandas as pd
import boto3
from boto3.dynamodb.conditions import Attr
import os
import json
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
NSE_OI_TABLE = "NSE_OI_DATA" # Table name for Sector Data
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

# --- SECTOR MAPPING FOR NSE DATA ---
SECTOR_MAP = {
    # BANKING & FINANCE
    "HDFCBANK": "Banking", "ICICIBANK": "Banking", "SBIN": "Banking", "AXISBANK": "Banking",
    "KOTAKBANK": "Banking", "INDUSINDBK": "Banking", "BANKBARODA": "Banking", "PNB": "Banking",
    "AUBANK": "Banking", "BANDHANBNK": "Banking", "FEDERALBNK": "Banking", "IDFCFIRSTB": "Banking",
    "RBLBANK": "Banking", "BAJFINANCE": "Finance", "BAJAJFINSV": "Finance", "CHOLAFIN": "Finance",
    "SHRIRAMFIN": "Finance", "MUTHOOTFIN": "Finance", "SBICARD": "Finance", "PEL": "Finance",
    "MANAPPURAM": "Finance", "L&TFH": "Finance", "M&MFIN": "Finance", "PFC": "Finance", "RECLTD": "Finance",
    # IT & TECH
    "TCS": "IT", "INFY": "IT", "HCLTECH": "IT", "WIPRO": "IT", "TECHM": "IT", "LTIM": "IT",
    "LTTS": "IT", "PERSISTENT": "IT", "COFORGE": "IT", "MPHASIS": "IT", "TATAELXSI": "IT",
    "OFSS": "IT", "KPITTECH": "IT",
    # AUTO & ANCILLARY
    "MARUTI": "Auto", "TATAMOTORS": "Auto", "M&M": "Auto", "BAJAJ-AUTO": "Auto", "EICHERMOT": "Auto",
    "HEROMOTOCO": "Auto", "TVSMOTOR": "Auto", "ASHOKLEY": "Auto", "BHARATFORG": "Auto",
    "BALKRISIND": "Auto", "MRF": "Auto", "APOLLOTYRE": "Auto", "MOTHERSON": "Auto", "BOSCHLTD": "Auto",
    # ENERGY & POWER
    "RELIANCE": "Oil & Gas", "ONGC": "Oil & Gas", "BPCL": "Oil & Gas", "IOC": "Oil & Gas", "HPCL": "Oil & Gas",
    "GAIL": "Oil & Gas", "PETRONET": "Oil & Gas", "NTPC": "Power", "POWERGRID": "Power", "TATAPOWER": "Power",
    "ADANIGREEN": "Power", "ADANIENSOL": "Power", "JSWENERGY": "Power", "NHPC": "Power",
    # CONSUMER & FMCG
    "ITC": "FMCG", "HINDUNILVR": "FMCG", "NESTLEIND": "FMCG", "BRITANNIA": "FMCG", "TATACONSUM": "FMCG",
    "DABUR": "FMCG", "GODREJCP": "FMCG", "MARICO": "FMCG", "COLPAL": "FMCG", "VBL": "FMCG",
    "ASIANPAINT": "Consumer", "BERGEPAINT": "Consumer", "PIDILITIND": "Consumer", "TITAN": "Consumer",
    "HAVELLS": "Consumer", "VOLTAS": "Consumer", "WHIRLPOOL": "Consumer", "PAGEIND": "Consumer", "TRENT": "Consumer",
    # PHARMA & HEALTHCARE
    "SUNPHARMA": "Pharma", "CIPLA": "Pharma", "DRREDDY": "Pharma", "DIVISLAB": "Pharma", "TORNTPHARM": "Pharma",
    "LUPIN": "Pharma", "AUROPHARMA": "Pharma", "ALKEM": "Pharma", "BIOCON": "Pharma", "SYNGENE": "Pharma",
    "GLENMARK": "Pharma", "GRANULES": "Pharma", "LAURUSLABS": "Pharma", "APOLLOHOSP": "Healthcare", 
    "METROPOLIS": "Healthcare", "LALPATHLAB": "Healthcare",
    # METALS & MINING
    "TATASTEEL": "Metals", "JSWSTEEL": "Metals", "HINDALCO": "Metals", "VEDL": "Metals", "JINDALSTEL": "Metals",
    "SAIL": "Metals", "NMDC": "Metals", "NATIONALUM": "Metals", "COALINDIA": "Metals", "HINDZINC": "Metals",
    # REALTY & INFRA
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

    # Standardize
    if 'Price' in df.columns and 'SignalPrice' not in df.columns:
        df.rename(columns={'Price': 'SignalPrice'}, inplace=True)
    
    # --- DIRECTION LOGIC ---
    if 'Side' in df.columns:
        df['Direction'] = df['Side'].astype(str).map({'Bullish': 'LONG', 'Bearish': 'SHORT', 'LONG': 'LONG', 'SHORT': 'SHORT'})
    
    if 'Direction' not in df.columns or df['Direction'].isnull().all():
        if 'Signal' in df.columns:
             df['Direction'] = df['Signal'].map({'LONG': 'LONG', 'SHORT': 'SHORT'})

    # --- OI FIELDS SAFE LOAD (UPDATED FOR NEW LAMBDA) ---
    if 'OI_Signal' not in df.columns: df['OI_Signal'] = "N/A"
    if 'Confidence' not in df.columns: df['Confidence'] = "N/A"
    if 'OI_Type' not in df.columns: df['OI_Type'] = "CHG"
    if 'TargetLevel' not in df.columns: df['TargetLevel'] = "N/A"
    if 'Boost_Notes' not in df.columns: df['Boost_Notes'] = ""
    if 'Time' not in df.columns: df['Time'] = ""

    # Numeric Conversion
    numeric_cols = ['SignalPrice', 'TargetPrice', 'TargetPct', 'RVOL', 'NetMovePct', 'RS_Score', 'OI_Change', 'Rank', 'Delta_OI', 'Delta_Price']
    for col in numeric_cols:
        if col not in df.columns: df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
    
    if 'BreakType' not in df.columns: df['BreakType'] = "N/A"
    
    return df

@st.cache_data(ttl=60)
def load_nse_sector_data():
    """Loads Raw NSE Data from NSE_OI_DATA Table for Sector View"""
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
# PAGE 1: LIVE ALERTS (BLASTS & FLOWS)
# =========================================================
def render_live_alerts(selected_date):
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

    st.subheader("🚀 Live OI Blasts & High Flows")
    
    # --- DATA ---
    # Query INTRADAY_BOOST but filter for the high quality ones
    df = load_data_from_dynamodb(selected_date, "INTRADAY_BOOST")
    
    if df.empty:
        st.info(f"No data found for {selected_date}")
        return

    # --- FILTER FOR BLASTS & HIGH CONFIDENCE ---
    # Logic: Either it has "BLAST" in notes OR it's High Confidence with big OI Change
    blast_mask = df['Boost_Notes'].str.contains("BLAST", case=False, na=False)
    flow_mask = (df['Confidence'] == "HIGH") & (df['OI_Change'].abs() > 15)
    
    alerts_df = df[blast_mask | flow_mask].copy()

    if alerts_df.empty:
        st.info("No 'Blast' or 'High Conviction' alerts yet today. Market might be quiet.")
        return

    # Fetch Live Prices
    loading_placeholder = st.empty()
    loading_placeholder.text(f'⚡ Fetching Prices for {selected_date}...')
    alerts_df = fetch_live_updates(alerts_df, selected_date)
    loading_placeholder.empty()

    # Calculations
    alerts_df['SignalPrice'] = pd.to_numeric(alerts_df['SignalPrice'], errors='coerce')
    alerts_df['Live_Price'] = pd.to_numeric(alerts_df['Live_Price'], errors='coerce')
    alerts_df['Live_Move_Pct'] = ((alerts_df['Live_Price'] - alerts_df['SignalPrice']) / alerts_df['SignalPrice']) * 100
    alerts_df['Live_Move_Pct'] = alerts_df['Live_Move_Pct'].fillna(0.0)
    alerts_df['Visual_Side'] = alerts_df['Direction'].map({'LONG': '🟢 LONG', 'SHORT': '🔴 SHORT'})
    
    # Identify Type
    def get_type(row):
        if "BLAST" in str(row['Boost_Notes']): return "🚀 BLAST"
        return "🌊 FLOW"
    
    alerts_df['Type'] = alerts_df.apply(get_type, axis=1)

    # --- METRICS ---
    total_alerts = len(alerts_df)
    blast_count = len(alerts_df[alerts_df['Type'] == "🚀 BLAST"])
    flow_count = len(alerts_df[alerts_df['Type'] == "🌊 FLOW"])
    
    c1, c2, c3 = st.columns(3)
    c1.markdown(metric_card("Total Alerts", total_alerts, "Significant Moves", "#eab308", glow=True), unsafe_allow_html=True)
    c2.markdown(metric_card("OI Blasts", blast_count, "Hidden Accumulation", "#60a5fa"), unsafe_allow_html=True)
    c3.markdown(metric_card("High Flows", flow_count, ">15% OI Change", "#22c55e"), unsafe_allow_html=True)

    st.divider()

    # --- TABLE ---
    if 'Name' in alerts_df.columns:
        alerts_df['Cleaned_Name'] = alerts_df['Name'].replace(TICKER_CORRECTIONS)
        alerts_df['TV_Symbol'] = DEFAULT_EXCHANGE + ":" + alerts_df['Cleaned_Name'].str.replace('&', '_').str.replace(' ', '')
        alerts_df['Chart'] = "https://www.tradingview.com/chart/?symbol=" + alerts_df['TV_Symbol']

    with st.container(border=True):
        st.markdown('<div class="table-header">🔥 Active Signals</div>', unsafe_allow_html=True)
        
        # Sort by Time (Latest First)
        if 'Time' in alerts_df.columns:
            alerts_df = alerts_df.sort_values(by='Time', ascending=False)
            
        st.data_editor(
            alerts_df[['Chart', 'Time', 'Name', 'Type', 'Visual_Side', 'SignalPrice', 'Delta_OI', 'OI_Change', 'TargetLevel']], 
            column_config={
                "Chart": st.column_config.LinkColumn("View", display_text="📊", width="small"),
                "Time": st.column_config.TextColumn("Entry Time", width="small"),
                "Name": st.column_config.TextColumn("Ticker", width="medium"),
                "Type": st.column_config.TextColumn("Alert Type", width="small"),
                "Visual_Side": st.column_config.TextColumn("Side", width="small"),
                "SignalPrice": st.column_config.NumberColumn("Price", format="%.2f"),
                "Delta_OI": st.column_config.NumberColumn("5m OI Surge", format="%.2f%%"),
                "OI_Change": st.column_config.NumberColumn("Total OI %", format="%.2f%%"),
                "TargetLevel": st.column_config.TextColumn("Target"),
            },
            hide_index=True, use_container_width=True, disabled=True, key="alerts_table"
        )

# =========================================================
# PAGE 2: MARKET LEADERBOARD (FULL LIST)
# =========================================================
def render_intraday_boost(selected_date):
    st.header("📈 Market Leaderboard (Intraday Boost)")
    st.info("Full list of Top Gainers/Losers monitored for Flows.")

    df = load_data_from_dynamodb(selected_date, "INTRADAY_BOOST")
    if df.empty:
        st.warning("No Boost alerts found.")
        return

    # 1. Prepare Chart Links
    if 'Name' in df.columns:
        df['Cleaned_Name'] = df['Name'].replace(TICKER_CORRECTIONS)
        df['TV_Symbol'] = DEFAULT_EXCHANGE + ":" + df['Cleaned_Name'].str.replace(' ', '')
        df['Chart'] = "https://www.tradingview.com/chart/?symbol=" + df['TV_Symbol']

    # 2. Ensure Numeric Sorting
    if 'SignalPrice' in df.columns: df['SignalPrice'] = pd.to_numeric(df['SignalPrice'], errors='coerce')
    if 'RS_Score' in df.columns: df['RS_Score'] = pd.to_numeric(df['RS_Score'], errors='coerce')
    if 'OI_Change' in df.columns: df['OI_Change'] = pd.to_numeric(df['OI_Change'], errors='coerce')
    
    if 'Rank' in df.columns: df = df.sort_values(by='Rank', ascending=True)

    st.markdown("### 🔥 Market Context")

    # 3. Classification Logic (Bulls vs Bears)
    def classify_boost_side(row):
        r_type = str(row.get('RankType', '')).upper()
        if 'TOP GAINER' in r_type: return 'LONG'
        if 'TOP LOSER' in r_type: return 'SHORT'

        b_type = str(row.get('BreakType', '')).upper()
        if 'PDH' in b_type or 'PWH' in b_type or 'HIGH' in b_type: return 'LONG'
        if 'PDL' in b_type or 'PWL' in b_type or 'LOW' in b_type: return 'SHORT'
        
        d = str(row.get('Direction', '')).upper()
        if d == 'LONG' or d == 'BULLISH': return 'LONG'
        if d == 'SHORT' or d == 'BEARISH': return 'SHORT'
        
        return 'UNKNOWN'

    df['Boost_Direction'] = df.apply(classify_boost_side, axis=1)

    df_bull = df[df['Boost_Direction'] == 'LONG'].copy()
    df_bear = df[df['Boost_Direction'] == 'SHORT'].copy()

    col1, col2 = st.columns(2)

    # --- DISPLAY COLUMNS (Added Delta_OI for context) ---
    display_cols = ['Chart', 'Name', 'SignalPrice', 'BreakType', 'OI_Signal', 'OI_Change']

    with col1:
        st.markdown("#### 🟢 Top Gainers & Breakouts")
        if not df_bull.empty:
            st.data_editor(
                df_bull[display_cols],
                column_config={
                    "Chart": st.column_config.LinkColumn("View", display_text="📈", width="small"),
                    "Name": st.column_config.TextColumn("Name", width="medium"),
                    "SignalPrice": st.column_config.NumberColumn("Price", format="%.2f"),
                    "BreakType": st.column_config.TextColumn("Status"),
                    "OI_Signal": st.column_config.TextColumn("OI Context"),
                    "OI_Change": st.column_config.NumberColumn("OI% Change", format="%.2f%%")
                },
                use_container_width=True, hide_index=True, disabled=True, key="bull_table"
            )
        else:
            st.caption("No Bullish setups found.")

    with col2:
        st.markdown("#### 🔴 Top Losers & Breakdowns")
        if not df_bear.empty:
            st.data_editor(
                df_bear[display_cols],
                column_config={
                    "Chart": st.column_config.LinkColumn("View", display_text="📉", width="small"),
                    "Name": st.column_config.TextColumn("Name", width="medium"),
                    "SignalPrice": st.column_config.NumberColumn("Price", format="%.2f"),
                    "BreakType": st.column_config.TextColumn("Status"),
                    "OI_Signal": st.column_config.TextColumn("OI Context"),
                    "OI_Change": st.column_config.NumberColumn("OI% Change", format="%.2f%%")
                },
                use_container_width=True, hide_index=True, disabled=True, key="bear_table"
            )
        else:
            st.caption("No Bearish setups found.")

# =========================================================
# PAGE 3: SECTOR VIEW (RAW DATA)
# =========================================================
def render_sector_view():
    st.header("📊 Sector Participation View")
    
    # 1. Load Data
    df = load_nse_sector_data()
    if df.empty:
        st.warning("No NSE OI Data found. Please check if the scraper is running.")
        return

    # 2. Process Data
    # Map Sectors
    df['Sector'] = df['symbol'].map(SECTOR_MAP).fillna('Others')
    
    # Ensure Numeric Types
    cols_to_fix = ['pChangeInOpenInterest', 'lastPrice', 'changeinOpenInterest']
    for col in cols_to_fix:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # 3. Sector Aggregation
    sector_stats = df.groupby('Sector')['pChangeInOpenInterest'].mean().reset_index()
    sector_stats = sector_stats.sort_values('pChangeInOpenInterest', ascending=False)
    
    sector_counts = df['Sector'].value_counts().reset_index()
    sector_counts.columns = ['Sector', 'Stock_Count']
    
    final_sector = pd.merge(sector_stats, sector_counts, on='Sector')
    final_sector = final_sector[final_sector['Sector'] != 'Others']

    # 4. Visualization
    st.subheader("🔥 Sector Heatmap (Avg OI Change %)")
    st.bar_chart(final_sector.set_index('Sector')['pChangeInOpenInterest'])

    # 5. Deep Dive Table
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.markdown("### 🏆 Top Sectors")
        st.dataframe(
            final_sector.style.format({"pChangeInOpenInterest": "{:.2f}%"}),
            column_order=("Sector", "pChangeInOpenInterest", "Stock_Count"),
            hide_index=True,
            use_container_width=True
        )

    with col2:
        st.markdown("### 🔍 Stock Drilldown")
        selected_sector = st.selectbox("Select Sector to Inspect", final_sector['Sector'].unique())
        
        subset = df[df['Sector'] == selected_sector].copy()
        subset = subset.sort_values('pChangeInOpenInterest', ascending=False)
        
        st.dataframe(
            subset[['symbol', 'pChangeInOpenInterest', 'openInterest', 'lastPrice']],
            column_config={
                "symbol": "Stock",
                "pChangeInOpenInterest": st.column_config.NumberColumn("OI Chg %", format="%.2f%%"),
                "openInterest": st.column_config.NumberColumn("Total OI", format="%d"),
                "lastPrice": st.column_config.NumberColumn("Price", format="%.2f"),
            },
            hide_index=True,
            use_container_width=True
        )

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
    # RENAMED PAGE 1 to reflect new logic
    page = st.radio("Navigate", ["🚀 Live Alerts (Blasts)", "📈 Market Leaderboard", "📊 Sector View"])
    st.divider()
    india_tz = pytz.timezone('Asia/Kolkata')
    selected_date = st.date_input("📅 Select Date", datetime.now(india_tz).date())

if page == "🚀 Live Alerts (Blasts)":
    render_live_alerts(selected_date)
elif page == "📈 Market Leaderboard":
    render_intraday_boost(selected_date)
elif page == "📊 Sector View":
    render_sector_view()
