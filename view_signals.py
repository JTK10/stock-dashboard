import streamlit as st
import pandas as pd
import boto3
from boto3.dynamodb.conditions import Attr
import os
from decimal import Decimal
from datetime import datetime
import pytz

# --- CONFIG ---
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1") 
DYNAMODB_TABLE = os.getenv("DYNAMODB_TABLE", "SentAlerts")
DEFAULT_EXCHANGE = "NSE"

# Global Mapping
TICKER_CORRECTIONS = {
    "PATANJALI FOODS LIMITED": "PATANJALI", "INDUSIND BANK LIMITED": "INDUSINDBK",
    "COAL INDIA LTD": "COALINDIA", "TATA MOTORS LIMITED": "TATAMOTORS",
    "INDIAN ENERGY EXC LTD": "IEX", "RELIANCE INDUSTRIES LTD": "RELIANCE",
    "CENTRAL DEPO SER (I) LTD": "CDSL", "GRASIM INDUSTRIES LTD": "GRASIM",
    "INDIAN RENEWABLE ENERGY": "IREDA", "LIFE INSURA CORP OF INDIA": "LICI",
    "FEDERAL BANK LTD": "FEDERALBNK", "JSW STEEL LIMITED": "JSWSTEEL",
    "RAIL VIKAS NIGAM LIMITED": "RVNL", "PIDILITE INDUSTRIES LTD": "PIDILITIND",
    "MANAPPURAM FINANCE LTD": "MANAPPURAM", "TORRENT PHARMACEUTICALS L": "TORNTPHARM",
    "BAJAJ AUTO LIMITED": "BAJAJ-AUTO", "LIC HOUSING FINANCE LTD": "LICHSGFIN",
    "DR. REDDY S LABORATORIES": "DRREDDY", "CIPLA LTD": "CIPLA",
    "MANKIND PHARMA LIMITED": "MANKIND", "NATIONAL ALUMINIUM CO LTD": "NATIONALUM",
    "CANARA BANK": "CANBK", "AVENUE SUPERMARTS LIMITED": "DMART",
    "STEEL AUTHORITY OF INDIA": "SAIL", "INDIAN RAIL TOUR CORP LTD": "IRCTC",
    "TORRENT POWER LTD": "TORNTPOWER", "JUBILANT FOODWORKS LTD": "JUBLFOOD",
    "INFO EDGE (I) LTD": "NAUKRI", "INTERGLOBE AVIATION LTD": "INDIGO",
    "UNION BANK OF INDIA": "UNIONBANK", "BAJAJ FINSERV LTD.": "BAJAJFINSV",
    "SUN PHARMACEUTICAL IND L": "SUNPHARMA", "EXIDE INDUSTRIES LTD": "EXIDEIND",
    "INDUS TOWERS LIMITED": "INDUSTOWER", "BHARAT PETROLEUM CORP  LT": "BPCL",
    "360 ONE WAM LIMITED": "360ONE", "SOLAR INDUSTRIES (I) LTD": "SOLARINDS",
    "TATA POWER CO LTD": "TATAPOWER", "SUPREME INDUSTRIES LTD": "SUPREMEIND",
    "ONE 97 COMMUNICATIONS LTD": "PAYTM", "TATA TECHNOLOGIES LIMITED": "TATATECH",
    "HERO MOTOCORP LIMITED": "HEROMOTOCO", "MARICO LIMITED": "MARICO",
    "VODAFONE IDEA LIMITED": "IDEA", "BRITANNIA INDUSTRIES LTD": "BRITANNIA",
    "ICICI PRU LIFE INS CO LTD": "ICICIPRULI", "AU SMALL FINANCE BANK LTD": "AUBANK",
    "INDIAN RAILWAY FIN CORP L": "IRFC", "FORTIS HEALTHCARE LTD": "FORTIS",
    "HINDUSTAN AERONAUTICS LTD": "HAL", "BHARAT FORGE LTD": "BHARATFORG",
    "BHARTI AIRTEL LIMITED": "BHARTIARTL", "TRENT LTD": "TRENT",
    "KALYAN JEWELLERS IND LTD": "KALYANKJIL", "AXIS BANK LIMITED": "AXISBANK",
    "BOSCH LIMITED": "BOSCHLTD", "ZYDUS LIFESCIENCES LTD": "ZYDUSLIFE",
    "PI INDUSTRIES LTD": "PIIND", "HINDUSTAN PETROLEUM CORP": "HINDPETRO",
    "MARUTI SUZUKI INDIA LTD.": "MARUTI", "THE PHOENIX MILLS LTD": "PHOENIXLTD",
    "NBCC (INDIA) LIMITED": "NBCC", "BSE LIMITED": "BSE",
    "HINDUSTAN ZINC LIMITED": "HINDZINC", "IDFC FIRST BANK LIMITED": "IDFCFIRSTB",
    "HSG & URBAN DEV CORPN LTD": "HUDCO", "ITC LTD": "ITC",
    "AUROBINDO PHARMA LTD": "AUROPHARMA", "KAYNES TECHNOLOGY IND LTD": "KAYNES",
    "TVS MOTOR COMPANY  LTD": "TVSMOTOR", "BHEL": "BHEL",
    "EICHER MOTORS LTD": "EICHERMOT", "BAJAJ FINANCE LIMITED": "BAJFINANCE",
    "TATA ELXSI LIMITED": "TATAELXSI", "NCC LIMITED": "NCC",
    "OBEROI REALTY LIMITED": "OBEROIRLTY", "HAVELLS INDIA LIMITED": "HAVELLS",
    "TATA CONSULTANCY SERV LT": "TCS", "JINDAL STEEL LIMITED": "JINDALSTEL",
    "CROMPT GREA CON ELEC LTD": "CROMPTON", "ALKEM LABORATORIES LTD.": "ALKEM",
    "ICICI BANK LTD.": "ICICIBANK", "DLF LIMITED": "DLF",
    "NESTLE INDIA LIMITED": "NESTLEIND", "AMBER ENTERPRISES (I) LTD": "AMBER",
    "SYNGENE INTERNATIONAL LTD": "SYNGENE", "DIXON TECHNO (INDIA) LTD": "DIXON",
    "COMPUTER AGE MNGT SER LTD": "CAMS", "BIOCON LIMITED.": "BIOCON",
    "DABUR INDIA LTD": "DABUR", "PAGE INDUSTRIES LTD": "PAGEIND",
    "ADANI PORT & SEZ LTD": "ADANIPORTS", "SONA BLW PRECISION FRGS L": "SONACOMS",
    "CYIENT LIMITED": "CYIENT", "ADANI GREEN ENERGY LTD": "ADANIGREEN",
    "CONTAINER CORP OF IND LTD": "CONCOR", "COFORGE LIMITED": "COFORGE",
    "HDFC AMC LIMITED": "HDFCAMC", "HDFC LIFE INS CO LTD": "HDFCLIFE",
    "DALMIA BHARAT LIMITED": "DALBHARAT", "ASTRAL LIMITED": "ASTRAL",
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
    "MAHINDRA & MAHINDRA LTD": "M_M", "INDIAN OIL CORP LTD": "IOC",
    "HFCL LIMITED": "HFCL", "KPIT TECHNOLOGIES LIMITED": "KPITTECH",
    "SAMMAAN CAPITAL LIMITED": "SAMMAANCAP", "HINDALCO  INDUSTRIES  LTD": "HINDALCO",
    "PERSISTENT SYSTEMS LTD": "PERSISTENT", "CHOLAMANDALAM IN & FIN CO": "CHOLAFIN",
    "INOX WIND LIMITED": "INOXWIND", "BLUE STAR LIMITED": "BLUESTARCO",
    "ABB INDIA LIMITED": "ABB", "LARSEN & TOUBRO LTD.": "LT",
    "ULTRATECH CEMENT LIMITED": "ULTRACEMCO", "INFOSYS LIMITED": "INFY",
    "GAIL (INDIA) LTD": "GAIL", "PETRONET LNG LIMITED": "PETRONET",
    "POWER FIN CORP LTD.": "PFC", "PNB HOUSING FIN LTD.": "PNBHOUSING",
    "WIPRO LTD": "WIPRO", "REC LIMITED": "RECLTD", "PRESTIGE ESTATE LTD": "PRESTIGE",
    "FSN E COMMERCE VENTURES": "NYKAA", "ADANI ENERGY SOLUTION LTD": "ADANIENSOL",
    "ADANI ENTERPRISES LIMITED": "ADANIENT", "TATA STEEL LIMITED": "TATASTEEL",
    "RBL BANK LIMITED": "RBLBANK", "INDRAPRASTHA GAS LTD": "IGL",
    "YES BANK LIMITED": "YESBANK", "SAMVRDHNA MTHRSN INTL LTD": "MOTHERSON",
    "GLENMARK PHARMACEUTICALS": "GLENMARK", "ADITYA BIRLA CAPITAL LTD.": "ABCAPITAL",
    "L&T FINANCE LIMITED": "LTF", "UNO MINDA LIMITED": "UNOMINDA",
    "ORACLE FIN SERV SOFT LTD.": "OFSS", "BANDHAN BANK LIMITED": "BANDHANBNK",
    "TECH MAHINDRA LIMITED": "TECHM", "IIFL FINANCE LIMITED": "IIFL",
    "JIO FIN SERVICES LTD": "JIOFIN", "MPHASIS LIMITED": "MPHASIS",
    "SIEMENS LTD": "SIEMENS", "OIL INDIA LTD": "OIL", "JSW ENERGY LIMITED": "JSWENERGY",
    "TUBE INVEST OF INDIA LTD": "TIINDIA", "LTIMINDTREE LIMITED": "LTIM",
    "ASHOK LEYLAND LTD": "ASHOKLEY", "THE INDIAN HOTELS CO. LTD": "INDHOTEL",
    "DELHIVERY LIMITED": "DELHIVERY", "DIVI S LABORATORIES LTD": "DIVISLAB",
    "TITAGARH RAIL SYSTEMS LTD": "TITAGARH", "BANK OF BARODA": "BANKBARODA",
    "MAX FINANCIAL SERV LTD": "MFSL", "MULTI COMMODITY EXCHANGE": "MCX",
    "TITAN COMPANY LIMITED": "TITAN", "VOLTAS LTD": "VOLTAS", "SRF LTD": "SRF",
    "ASIAN PAINTS LIMITED": "ASIANPAINT", "MUTHOOT FINANCE LIMITED": "MUTHOOTFIN",
    "PB FINTECH LIMITED": "POLICYBZR", "POLYCAB INDIA LIMITED": "POLYCAB",
    "BHARAT ELECTRONICS LTD": "BEL", "BANK OF INDIA": "BANKINDIA",
    "BHARAT DYNAMICS LIMITED": "BDL", "HINDUSTAN UNILEVER LTD.": "HINDUNILVR",
    "POWER GRID CORP. LTD.": "POWERGRID", "GODREJ PROPERTIES LTD": "GODREJPROP",
    "GMR AIRPORTS LIMITED": "GMRAIRPORT", "GODREJ CONSUMER PRODUCTS": "GODREJCP",
    "INDIAN BANK": "INDIANB", "PIRAMAL PHARMA LIMITED": "PPLPHARMA",
    "OIL AND NATURAL GAS CORP.": "ONGC", "KEI INDUSTRIES LTD.": "KEI",
    "LAURUS LABS LIMITED": "LAURUSLABS", "STATE BANK OF INDIA": "SBIN",
    "SHREE CEMENT LIMITED": "SHREECEM", "SHRIRAM FINANCE LIMITED": "SHRIRAMFIN",
    "PUNJAB NATIONAL BANK": "PNB", "UNITED SPIRITS LIMITED": "UNITDSPR",
    "UPL LIMITED": "UPL", "VARUN BEVERAGES LIMITED": "VBL",
    "VEDANTA LIMITED": "VEDL", "KFIN TECHNOLOGIES LIMITED": "KFINTECH",
    "NUVAMA WEALTH MANAGE LTD": "NUVAMA", "MAX HEALTHCARE INS LTD": "MAXHEALTH",
    "MAZAGON DOCK SHIPBUIL LTD": "MAZDOCK", "TATA CHEMICALS LTD": "TATACHEM"
}

st.set_page_config(page_title="Scanner Dashboard", layout="wide", page_icon="⚡")

def convert_decimal(obj):
    if isinstance(obj, list): return [convert_decimal(i) for i in obj]
    elif isinstance(obj, dict): return {k: convert_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal): return float(obj)
    return obj

@st.cache_data(ttl=60)
def load_data_from_dynamodb(target_date, signal_type):
    try:
        dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
        table = dynamodb.Table(DYNAMODB_TABLE)
        target_date_str = target_date.isoformat()
        response = table.scan(FilterExpression=Attr("Date").eq(target_date_str))
        items = response.get('Items', [])
        while 'LastEvaluatedKey' in response:
            response = table.scan(FilterExpression=Attr("Date").eq(target_date_str), ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response.get('Items', []))
    except Exception as e:
        st.error(f"Error: {e}")
        return pd.DataFrame()

    if not items: return pd.DataFrame()
    data = [convert_decimal(item) for item in items]
    df = pd.DataFrame(data)
    
    # Filter by Signal Type
    if 'Signal' in df.columns:
        if signal_type == "ALPHA_HUNTER":
            df = df[df['Signal'] == "ALPHA_HUNTER"].copy()
        elif signal_type == "INTRADAY_BOOST":
            df = df[df['Signal'] == "INTRADAY_BOOST"].copy()
    
    return df

# === PAGE 1: ALPHA HUNTER ===
def render_alpha_hunter(selected_date):
    st.header("🦁 Alpha Hunter")
    df = load_data_from_dynamodb(selected_date, "ALPHA_HUNTER")
    if df.empty:
        st.warning("No alerts.")
        return

    # Standardize Columns (Fix numeric conversion for existing page too)
    cols_to_convert = ['SignalPrice', 'NetMovePct', 'RVOL', 'SuperScore']
    for c in cols_to_convert:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')

    # Add TradingView Links
    if 'Name' in df.columns:
        df['Cleaned_Name'] = df['Name'].replace(TICKER_CORRECTIONS)
        df['TV_Symbol'] = DEFAULT_EXCHANGE + ":" + df['Cleaned_Name'].str.replace(' ', '')
        df['Chart'] = "https://www.tradingview.com/chart/?symbol=" + df['TV_Symbol'] + "&interval=5"

    st.data_editor(
        df[['Chart', 'Name', 'Time', 'Side', 'SignalPrice', 'NetMovePct', 'RVOL', 'SuperScore', 'Rating', 'PD_Break']], 
        column_config={
            "Chart": st.column_config.LinkColumn("Chart", display_text="📈"),
            "SignalPrice": st.column_config.NumberColumn("Price", format="%.2f"),
            "NetMovePct": st.column_config.NumberColumn("Move %", format="%.2f%%"),
            "RVOL": st.column_config.NumberColumn("RVOL", format="%.2fx"),
        },
        use_container_width=True, hide_index=True, disabled=True
    )

# === PAGE 2: INTRADAY BOOST ===
def render_intraday_boost(selected_date):
    st.header("🚀 Intraday Boost (Accumulated Gainers/Losers)")
    st.info("Showing F&O stocks accumulated till 12 PM that broke PDH/PDL/PWH/PWL.")
    
    df = load_data_from_dynamodb(selected_date, "INTRADAY_BOOST")
    if df.empty:
        st.warning("No Boost alerts yet.")
        return

    # --- FIX START: Convert Strings to Numbers before Sorting ---
    if 'NetMovePct' in df.columns:
        df['NetMovePct'] = pd.to_numeric(df['NetMovePct'], errors='coerce').fillna(0.0)
        df['AbsMove'] = df['NetMovePct'].abs()
        df = df.sort_values(by='AbsMove', ascending=False)
    
    if 'SignalPrice' in df.columns:
        df['SignalPrice'] = pd.to_numeric(df['SignalPrice'], errors='coerce')
    # --- FIX END ---

    if 'Name' in df.columns:
        df['Cleaned_Name'] = df['Name'].replace(TICKER_CORRECTIONS)
        df['TV_Symbol'] = DEFAULT_EXCHANGE + ":" + df['Cleaned_Name'].str.replace(' ', '')
        df['Chart'] = "https://www.tradingview.com/chart/?symbol=" + df['TV_Symbol'] + "&interval=5"

    # Columns
    cols = ['Chart', 'Name', 'Time', 'BreakType', 'SignalPrice', 'NetMovePct']
    final_cols = [c for c in cols if c in df.columns]

    st.data_editor(
        df[final_cols],
        column_config={
            "Chart": st.column_config.LinkColumn("Chart", display_text="📈"),
            "SignalPrice": st.column_config.NumberColumn("Price", format="%.2f"),
            "NetMovePct": st.column_config.NumberColumn("% Change", format="%.2f%%"),
            "BreakType": st.column_config.TextColumn("Broken Level", help="PDH: Prev Day High, etc.")
        },
        use_container_width=True, hide_index=True, disabled=True
    )

# === MAIN ===
st.sidebar.title("⚡ Dashboard")
page = st.sidebar.radio("Navigate", ["Alpha Hunter", "Intraday Boost"])
india_tz = pytz.timezone('Asia/Kolkata')
sel_date = st.sidebar.date_input("Date", datetime.now(india_tz).date())

if page == "Alpha Hunter": render_alpha_hunter(sel_date)
else: render_intraday_boost(sel_date)
