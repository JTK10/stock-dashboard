import streamlit as st
import pandas as pd
import boto3
import io
import json
import os

# --- CONFIG ---
# Use environment variable if available, otherwise default
S3_BUCKET = os.getenv("S3_BUCKET_NAME", "jtscanner") 
DEFAULT_EXCHANGE = "NSE"

# === UPDATED TRADINGVIEW MAPPING (From your list) ===
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

# --- STREAMLIT PAGE ---
st.set_page_config(page_title="Golden Scanner Dashboard", layout="wide", page_icon="⚡")
st.title("⚡ Golden Momentum Scanner Dashboard")

# --- LOAD JSON FROM S3 ---
@st.cache_data(ttl=300)
def load_latest_json_from_s3():
    """
    Fetches the latest JSON log from S3 and standardizes columns for Hybrid Compatibility.
    Supports both Old Scanner (BrokenLevel) and New Scanner (Golden Momentum).
    """
    try:
        s3 = boto3.client("s3")
        resp = s3.list_objects_v2(Bucket=S3_BUCKET)
    except Exception as e:
        st.error(f"Could not init S3 client. Check credentials. Error: {e}")
        return pd.DataFrame()

    # Find alert log files
    contents = resp.get("Contents", [])
    if not contents:
        st.warning("Bucket is empty or not accessible.")
        return pd.DataFrame()

    json_files = [
        obj["Key"] for obj in contents
        if "sent_alerts_log_" in obj["Key"] and obj["Key"].endswith(".json")
    ]
    
    if not json_files:
        st.warning("No 'sent_alerts_log_' JSON files found in bucket.")
        return pd.DataFrame()

    # Get the absolute latest file by timestamp/name
    latest_file = sorted(json_files)[-1]
    file_date = latest_file.replace("sent_alerts_log_", "").replace(".json", "")
    st.info(f"📅 Displaying Alerts for: **{file_date}** (File: `{latest_file}`)")

    # Read File
    buf = io.BytesIO()
    try:
        s3.download_fileobj(S3_BUCKET, latest_file, buf)
        buf.seek(0)
        df = pd.read_json(buf)
    except Exception as e:
        st.error(f"Error reading JSON file: {e}")
        return pd.DataFrame()

    if df.empty:
        return df

    # === HYBRID COMPATIBILITY LAYER ===
    # This block ensures New Scanner Data looks like Old Scanner Data where needed
    
    # 1. Map 'Price' (New) to 'SignalPrice' (Old/UI expected)
    if 'Price' in df.columns and 'SignalPrice' not in df.columns:
        df.rename(columns={'Price': 'SignalPrice'}, inplace=True)
        
    # 2. Map 'BrokenLevel' (Used in Filters)
    # The new scanner uses VWAP/PDH implicitly. We map Signal -> Level name.
    if 'BrokenLevel' not in df.columns:
        if 'Signal' in df.columns:
            df['BrokenLevel'] = df['Signal'].map({'LONG': 'PDH Breakout', 'SHORT': 'PDL Breakdown'})
        else:
            df['BrokenLevel'] = 'N/A'
            
    # 3. Handle 'LevelValue' (Optional display)
    if 'LevelValue' not in df.columns:
        # For new scanner, we might want to show VWAP or leaving it blank is fine
        if 'VWAP' in df.columns:
             df['LevelValue'] = df['VWAP'] # Proxy VWAP as the reference level
        else:
             df['LevelValue'] = 0.0

    return df

# --- MAIN APP ---
try:
    df = load_latest_json_from_s3()
    
    if df.empty:
        st.warning("⚠️ No alerts found in the loaded file.")
        st.stop()

    # --- Sidebar Filters ---
    st.sidebar.header("🔍 Filter Alerts")
    
    # Signal Filter
    if 'Signal' in df.columns:
        signals = sorted(df['Signal'].astype(str).unique().tolist())
        selected_signals = st.sidebar.multiselect('Signal Type', signals, default=signals)
    else:
        selected_signals = []
        
    # Level Filter
    if 'BrokenLevel' in df.columns:
        levels = sorted(df['BrokenLevel'].astype(str).unique().tolist())
        selected_levels = st.sidebar.multiselect('Setup Type', levels, default=levels)
    else:
        selected_levels = []

    # Apply Filters
    mask = pd.Series(True, index=df.index)
    if 'Signal' in df.columns:
        mask &= df['Signal'].isin(selected_signals)
    if 'BrokenLevel' in df.columns:
        mask &= df['BrokenLevel'].isin(selected_levels)
        
    df_filtered = df[mask].copy()

    # --- Data Presentation ---
    
    # 1. TradingView Link Generation
    if 'Name' in df_filtered.columns:
        # Use the Mapping Dictionary to get clean ticker
        df_filtered['Cleaned_Name'] = df_filtered['Name'].replace(TICKER_CORRECTIONS)
        
        # Create Symbol column for URL
        df_filtered['TV_Symbol'] = DEFAULT_EXCHANGE + ":" + df_filtered['Cleaned_Name'].str.replace(' ', '')
        
        # Generate URL
        df_filtered['Chart'] = (
            "https://www.tradingview.com/chart/?symbol=" + df_filtered['TV_Symbol'] + "&interval=5"
        )
    else:
        st.error("Column 'Name' missing. Cannot generate charts.")

    # 2. Column Selection & Ordering
    # We want a clean view. We prefer New Scanner columns if available.
    
    desired_order = [
        'Chart', 'Name', 'Time', 'Signal', 'SignalPrice', 
        'OIChgPct', 'MomentumPct', 'VWAP', # New Scanner Columns
        'BrokenLevel', 'LevelValue'        # Old/Hybrid Columns
    ]
    
    # Filter only columns that actually exist in this dataframe
    final_cols = [c for c in desired_order if c in df_filtered.columns]
    
    # Add any extra columns at the end (debugging)
    # remaining_cols = [c for c in df_filtered.columns if c not in final_cols and c not in ['InstrumentKey', 'Symbol', 'Cleaned_Name', 'TV_Symbol']]
    # final_cols.extend(remaining_cols)

    df_display = df_filtered[final_cols]

    # --- Display ---
    st.metric(label="Total Alerts", value=len(df_display))
    
    # Configure Columns
    column_config = {
        "Chart": st.column_config.LinkColumn("TradingView", display_text="📈 Open Chart"),
        "SignalPrice": st.column_config.NumberColumn("Price", format="%.2f"),
        "VWAP": st.column_config.NumberColumn("VWAP", format="%.2f"),
        "OIChgPct": st.column_config.NumberColumn("OI Chg %", format="%.2f%%"),
        "MomentumPct": st.column_config.NumberColumn("Mom %", format="%.2f%%"),
        "LevelValue": st.column_config.NumberColumn("Ref Lvl", format="%.2f"),
    }

    st.data_editor(
        df_display, 
        use_container_width=True, 
        disabled=True, 
        column_config=column_config,
        hide_index=True
    )

except Exception as e:
    st.error(f"Unexpected error in dashboard: {e}")
    # st.exception(e) # Uncomment for debugging
    st.stop()
