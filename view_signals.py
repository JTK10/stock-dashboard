import streamlit as st
import pandas as pd
import boto3
import io
import json

# --- CONFIG ---
S3_BUCKET = "jtscanner"

# ASSUMPTION: All symbols are from this exchange. Change if needed.
# Examples: "NSE", "BSE", "NASDAQ", "NYSE"
DEFAULT_EXCHANGE = "NSE" 

# Update REQUIRED_COLUMNS to reflect the actual column names in your JSON
REQUIRED_COLUMNS = ['Name', 'Date', 'Time', 'Signal', 'BrokenLevel', 'LevelValue', 'SignalPrice']

# 1. Define a map to correct special ticker names for TradingView compatibility.
#    This map was automatically generated to include all 209 entries from 'instrument_keys (1).csv',
#    plus a few manual corrections for common symbols.
#    Format: "Name_from_S3_Data": "Name_TradingView_Expects"
TICKER_CORRECTIONS = {
    # --- Manual Corrections ---
    "BAJAJ-AUTO": "BAJAJAUTO",
    "M&M": "M_M",
    "M&MFIN": "M_MFIN",
    "ADANI-PORTS": "ADANIPORTS",
    "MCDOWELL-N": "MCDOWELLN",
    "HDFC-BANK": "HDFCBANK",
    
    # --- Full List of 209 Mappings from instrument_keys (1).csv ---
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
    "INFOSYS LTD": "INFY",
    "ADANI TOTAL GAS LIMITED": "ATGL",
    "ZOMATO LIMITED": "ZOMATO",
    "HCL TECHNOLOGIES LTD": "HCLTECH",
    "WIPRO LTD": "WIPRO",
    "VEDANTA LIMITED": "VEDL",
    "ICICI BANK LTD.": "ICICIBANK",
    "NATIONAL ALUMINIUM CO LTD": "NATIONALUM",
    "TECH MAHINDRA LTD": "TECHM",
    "AU SMALL FINANCE BANK LTD": "AUBANK",
    "MAHINDRA & MAHINDRA LTD": "M_M",
    "ADANI ENTERPRISES LTD": "ADANIENT",
    "SUN PHARM. INDS. LTD.": "SUNPHARMA",
    "TATA STEEL LTD": "TATASTEEL",
    "BHARTI AIRTEL LTD": "BHARTIARTL",
    "CANARA BANK": "CANARABANK",
    "ACCELYA SOLUTIONS INDIA": "ACCELYA",
    "TATA CONSULTANCY SERV LTD": "TCS",
    "BANK OF BARODA": "BANKBARODA",
    "HINDALCO INDUSTRIES LTD.": "HINDALCO",
    "BHARAT FORGE LTD": "BHARATFORG",
    "PUNJAB NATIONAL BANK": "PNB",
    "ABB INDIA LIMITED": "ABB",
    "LARSEN & TOUBRO LTD.": "LT",
    "BAJAJ FINANCE LIMITED": "BAJFINANCE",
    "DLF LIMITED": "DLF",
    "NMDC LTD.": "NMDC",
    "GAIL (INDIA) LTD": "GAIL",
    "IRCTC LIMITED": "IRCTC",
    "HOUSING DEVELOPMENT FIN.": "HDFC",
    "SIEMENS LTD": "SIEMENS",
    "INDIGO PAINTS LIMITED": "INDIGOPNTS",
    "MARUTI SUZUKI INDIA LTD.": "MARUTI",
    "GODREJ AGROVET LIMITED": "GODREJAGRO",
    "KOTAK MAHINDRA BANK LTD": "KOTAKBANK",
    "BRITANNIA INDUSTRIES LTD": "BRITANNIA",
    "HDFC LIFE INSURANCE CO.": "HDFCLIFE",
    "CHOLAMANDALAM INV. & FIN": "CHOLAFIN",
    "DR. REDDY'S LABS LTD": "DRREDDY",
    "ULTRATECH CEMENT LTD": "ULTRACEMCO",
    "BAJAJ AUTO LIMITED": "BAJAJAUTO",
    "TRENT LIMITED": "TRENT",
    "AXIS BANK LIMITED": "AXISBANK",
    "NTPC LTD": "NTPC",
    "TITAN COMPANY LIMITED": "TITAN",
    "UNITED SPIRITS LIMITED": "MCDOWELL-N",
    "ADANI PORTS & SEZ LTD": "ADANIPORTS",
    "BAJAJ FINSERV LTD.": "BAJAJFINSV",
    "OIL INDIA LIMITED": "OIL",
    "YES BANK LTD": "YESBANK",
    "BOSCH LIMITED": "BOSCHLTD",
    "AMBUJA CEMENTS LTD": "AMBUJACEM",
    "EICHER MOTORS LTD": "EICHERMOT",
    "SHREE CEMENTS LIMITED": "SHREECEM",
    "HIND. AERONAUTICS LTD": "HAL",
    "STATE BANK OF INDIA": "SBIN",
    "HINDUSTAN PETROLEUM CORP": "HINDPETRO",
    "GMR INFRASTRUCTURE LTD.": "GMRINFRA",
    "INTERGLOBE AVIATION LTD": "INDIGO",
    "ADANI POWER LIMITED": "ADANIPOWER",
    "JINDAL STEEL & POWER LTD": "JINDALSTEL",
    "TATA POWER CO. LTD.": "TATAPOWER",
    "POWER FINANCE CORP. LTD.": "PFC",
    "UNION BANK OF INDIA": "UNIONBANK",
    "HONEYWELL AUTOMATION IND": "HONAUT",
    "TATA CONSUMER PROD LTD": "TATACONSUM",
    "ICICI PRU LIFE INS CO LTD": "ICICIPRULI",
    "AVANTI FEEDS LTD": "AVANTIFEED",
    "TATA CHEMICALS LTD": "TATACHEM",
    "APOLLO HOSPITALS ENT. L": "APOLLOHOSP",
    "H.G. INFRA ENGR. LTD": "HGINFRA",
    "DABUR INDIA LTD": "DABUR",
    "CIPLA LTD": "CIPLA",
    "SHIVALIK BIMANO MFG. CO": "SHIVALIK",
    "M&M FINANCIAL SERVICES": "M_MFIN",
    "TORRENT PHARMA LTD": "TORNTPHARM",
    "ADANI TRANSMISSION LTD": "ADANITRANS",
    "NESTLE INDIA LTD": "NESTLEIND",
    "PETRONET LNG LIMITED": "PETRONET",
    "SUN TV NETWORK LIMITED": "SUNTV",
    "BHARAT PETROLEUM CORP LTD": "BPCL",
    "TVS MOTOR COMPANY LTD": "TVSMOTOR",
    "HERO MOTOCORP LIMITED": "HEROMOTOCO",
    "ICICI LOMBARD GIC LTD": "ICICIGI",
    "ZEE ENTERTAINMENT ENT. L": "ZEEL",
    "INDIABULLS HSG FIN LTD": "IBULHSGFIN",
    "PERSISTENT SYSTEMS LTD": "PERSISTENT",
    "APOLLO TYRES LTD": "APOLLOTYRE",
    "DLF LTD": "DLF",
    "ADANI GREEN ENERGY LTD": "ADANIGREEN",
    "TATA COMMUNICATIONS LTD": "TATACOMM",
    "AUROBINDO PHARMA LTD": "AUROPHARMA",
    "TECH MAHINDRA LTD": "TECHM",
    "HIND ZINC LTD": "HINDZINC",
    "NATIONAL FERTILIZERS LTD": "NFL",
    "COLGATE-PALMOLIVE (I) L": "COLPAL",
    "CAN FIN HOMES LTD": "CANFINHOME",
    "DIVI'S LAB. LTD.": "DIVISLAB",
    "JINDAL SAW LIMITED": "JINDALSAW",
    "L&T TECHNOLOGY SERV LTD": "LTTS",
    "MINDTREE LTD": "MINDTREE",
    "TATA ELXSI LIMITED": "TATAELXSI",
    "VEDANTA LTD": "VEDL",
    "VOLTAS LTD": "VOLTAS",
    "ZEE ENTERTAINMENT ENT.": "ZEEL",
    "CHOLAMANDALAM INV & FIN": "CHOLAFIN",
    "GMR INFRASTRUCTURE LTD": "GMRINFRA",
    "HDFC ASSET MGMT CO LTD": "HDFCAMC",
    "HDFC LIFE INS CO LTD": "HDFCLIFE",
    "ICICI LOMBARD GIC LTD": "ICICIGI",
    "L&T FINANCE HOLDINGS LTD": "L_TFH",
    "LIC HOUSING FINANCE LTD": "LICHSGFIN",
    "M&M FINANCIAL SERVICES": "M_MFIN",
    "PB FINTECH LTD": "POLICYBZR",
    "STAR HEALTH & ALLIED IN": "STARHEALTH",
    "SYNGENE INTERNATIONAL L": "SYNGENE",
    "ABBOT INDIA LIMITED": "ABBOTINDIA",
    "AJANTA PHARMA LIMITED": "AJANTPHARM",
    "ALKEM LABORATORIES LTD": "ALKEM",
    "ALEMBIC PHARMACEUTICALS": "APLLtd",
    "AMARA RAJA BATTERIES LT": "AMARAJABAT",
    "APOLLO TYRES LTD": "APOLLOTYRE",
    "ASHOK LEYLAND LTD": "ASHOKLEY",
    "AUROBINDO PHARMA LTD": "AUROPHARMA",
    "BALKRISHNA IND. LTD.": "BALKRISHNA",
    "BANDHAN BANK LIMITED": "BANDHANBNK",
    "BASF INDIA LTD": "BASF",
    "BHARAT FORGE LTD": "BHARATFORG",
    "BIOCON LIMITED": "BIOCON",
    "BOSCH LIMITED": "BOSCHLTD",
    "CADILA HEALTHCARE LTD": "CADILAHC",
    "CG POWER AND IND. SOL.": "CGPOWER",
    "CROMPTON GREAVES CONS.": "CROMPTON",
    "CUMMINS INDIA LTD": "CUMMINSIND",
    "DELHIVERY LIMITED": "DELHIVERY",
    "DIXON TECHNOLOGIES LTD": "DIXON",
    "DREAMFOLKS SERVICES LTD": "DREAMFOLKS",
    "DR. REDDY'S LABS LTD": "DRREDDY",
    "EICHER MOTORS LTD": "EICHERMOT",
    "EXIDE INDUSTRIES LTD": "EXIDEIND",
    "FEDERAL BANK LTD": "FEDERALBNK",
    "GLAXOSMITHKLINE PHAR.": "GLAXO",
    "GODREJ PROPERTIES LTD": "GODREJPROP",
    "GRAPHITE INDIA LTD": "GRAPHITE",
    "GRINDWELL NORTON LTD": "GRINDWELL",
    "H.G. INFRA ENGR. LTD": "HGINFRA",
    "HBL POWER SYSTEMS LTD": "HBLPOWER",
    "HDFC ASSET MANAGEMENT CO": "HDFCAMC",
    "HDFC BANK LTD": "HDFCBANK",
    "HDFC LIFE INS CO LTD": "HDFCLIFE",
    "HEIDELBERGCEMENT IND.": "HEIDELBERG",
    "HONEYWELL AUTO. INDIA": "HONAUT",
    "INDIABULLS HSG FIN LTD": "IBULHSGFIN",
    "INDIAN HOTELS CO LTD": "INDHOTEL",
    "INDIGO PAINTS LTD": "INDIGOPNTS",
    "INFOSYS LTD": "INFY",
    "IRCTC LIMITED": "IRCTC",
    "JK CEMENT LTD": "JKCEMENT",
    "JSW ENERGY LIMITED": "JSWENERGY",
    "JUBILANT FOODWORKS LTD": "JUBLFOOD",
    "KOTAK MAHINDRA BANK LTD": "KOTAKBANK",
    "LARSEN & TOUBRO LTD": "LT",
    "LICI": "LICI",
    "LIC HOUSING FINANCE LTD": "LICHSGFIN",
    "LUPIN LIMITED": "LUPIN",
    "M&M FINANCIAL SERVICES": "M_MFIN",
    "MARUTI SUZUKI INDIA LTD": "MARUTI",
    "MCDOWELL-N": "MCDOWELL-N",
    "MINDTREE LTD": "MINDTREE",
    "MUTHOOT FINANCE LIMITED": "MUTHOOTFIN",
    "NATIONAL FERTILIZERS LTD": "NFL",
    "NESTLE INDIA LTD": "NESTLEIND",
    "NTPC LTD": "NTPC",
    "OIL INDIA LIMITED": "OIL",
    "ONGC": "ONGC",
    "P&G HEALTH LTD": "PGHL",
    "PERM.SYSTEMS LTD": "PERSISTENT",
    "PIDILITE IND LTD": "PIDILITIND",
    "PIRAMAL ENTERPRISES LTD": "PEL",
    "PIRAMAL PHARMA LIMITED": "PPLPHARMA",
    "PNB": "PNB",
    "POWER FINANCE CORP. LTD": "PFC",
    "POWER GRID CORP. LTD": "POWERGRID",
    "RAMCO CEMENTS LTD": "RAMCOCEM",
    "RBL BANK LIMITED": "RBLBANK",
    "RELIANCE INDUSTRIES LTD": "RELIANCE",
    "SHREE CEMENTS LIMITED": "SHREECEM",
    "SIEMENS LTD": "SIEMENS",
    "SRF LIMITED": "SRF",
    "STAR HEALTH & ALLIED IN": "STARHEALTH",
    "SUN PHARM. INDS. LTD": "SUNPHARMA",
    "SUN TV NETWORK LIMITED": "SUNTV",
    "SYNGENE INTERNATIONAL LTD": "SYNGENE",
    "TATA CONSUMER PROD LTD": "TATACONSUM",
    "TATA STEEL LTD": "TATASTEEL",
    "TATA MOTORS LIMITED": "TATAMOTORS",
    "TCS": "TCS",
    "TECH MAHINDRA LTD": "TECHM",
    "TORNTPHARM": "TORNTPHARM",
    "TRENT LIMITED": "TRENT",
    "TVS MOTOR COMPANY LTD": "TVSMOTOR",
    "UNION BANK OF INDIA": "UNIONBANK",
    "UNITED SPIRITS LIMITED": "MCDOWELL-N",
    "VEDANTA LIMITED": "VEDL",
    "VOLTAS LTD": "VOLTAS",
    "WIPRO LTD": "WIPRO",
    "YES BANK LTD": "YESBANK",
    "ZOMATO LIMITED": "ZOMATO"
}


# --- PAGE SETUP ---
st.set_page_config(page_title="Stock Scanner Dashboard", layout="wide")
st.title("📊 Stock Scanner Dashboard")

# --- DATA LOADING FUNCTION ---
@st.cache_data(ttl=300)
def load_latest_json_from_s3():
    """Automatically get the latest sent_alerts_log_*.json file."""
    # Ensure AWS credentials are configured in the environment or Streamlit secrets
    try:
        s3 = boto3.client("s3")
    except Exception as e:
        st.error(f"Could not initialize S3 client. Check AWS credentials setup: {e}")
        return pd.DataFrame()
        
    resp = s3.list_objects_v2(Bucket=S3_BUCKET)

    json_files = [
        obj["Key"] for obj in resp.get("Contents", [])
        if obj["Key"].startswith("sent_alerts_log_") and obj["Key"].endswith(".json")
    ]
    
    if not json_files:
        st.warning("No alert files (sent_alerts_log_*.json) found in the bucket.")
        return pd.DataFrame()

    latest_file = sorted(json_files)[-1]
    st.info(f"Loading latest file: {latest_file}")

    try:
        buf = io.BytesIO()
        s3.download_fileobj(S3_BUCKET, latest_file, buf)
    except Exception as e:
        st.error(f"Error downloading file {latest_file}: {e}")
        return pd.DataFrame()


    if buf.getbuffer().nbytes == 0:
        st.warning(f"File '{latest_file}' is empty. No alerts recorded yet.")
        return pd.DataFrame()
        
    buf.seek(0)
    
    try:
        # The JSON data is expected to be an array of objects
        return pd.read_json(buf)
    except Exception as e:
        st.error(f"Error reading JSON from S3 file: {e}")
        return pd.DataFrame()


# --- MAIN APP LOGIC ---
try:
    df = load_latest_json_from_s3()
    
    if df.empty:
        st.success("✅ Data loaded. No new alerts found in the latest file.")
        # If the dataframe is empty due to warnings in the loader, we just display the success
        # message and stop execution to prevent errors below.
        st.stop() 

    # --- VALIDATE COLUMNS ---
    # Check that the critical filtering columns exist
    critical_cols = ['Signal', 'BrokenLevel']
    missing_cols = [col for col in critical_cols if col not in df.columns]
    
    if missing_cols:
        st.error(f"❌ Alert file is missing critical columns for filtering: {', '.join(missing_cols)}. Cannot proceed with filters.")
        st.dataframe(df, use_container_width=True)
        st.stop()
        
    # --------------------------------
    # --- FILTER IMPLEMENTATION START ---
    # --------------------------------
    
    st.sidebar.header("Filter Alerts")
    
    # 1. Filter for 'Signal' (using the correct capitalization)
    all_signals = df['Signal'].unique().tolist()
    selected_signals = st.sidebar.multiselect(
        'Select Signal Type',
        options=all_signals,
        default=all_signals 
    )

    # 2. Filter for 'BrokenLevel' (using the correct capitalization)
    # Base the available levels on the currently filtered signals
    temp_df = df[df['Signal'].isin(selected_signals)]
    all_levels = temp_df['BrokenLevel'].unique().tolist()
    
    selected_levels = st.sidebar.multiselect(
        'Select Broken Level',
        options=all_levels,
        default=all_levels
    )

    # 3. Apply the filters to the final DataFrame
    df_filtered = df[
        (df['Signal'].isin(selected_signals)) &
        (df['BrokenLevel'].isin(selected_levels))
    ].copy() 

    # --------------------------------
    # --- FILTER IMPLEMENTATION END ---
    # --------------------------------

    # --- Add TradingView Link Column ---
    column_config = {} # Initialize empty config
    
    if 'Name' in df_filtered.columns:
        
        # 1. The TICKER_CORRECTIONS map is defined in the CONFIG section at the top.
        
        # 2. Apply the corrections:
        df_filtered['Cleaned_Name'] = df_filtered['Name'].replace(TICKER_CORRECTIONS)
        
        # 3. Create the full symbol (e.g., "NSE:M_M") using the CLEANED name
        df_filtered['Symbol'] = DEFAULT_EXCHANGE + ":" + df_filtered['Cleaned_Name']
        
        # 4. Generate the link using the corrected 'Symbol'
        df_filtered['TradingView'] = "https://www.tradingview.com/chart/?symbol=" + df_filtered['Symbol'] + "&interval=5"

        st.success(f"Displaying {len(df_filtered)} of {len(df)} total alerts.")
        
        # 5. Configure columns for display
        column_config={
            "Time": st.column_config.TextColumn("Time"),
            "TradingView": st.column_config.LinkColumn(
                "TradingView",
                display_text="Open 5m Chart 📈" 
            ),
            # Hide the helper columns we created
            "Symbol": None, 
            "Cleaned_Name": None
        }

    else:
        st.warning("⚠️ 'Name' column not found. Cannot generate TradingView links.")
        st.success(f"Displaying {len(df_filtered)} of {len(df)} total alerts.")

    
    # Display the filtered data using st.data_editor
    st.data_editor(
        df_filtered, 
        use_container_width=True,
        disabled=True, 
        # Apply the column config
        column_config=column_config
    )
    
except Exception as e:
    st.error(f"❌ An unexpected error occurred: {e}")
    st.stop()
