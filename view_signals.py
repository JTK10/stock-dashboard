import streamlit as st
import pandas as pd
import boto3
from boto3.dynamodb.conditions import Attr
import os
from decimal import Decimal
from datetime import datetime
import pytz
import yfinance as yf
import altair as alt
from streamlit_autorefresh import st_autorefresh
from streamlit_option_menu import option_menu

# --- 1. PAGE CONFIG & AUTO REFRESH ---
st.set_page_config(page_title="Scanner Dashboard", layout="wide", page_icon="⚡")
count = st_autorefresh(interval=300 * 1000, key="datarefresh")

# --- 2. CUSTOM CSS ---
st.markdown("""
<style>
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

# === TRADINGVIEW & YAHOO MAPPING ===
# This dictionary corrects the DynamoDB names to valid Ticker Symbols.
TICKER_CORRECTIONS = {
    # --- CRITICAL FIXES FROM YOUR SCREENSHOT ---
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
    "HINDALCO INDUSTRIES LTD": "HINDALCO",
    "HINDALCO INDUSTRIES LIMITED": "HINDALCO",
    "HINDALCO": "HINDALCO",
    "HINDALCO  INDUSTRIES  LTD": "HINDALCO",
    # --- COMMON CORRECTIONS ---
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
    "HINDALCO  INDUSTRIES  LTD": "HINDALCO",
    "BAJAJ HOLDINGS & INVS LTD": "3BAJAJHLDNG",  # Matches "INVS" abbreviation
    "WAAREE ENERGIES LIMITED": "WAAREEENER",    # Maps to correct 10-char symbol
    "SWIGGY LIMITED": "SWIGGY"           # Maps to standard symbol,  


}

# --- 3. LOAD DATA FROM DYNAMODB ---
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

# --- 4. LIVE DATA ENGINE (YAHOO) ---
def fetch_live_updates(df):
    if df.empty or 'Name' not in df.columns: return df

    # 1. Clean Names using the Dictionary
    df['Cleaned_Name'] = df['Name'].replace(TICKER_CORRECTIONS)
    
    # 2. Prepare Yahoo Tickers (Append .NS)
    # We use Cleaned_Name to get the Base Symbol
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

        # Map back to DataFrame
        df['Live_Price'] = df['Cleaned_Name'].map(price_map)
        
        # Calc PnL
        df['Live_Move_Pct'] = ((df['Live_Price'] - df['SignalPrice']) / df['SignalPrice']) * 100
        df['Live_Move_Pct'] = df['Live_Move_Pct'].fillna(0.0)
        
    except Exception as e:
        df['Live_Price'] = 0.0
        df['Live_Move_Pct'] = 0.0
        
    return df

# --- 5. SECTOR FETCHING HELPER ---
@st.cache_data(show_spinner=False)
def get_sector_map(clean_ticker_list):
    sector_map = {}
    for ticker in clean_ticker_list:
        try:
            # Use the Yahoo Symbol (.NS) to get info
            yf_ticker = f"{ticker}.NS"
            info = yf.Ticker(yf_ticker).info
            sector = info.get('sector', 'Others')
            
            # Shorten Sector Names
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
        options=["Alpha Stream", "Sector Scope"], 
        icons=["activity", "grid"],
        menu_icon="cast",
        default_index=0,
    )
    st.divider()
    india_tz = pytz.timezone('Asia/Kolkata')
    today_india = datetime.now(india_tz).date()
    selected_date = st.date_input("📅 Select Date", today_india)

# 1. Alpha Stream
if selected == "Alpha Stream":
    st.title("🚀 Alpha Stream")
    df = load_data_from_dynamodb(selected_date)
    
    if df.empty:
        st.info(f"No alerts found for {selected_date}")
        st.stop()

    if selected_date == today_india:
        with st.spinner('⚡ Fetching Live Market Rates...'):
            df = fetch_live_updates(df)
    else:
        df['Live_Price'] = 0.0
        df['Live_Move_Pct'] = 0.0

    # --- TRADINGVIEW LINK GENERATOR (PROTECTED) ---
    if 'Name' in df.columns:
        # 1. Get Clean Symbol
        df['Cleaned_Name'] = df['Name'].replace(TICKER_CORRECTIONS)
        # 2. Create TV Symbol (Replace & with _ for stocks like M&M)
        df['TV_Symbol'] = DEFAULT_EXCHANGE + ":" + df['Cleaned_Name'].str.replace('&', '_').str.replace(' ', '')
        df['Chart'] = "https://www.tradingview.com/chart/?symbol=" + df['TV_Symbol']

    col1, col2 = st.columns(2)

    with col1:
        st.markdown('<div class="dashboard-card">', unsafe_allow_html=True)
        st.subheader("🟢 BULLISH BEACONS")
        bull_df = df[df['Direction'] == 'LONG'].copy()
        if not bull_df.empty:
            bull_df = bull_df.sort_values(by='Live_Move_Pct', ascending=False)
            st.data_editor(
                bull_df[['Chart', 'Name', 'Live_Move_Pct', 'Time', 'RVOL']], 
                column_config={
                    "Chart": st.column_config.LinkColumn("View", display_text="📈"),
                    "Live_Move_Pct": st.column_config.ProgressColumn("%", format="%.2f%%", min_value=-5, max_value=5),
                    "Time": st.column_config.TextColumn("Entry Time"), 
                    "RVOL": st.column_config.NumberColumn("RVOL", format="%.2fx"),
                },
                hide_index=True, use_container_width=True, disabled=True, key="bull_table"
            )
        else:
            st.caption("No Bullish Signals yet.")
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="dashboard-card">', unsafe_allow_html=True)
        st.subheader("🔴 BEARISH DRAGONS")
        bear_df = df[df['Direction'] == 'SHORT'].copy()
        if not bear_df.empty:
            bear_df = bear_df.sort_values(by='Live_Move_Pct', ascending=True)
            st.data_editor(
                bear_df[['Chart', 'Name', 'Live_Move_Pct', 'Time', 'RVOL']], 
                column_config={
                    "Chart": st.column_config.LinkColumn("View", display_text="📉"),
                    "Live_Move_Pct": st.column_config.ProgressColumn("%", format="%.2f%%", min_value=-5, max_value=5),
                    "Time": st.column_config.TextColumn("Entry Time"),
                    "RVOL": st.column_config.NumberColumn("RVOL", format="%.2fx"),
                },
                hide_index=True, use_container_width=True, disabled=True, key="bear_table"
            )
        else:
            st.caption("No Bearish Signals yet.")
        st.markdown('</div>', unsafe_allow_html=True)

# 2. Sector Scope
elif selected == "Sector Scope":
    st.title("🏙️ Sector Scope")
    st.caption("Active Sectors based on alerts")

    df = load_data_from_dynamodb(selected_date)
    if df.empty:
        st.info(f"No alerts found for {selected_date}")
        st.stop()

    if 'Name' in df.columns:
        # Use Corrected Names for Sector Lookup too!
        df['Cleaned_Name'] = df['Name'].replace(TICKER_CORRECTIONS)
        unique_stocks = df['Cleaned_Name'].unique().tolist()
        
        with st.spinner(f"Mapping sectors for {len(unique_stocks)} stocks..."):
            sector_mapping = get_sector_map(unique_stocks)
        
        df['Sector'] = df['Cleaned_Name'].map(sector_mapping)
        sector_counts = df['Sector'].value_counts().reset_index()
        sector_counts.columns = ['Sector', 'Count']

        # GREEN BAR CHART
        chart = alt.Chart(sector_counts).mark_bar(
            color='#2ecc71',
            cornerRadiusTopLeft=5,
            cornerRadiusTopRight=5
        ).encode(
            x=alt.X('Sector', sort='-y', axis=alt.Axis(labelAngle=-90, title=None)),
            y=alt.Y('Count', axis=alt.Axis(title=None, tickMinStep=1)),
            tooltip=['Sector', 'Count']
        ).configure_axis(
            grid=False, labelColor='#eee', domainColor='#333'
        ).configure_view(strokeWidth=0).properties(height=400)

        st.altair_chart(chart, use_container_width=True)

        st.divider()
        st.subheader("Sector Details")
        st.dataframe(
            df[['Name', 'Sector', 'Direction', 'SignalPrice']].sort_values(by='Sector'),
            use_container_width=True, hide_index=True
        )











