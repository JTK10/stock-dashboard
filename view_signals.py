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

# --- PAGE SETUP ---
st.set_page_config(page_title="Stock Scanner Dashboard", layout="wide")
st.title("📊 Stock Scanner Dashboard")

# --- DATA LOADING FUNCTION ---
@st.cache_data(ttl=300)
def load_latest_json_from_s3():
    """Automatically get the latest sent_alerts_log_*.json file."""
    s3 = boto3.client("s3")
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

    buf = io.BytesIO()
    s3.download_fileobj(S3_BUCKET, latest_file, buf)

    if buf.getbuffer().nbytes == 0:
        st.warning(f"File '{latest_file}' is empty. No alerts recorded yet.")
        return pd.DataFrame()
        
    buf.seek(0)
    # The JSON data is an array of objects, which is correctly read by pd.read_json(buf)
    return pd.read_json(buf)

# --- MAIN APP LOGIC ---
try:
    df = load_latest_json_from_s3()
    
    if df.empty:
        st.success("✅ Data loaded. No new alerts found in the latest file.")
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
    if 'Name' in df_filtered.columns:
        # Create the full symbol (e.g., "NSE:RELIANCE")
        df_filtered['Symbol'] = DEFAULT_EXCHANGE + ":" + df_filtered['Name']
        
        # --- THIS IS THE MODIFIED LINE ---
        # We add "&interval=5" to the end of the URL
        df_filtered['TradingView'] = "https://www.tradingview.com/chart/?symbol=" + df_filtered['Symbol'] + "&interval=5"
        # --- END MODIFICATION ---

    else:
        st.warning("⚠️ 'Name' column not found. Cannot generate TradingView links.")

    st.success(f"Displaying {len(df_filtered)} of {len(df)} total alerts.")
    
    # Display the filtered data using st.data_editor
    st.data_editor(
        df_filtered, 
        use_container_width=True,
        disabled=True, 
        # Configure columns, including the link
        column_config={
            "Time": st.column_config.TextColumn("Time"),
            "TradingView": st.column_config.LinkColumn(
                "TradingView",             # Column header
                display_text="Open 5m Chart 📈" # Updated link text
            ),
            # Optional: Hide the helper 'Symbol' column we created
            "Symbol": None, 
        }
    )
    
except Exception as e:
    st.error(f"❌ An unexpected error occurred: {e}")
    st.stop()
