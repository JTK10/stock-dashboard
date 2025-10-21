import streamlit as st
import pandas as pd
import boto3
import io
import json

# --- CONFIG ---
S3_BUCKET = "jtscanner"

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
    return pd.read_json(buf)

# --- MAIN APP LOGIC ---
try:
    df = load_latest_json_from_s3()
    
    if df.empty:
        st.success("✅ Data loaded. No new alerts found in the latest file.")
        st.stop() 

    # --- FILTER IMPLEMENTATION START ---
    
    st.sidebar.header("Filter Alerts")
    
    # 1. Filter for 'signal'
    all_signals = df['signal'].unique().tolist()
    selected_signals = st.sidebar.multiselect(
        'Select Signal Type',
        options=all_signals,
        default=all_signals 
    )

    # 2. Filter for 'initial_broken_level'
    # Use the filtered signal list to determine the available levels for better user experience
    temp_df = df[df['signal'].isin(selected_signals)]
    all_levels = temp_df['initial_broken_level'].unique().tolist()
    
    selected_levels = st.sidebar.multiselect(
        'Select Initial Broken Level',
        options=all_levels,
        default=all_levels
    )

    # 3. Apply the filters to the final DataFrame
    df_filtered = df[
        (df['signal'].isin(selected_signals)) &
        (df['initial_broken_level'].isin(selected_levels))
    ]

    # --- FILTER IMPLEMENTATION END ---

    st.success(f"Displaying {len(df_filtered)} of {len(df)} total alerts.")
    
    # Display the filtered data using st.data_editor
    st.data_editor(
        df_filtered, 
        use_container_width=True,
        disabled=True, 
        column_config={
            "breakout_time": st.column_config.DatetimeColumn("Time", format="HH:mm:ss")
        }
    )
    
except Exception as e:
    st.error(f"❌ Could not load data from S3: {e}")
    st.stop()
