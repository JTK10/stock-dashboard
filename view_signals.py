import streamlit as st
import pandas as pd
import boto3
import io
import json

S3_BUCKET = "jtscanner"

st.set_page_config(page_title="Stock Scanner Dashboard", layout="wide")
st.title("📊 Stock Scanner Dashboard")

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
        # Return an empty DataFrame if no files are found
        st.warning("No alert files (sent_alerts_log_*.json) found in the bucket.")
        return pd.DataFrame()

    # Pick latest by date
    latest_file = sorted(json_files)[-1]
    st.info(f"Loading latest file: {latest_file}")

    buf = io.BytesIO()
    s3.download_fileobj(S3_BUCKET, latest_file, buf)

    # Handle case where the file might be empty (0 bytes)
    if buf.getbuffer().nbytes == 0:
        st.warning(f"File '{latest_file}' is empty. No alerts recorded yet.")
        return pd.DataFrame()
        
    buf.seek(0)
    
    # This will now correctly read the list of dictionaries
    return pd.read_json(buf)

try:
    df = load_latest_json_from_s3()
    
    # --- THIS IS THE FIX ---
    # After loading, check if the DataFrame is empty and display it
    
    if df.empty:
        st.success("✅ Data loaded. No new alerts found in the latest file.")
    else:
        # Display the data
        st.dataframe(df, use_container_width=True) 
        
except Exception as e:
    st.error(f"❌ Could not load data from S3: {e}")
    st.stop()