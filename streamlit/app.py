import streamlit as st
import pandas as pd
import snowflake.connector
import os
from dotenv import load_dotenv
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import plotly.express as px

#load env variables 
load_dotenv()


def _load_private_key_from_env():
    key_path = os.environ.get("SF_PRIVATE_KEY_PATH")
    passphrase = os.environ.get("SF_PRIVATE_KEY_PASSPHRASE")
    if not key_path or not passphrase:
        return None

    with open(key_path, "rb") as f:
        key_data = f.read()

    try:
        # Try PEM first (most common when saved as .pem/.p8)
        p_key = serialization.load_pem_private_key(
            key_data,
            password=passphrase.encode(),
            backend=default_backend(),
        )
    except ValueError:
        # Fallback for DER-formatted PKCS8 keys
        p_key = serialization.load_der_private_key(
            key_data,
            password=passphrase.encode(),
            backend=default_backend(),
        )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return pkb


def get_connection():
    pkb = _load_private_key_from_env()
    common = dict(
        account=os.environ["SF_ACCOUNT"],
        user=os.environ["SF_USER"],
        warehouse=os.environ.get("SF_WAREHOUSE", "COMPUTE_WH"),
        database=os.environ.get("SF_DATABASE", "UBERIDES"),
        schema=os.environ.get("SF_SCHEMA", "ANALYTICS"),
        role=os.environ.get("SF_ROLE"),
    )

    if pkb is not None:
        return snowflake.connector.connect(private_key=pkb, **common)

    # Fallback to password if key auth is not configured
    return snowflake.connector.connect(
        password=os.environ["SF_PASSWORD"],
        **common,
    )

st.set_page_config(page_title="Uberides Analytics", layout="wide")
st.title("🚕 Uberides Analytics Demo")

@st.cache_data(ttl=300)
def load_rolling_data():
    with get_connection() as cn:
        sql = """
        select
            ride_date,
            city,
            rides_30d,
            cancel_rate_30d
        from ANALYTICS.FCT_RIDES_ROLLING_CITY
        where ride_date >= dateadd('month', -6, current_date())
        order by 1, 2
        """
        return pd.read_sql(sql, cn)

df = load_rolling_data()

# ---- Plotly Charts ----
st.subheader("Rides (30-day rolling) — Last 6 Months")
fig_rides = px.line(
    df,
    x="RIDE_DATE",
    y="RIDES_30D",
    color="CITY",
    labels={"RIDE_DATE": "Date", "RIDES_30D": "Rides (30d)"},
)
st.plotly_chart(fig_rides, use_container_width=True)

st.subheader("Cancellation Rate (30-day rolling) — Last 6 Months")
fig_cancel = px.line(
    df,
    x="RIDE_DATE",
    y="CANCEL_RATE_30D",
    color="CITY",
    labels={"RIDE_DATE": "Date", "CANCEL_RATE_30D": "Cancel Rate (30d)"},
)
st.plotly_chart(fig_cancel, use_container_width=True)

# Show underlying data for reference
with st.expander("Show data frame"):
    st.dataframe(df)
