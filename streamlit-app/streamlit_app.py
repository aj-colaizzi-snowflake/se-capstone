"""
DK Aviation Flight Insights
Streamlit Application (Local + Streamlit in Snowflake)

A customer-facing data application POC that showcases how DK Aviation's 
consolidated ADS-B + FAA flight data can be used by aircraft operators, 
dispatchers, and analysts to make data-driven decisions.

Supports both:
- Local development (using snowflake-connector-python)
- Streamlit in Snowflake deployment (using snowflake.snowpark.context)
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os

# =============================================================================
# Page Configuration
# =============================================================================
st.set_page_config(
    page_title="DK Aviation Flight Insights",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =============================================================================
# Database Connection - Supports Local and SiS
# =============================================================================
def is_running_in_snowflake():
    """Check if running in Streamlit in Snowflake environment."""
    try:
        from snowflake.snowpark.context import get_active_session
        get_active_session()
        return True
    except:
        return False

@st.cache_resource
def get_snowflake_connection():
    """
    Get Snowflake connection - works both locally and in SiS.
    
    For local development:
    - Uses credentials from .streamlit/secrets.toml
    - Supports both password and key-pair authentication
    
    For Streamlit in Snowflake:
    - Uses the active session context
    """
    if is_running_in_snowflake():
        # Running in Streamlit in Snowflake
        from snowflake.snowpark.context import get_active_session
        return get_active_session(), "sis"
    else:
        # Running locally - use snowflake-connector-python
        import snowflake.connector
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization
        
        # Base connection params
        conn_params = {
            "account": st.secrets["snowflake"]["account"],
            "user": st.secrets["snowflake"]["user"],
            "warehouse": st.secrets["snowflake"]["warehouse"],
            "database": st.secrets["snowflake"]["database"],
            "schema": st.secrets["snowflake"]["schema"],
            "role": st.secrets["snowflake"].get("role", "ACCOUNTADMIN")
        }
        
        # Check for key-pair authentication (preferred)
        if "private_key_path" in st.secrets["snowflake"]:
            # Load private key from file
            private_key_path = os.path.expanduser(st.secrets["snowflake"]["private_key_path"])
            passphrase = st.secrets["snowflake"].get("private_key_passphrase", None)
            
            with open(private_key_path, "rb") as key_file:
                p_key = serialization.load_pem_private_key(
                    key_file.read(),
                    password=passphrase.encode() if passphrase else None,
                    backend=default_backend()
                )
            
            # Get the private key bytes
            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
            conn_params["private_key"] = pkb
        elif "password" in st.secrets["snowflake"] and st.secrets["snowflake"]["password"]:
            # Fall back to password auth
            conn_params["password"] = st.secrets["snowflake"]["password"]
        else:
            raise ValueError("No authentication method configured. Set either 'private_key_path' or 'password' in secrets.toml")
        
        conn = snowflake.connector.connect(**conn_params)
        return conn, "local"

# Get connection
connection, env_type = get_snowflake_connection()

def run_query(query: str) -> pd.DataFrame:
    """Execute a query and return results as DataFrame - works in both environments."""
    if env_type == "sis":
        # Streamlit in Snowflake - use Snowpark session
        return connection.sql(query).to_pandas()
    else:
        # Local - use snowflake-connector-python
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            columns = [col[0] for col in cursor.description]
            data = cursor.fetchall()
            return pd.DataFrame(data, columns=columns)
        finally:
            cursor.close()

# =============================================================================
# Data Access Functions
# =============================================================================
@st.cache_data(ttl=600)
def get_overview_metrics():
    """Get high-level metrics for the dashboard."""
    query = """
    SELECT 
        COUNT(*) as TOTAL_RECORDS,
        COUNT(DISTINCT TAIL_NUMBER) as UNIQUE_AIRCRAFT,
        COUNT(DISTINCT AIRCRAFT_MANUFACTURER) as UNIQUE_MANUFACTURERS,
        COUNT(DISTINCT OWNER_NAME) as UNIQUE_OWNERS,
        MIN(RECORD_TS) as EARLIEST_RECORD,
        MAX(RECORD_TS) as LATEST_RECORD
    FROM CAPSTONE.GOLD.AIRCRAFT_FLIGHT_VW
    """
    return run_query(query)

@st.cache_data(ttl=600)
def get_source_breakdown():
    """Get record counts by data source (KBFI vs KAPA)."""
    query = """
    SELECT 
        SOURCE_TYPE,
        COUNT(*) as RECORD_COUNT,
        COUNT(DISTINCT TAIL_NUMBER) as UNIQUE_AIRCRAFT
    FROM CAPSTONE.GOLD.AIRCRAFT_FLIGHT_VW
    GROUP BY SOURCE_TYPE
    ORDER BY RECORD_COUNT DESC
    """
    return run_query(query)

@st.cache_data(ttl=600)
def get_top_manufacturers(limit: int = 15):
    """Get top aircraft manufacturers by flight activity."""
    query = f"""
    SELECT 
        TRIM(AIRCRAFT_MANUFACTURER) as MANUFACTURER,
        COUNT(*) as FLIGHT_RECORDS,
        COUNT(DISTINCT TAIL_NUMBER) as UNIQUE_AIRCRAFT
    FROM CAPSTONE.GOLD.AIRCRAFT_FLIGHT_VW
    WHERE AIRCRAFT_MANUFACTURER IS NOT NULL
    GROUP BY AIRCRAFT_MANUFACTURER
    ORDER BY FLIGHT_RECORDS DESC
    LIMIT {limit}
    """
    return run_query(query)

@st.cache_data(ttl=300)
def search_aircraft(search_term: str):
    """Search for aircraft by tail number or callsign."""
    query = f"""
    SELECT DISTINCT
        TAIL_NUMBER,
        FLIGHT_CALLSIGN,
        TRIM(AIRCRAFT_MANUFACTURER) as AIRCRAFT_MANUFACTURER,
        TRIM(AIRCRAFT_MODEL) as AIRCRAFT_MODEL,
        AIRCRAFT_YEAR,
        TRIM(ENGINE_MANUFACTURER) as ENGINE_MANUFACTURER,
        TRIM(ENGINE_MODEL) as ENGINE_MODEL,
        TRIM(OWNER_NAME) as OWNER_NAME,
        SOURCE_TYPE
    FROM CAPSTONE.GOLD.AIRCRAFT_FLIGHT_VW
    WHERE UPPER(TAIL_NUMBER) LIKE UPPER('%{search_term}%')
       OR UPPER(FLIGHT_CALLSIGN) LIKE UPPER('%{search_term}%')
    LIMIT 100
    """
    return run_query(query)

@st.cache_data(ttl=300)
def get_aircraft_activity(tail_number: str, limit: int = 100):
    """Get recent flight activity for a specific aircraft."""
    query = f"""
    SELECT 
        RECORD_TS,
        FLIGHT_CALLSIGN,
        LATITUDE,
        LONGITUDE,
        ALTITUDE_BARO,
        GROUND_SPEED,
        TRACK,
        AIR_GROUND_STATUS,
        SOURCE_TYPE
    FROM CAPSTONE.GOLD.AIRCRAFT_FLIGHT_VW
    WHERE TAIL_NUMBER = '{tail_number}'
    ORDER BY RECORD_TS DESC
    LIMIT {limit}
    """
    return run_query(query)

@st.cache_data(ttl=600)
def get_hourly_traffic(date_filter: str = None):
    """Get flight counts by hour for traffic analysis."""
    where_clause = ""
    if date_filter:
        where_clause = f"WHERE DATE(RECORD_TS) = '{date_filter}'"
    
    query = f"""
    SELECT 
        HOUR(RECORD_TS) as HOUR_OF_DAY,
        COUNT(*) as FLIGHT_COUNT,
        COUNT(DISTINCT TAIL_NUMBER) as UNIQUE_AIRCRAFT
    FROM CAPSTONE.GOLD.AIRCRAFT_FLIGHT_VW
    {where_clause}
    GROUP BY HOUR(RECORD_TS)
    ORDER BY HOUR_OF_DAY
    """
    return run_query(query)

@st.cache_data(ttl=600)
def get_daily_traffic(days: int = 30):
    """Get flight counts by day for the last N days."""
    query = f"""
    SELECT 
        DATE(RECORD_TS) as FLIGHT_DATE,
        COUNT(*) as FLIGHT_COUNT,
        COUNT(DISTINCT TAIL_NUMBER) as UNIQUE_AIRCRAFT
    FROM CAPSTONE.GOLD.AIRCRAFT_FLIGHT_VW
    WHERE RECORD_TS >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
    GROUP BY DATE(RECORD_TS)
    ORDER BY FLIGHT_DATE
    """
    return run_query(query)

@st.cache_data(ttl=600)
def get_air_ground_distribution():
    """Get distribution of air vs ground status."""
    query = """
    SELECT 
        AIR_GROUND_STATUS,
        COUNT(*) as RECORD_COUNT
    FROM CAPSTONE.GOLD.AIRCRAFT_FLIGHT_VW
    WHERE AIR_GROUND_STATUS IS NOT NULL
    GROUP BY AIR_GROUND_STATUS
    ORDER BY RECORD_COUNT DESC
    """
    return run_query(query)

@st.cache_data(ttl=300)
def get_recent_flights(limit: int = 1000):
    """Get recent flight positions for map visualization."""
    query = f"""
    SELECT 
        TAIL_NUMBER,
        FLIGHT_CALLSIGN,
        LATITUDE,
        LONGITUDE,
        ALTITUDE_BARO,
        GROUND_SPEED,
        TRIM(AIRCRAFT_MANUFACTURER) as AIRCRAFT_MANUFACTURER,
        TRIM(AIRCRAFT_MODEL) as AIRCRAFT_MODEL,
        AIR_GROUND_STATUS,
        RECORD_TS
    FROM CAPSTONE.GOLD.AIRCRAFT_FLIGHT_VW
    WHERE LATITUDE IS NOT NULL 
      AND LONGITUDE IS NOT NULL
      AND AIR_GROUND_STATUS = 'AIR'
    ORDER BY RECORD_TS DESC
    LIMIT {limit}
    """
    return run_query(query)

@st.cache_data(ttl=600)
def get_top_aircraft(limit: int = 10):
    """Get most active aircraft by flight record count."""
    query = f"""
    SELECT 
        TAIL_NUMBER,
        TRIM(AIRCRAFT_MANUFACTURER) as AIRCRAFT_MANUFACTURER,
        TRIM(AIRCRAFT_MODEL) as AIRCRAFT_MODEL,
        TRIM(OWNER_NAME) as OWNER_NAME,
        COUNT(*) as RECORD_COUNT,
        MAX(RECORD_TS) as LAST_SEEN
    FROM CAPSTONE.GOLD.AIRCRAFT_FLIGHT_VW
    WHERE TAIL_NUMBER IS NOT NULL
    GROUP BY TAIL_NUMBER, AIRCRAFT_MANUFACTURER, AIRCRAFT_MODEL, OWNER_NAME
    ORDER BY RECORD_COUNT DESC
    LIMIT {limit}
    """
    return run_query(query)

@st.cache_data(ttl=600)
def get_manufacturer_list():
    """Get list of all manufacturers with aircraft counts."""
    query = """
    SELECT 
        TRIM(AIRCRAFT_MANUFACTURER) as MANUFACTURER,
        COUNT(DISTINCT TAIL_NUMBER) as AIRCRAFT_COUNT
    FROM CAPSTONE.GOLD.AIRCRAFT_FLIGHT_VW
    WHERE AIRCRAFT_MANUFACTURER IS NOT NULL
    GROUP BY AIRCRAFT_MANUFACTURER
    HAVING COUNT(DISTINCT TAIL_NUMBER) > 0
    ORDER BY AIRCRAFT_COUNT DESC
    """
    return run_query(query)

@st.cache_data(ttl=300)
def get_aircraft_by_manufacturer(manufacturer: str):
    """Get all aircraft for a specific manufacturer."""
    query = f"""
    SELECT DISTINCT
        TAIL_NUMBER,
        TRIM(AIRCRAFT_MODEL) as AIRCRAFT_MODEL,
        AIRCRAFT_YEAR,
        TRIM(OWNER_NAME) as OWNER_NAME,
        COUNT(*) as RECORD_COUNT
    FROM CAPSTONE.GOLD.AIRCRAFT_FLIGHT_VW
    WHERE TRIM(AIRCRAFT_MANUFACTURER) = '{manufacturer}'
    GROUP BY TAIL_NUMBER, AIRCRAFT_MODEL, AIRCRAFT_YEAR, OWNER_NAME
    ORDER BY RECORD_COUNT DESC
    LIMIT 50
    """
    return run_query(query)

# =============================================================================
# Operational Intelligence Functions
# =============================================================================
@st.cache_data(ttl=120)
def get_current_hour_stats():
    """Get current hour traffic compared to historical average for same hour."""
    query = """
    WITH current_hour AS (
        SELECT 
            COUNT(*) as current_count,
            COUNT(DISTINCT TAIL_NUMBER) as current_aircraft,
            HOUR(CURRENT_TIMESTAMP()) as hour_of_day
        FROM CAPSTONE.GOLD.AIRCRAFT_FLIGHT_VW
        WHERE RECORD_TS >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
    ),
    historical_avg AS (
        SELECT 
            hour_of_day,
            AVG(daily_count) as avg_count,
            AVG(daily_aircraft) as avg_aircraft
        FROM (
            SELECT 
                DATE(RECORD_TS) as record_date,
                HOUR(RECORD_TS) as hour_of_day,
                COUNT(*) as daily_count,
                COUNT(DISTINCT TAIL_NUMBER) as daily_aircraft
            FROM CAPSTONE.GOLD.AIRCRAFT_FLIGHT_VW
            GROUP BY DATE(RECORD_TS), HOUR(RECORD_TS)
        ) daily_stats
        GROUP BY hour_of_day
    )
    SELECT 
        c.current_count,
        c.current_aircraft,
        c.hour_of_day,
        COALESCE(h.avg_count, 0) as historical_avg_count,
        COALESCE(h.avg_aircraft, 0) as historical_avg_aircraft,
        CASE 
            WHEN h.avg_count > 0 THEN ROUND((c.current_count - h.avg_count) / h.avg_count * 100, 1)
            ELSE 0 
        END as pct_vs_average
    FROM current_hour c
    LEFT JOIN historical_avg h ON c.hour_of_day = h.hour_of_day
    """
    return run_query(query)

@st.cache_data(ttl=600)
def get_traffic_by_day_of_week():
    """Get traffic patterns by day of week."""
    query = """
    SELECT 
        DAYOFWEEK(RECORD_TS) as day_num,
        DAYNAME(RECORD_TS) as day_name,
        COUNT(*) as total_records,
        COUNT(DISTINCT TAIL_NUMBER) as unique_aircraft,
        COUNT(DISTINCT DATE(RECORD_TS)) as num_days,
        ROUND(COUNT(*) / COUNT(DISTINCT DATE(RECORD_TS)), 0) as avg_daily_records
    FROM CAPSTONE.GOLD.AIRCRAFT_FLIGHT_VW
    GROUP BY DAYOFWEEK(RECORD_TS), DAYNAME(RECORD_TS)
    ORDER BY day_num
    """
    return run_query(query)

@st.cache_data(ttl=300)
def get_optimal_flight_windows():
    """Identify optimal (low traffic) flight windows based on historical hourly patterns."""
    query = """
    WITH hourly_stats AS (
        SELECT 
            HOUR(RECORD_TS) as hour_of_day,
            COUNT(*) as total_records,
            COUNT(DISTINCT DATE(RECORD_TS)) as num_days,
            ROUND(COUNT(*) / COUNT(DISTINCT DATE(RECORD_TS)), 0) as avg_hourly_traffic
        FROM CAPSTONE.GOLD.AIRCRAFT_FLIGHT_VW
        GROUP BY HOUR(RECORD_TS)
    ),
    traffic_percentiles AS (
        SELECT 
            hour_of_day,
            avg_hourly_traffic,
            PERCENT_RANK() OVER (ORDER BY avg_hourly_traffic) as traffic_percentile
        FROM hourly_stats
    )
    SELECT 
        hour_of_day,
        avg_hourly_traffic,
        traffic_percentile,
        CASE 
            WHEN traffic_percentile <= 0.25 THEN 'LOW'
            WHEN traffic_percentile <= 0.75 THEN 'MODERATE'
            ELSE 'HIGH'
        END as congestion_level
    FROM traffic_percentiles
    ORDER BY hour_of_day
    """
    return run_query(query)

@st.cache_data(ttl=120)
def get_recent_activity_summary():
    """Get summary of recent flight activity for operations monitoring."""
    query = """
    SELECT 
        COUNT(*) as records_last_hour,
        COUNT(DISTINCT TAIL_NUMBER) as aircraft_last_hour,
        SUM(CASE WHEN AIR_GROUND_STATUS = 'AIR' THEN 1 ELSE 0 END) as airborne_records,
        SUM(CASE WHEN AIR_GROUND_STATUS = 'GROUND' THEN 1 ELSE 0 END) as ground_records,
        MAX(RECORD_TS) as last_record_time
    FROM CAPSTONE.GOLD.AIRCRAFT_FLIGHT_VW
    WHERE RECORD_TS >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
    """
    return run_query(query)

@st.cache_data(ttl=120)
def get_currently_active_aircraft(limit: int = 20):
    """Get aircraft with recent activity (proxy for currently active)."""
    query = f"""
    SELECT 
        TAIL_NUMBER,
        FLIGHT_CALLSIGN,
        TRIM(AIRCRAFT_MANUFACTURER) as MANUFACTURER,
        TRIM(AIRCRAFT_MODEL) as MODEL,
        MAX(ALTITUDE_BARO) as LAST_ALTITUDE,
        MAX(GROUND_SPEED) as LAST_SPEED,
        MAX(AIR_GROUND_STATUS) as STATUS,
        MAX(RECORD_TS) as LAST_SEEN
    FROM CAPSTONE.GOLD.AIRCRAFT_FLIGHT_VW
    WHERE RECORD_TS >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
    GROUP BY TAIL_NUMBER, FLIGHT_CALLSIGN, AIRCRAFT_MANUFACTURER, AIRCRAFT_MODEL
    ORDER BY LAST_SEEN DESC
    LIMIT {limit}
    """
    return run_query(query)

@st.cache_data(ttl=600)
def get_activity_trend_24h():
    """Get hourly activity trend for last 24 hours."""
    query = """
    SELECT 
        DATE_TRUNC('hour', RECORD_TS) as hour_bucket,
        COUNT(*) as record_count,
        COUNT(DISTINCT TAIL_NUMBER) as unique_aircraft
    FROM CAPSTONE.GOLD.AIRCRAFT_FLIGHT_VW
    WHERE RECORD_TS >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
    GROUP BY DATE_TRUNC('hour', RECORD_TS)
    ORDER BY hour_bucket
    """
    return run_query(query)

@st.cache_data(ttl=600)
def get_pipeline_health():
    """Get data pipeline health metrics (KBFI vs KAPA source distribution and freshness)."""
    query = """
    SELECT 
        SOURCE_TYPE,
        COUNT(*) as total_records,
        COUNT(DISTINCT TAIL_NUMBER) as unique_aircraft,
        MIN(RECORD_TS) as earliest_record,
        MAX(RECORD_TS) as latest_record,
        DATEDIFF(minute, MAX(RECORD_TS), CURRENT_TIMESTAMP()) as minutes_since_last
    FROM CAPSTONE.GOLD.AIRCRAFT_FLIGHT_VW
    GROUP BY SOURCE_TYPE
    """
    return run_query(query)

# =============================================================================
# Design System
# =============================================================================
# Color palette
COLORS = {
    'background': '#0A0A0B',
    'surface': '#141416',
    'border': '#27272A',
    'muted': '#71717A',
    'foreground': '#FAFAFA',
    'accent': '#F59E0B',
    'accent_muted': '#D97706',
    'success': '#22C55E',
    'info': '#3B82F6',
}

# Chart color scales
CHART_COLORS = ['#F59E0B', '#D97706', '#B45309', '#92400E', '#78350F']
CHART_COLORSCALE = [[0, '#3B82F6'], [0.5, '#F59E0B'], [1, '#DC2626']]

st.markdown("""
<style>
    /* Import Plus Jakarta Sans */
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap');
    
    /* Global Typography */
    html, body, [class*="css"] {
        font-family: 'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    }
    
    /* Page Background */
    .stApp {
        background-color: #0A0A0B;
    }
    
    /* Main content area */
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1200px;
    }
    
    /* Page Header Styling */
    .page-header {
        margin-bottom: 2rem;
    }
    .page-title {
        font-family: 'Plus Jakarta Sans', sans-serif;
        font-size: 2rem;
        font-weight: 600;
        color: #FAFAFA;
        margin: 0 0 0.5rem 0;
        letter-spacing: -0.02em;
    }
    .page-subtitle {
        font-size: 1rem;
        color: #71717A;
        margin: 0;
        font-weight: 400;
    }
    
    /* Section Headers */
    .section-header {
        font-family: 'Plus Jakarta Sans', sans-serif;
        font-size: 1.125rem;
        font-weight: 600;
        color: #FAFAFA;
        margin: 1.5rem 0 1rem 0;
        padding-bottom: 0.5rem;
        border-bottom: 1px solid #27272A;
    }
    
    /* Metric Cards */
    [data-testid="stMetric"] {
        background-color: #141416;
        border: 1px solid #27272A;
        border-radius: 8px;
        padding: 1rem 1.25rem;
        border-left: 3px solid #F59E0B;
    }
    [data-testid="stMetricLabel"] {
        font-size: 0.75rem;
        font-weight: 500;
        color: #71717A;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    [data-testid="stMetricValue"] {
        font-family: 'Plus Jakarta Sans', sans-serif;
        font-size: 1.75rem;
        font-weight: 600;
        color: #FAFAFA;
    }
    [data-testid="stMetricDelta"] {
        font-size: 0.8rem;
        color: #71717A;
    }
    [data-testid="stMetricDelta"] svg {
        display: none;
    }
    
    /* Sidebar Styling */
    [data-testid="stSidebar"] {
        background-color: #0A0A0B;
        border-right: 1px solid #27272A;
    }
    [data-testid="stSidebar"] .block-container {
        padding-top: 2rem;
    }
    
    /* Sidebar Brand */
    .sidebar-brand {
        padding: 0 1rem 1.5rem 1rem;
        border-bottom: 1px solid #27272A;
        margin-bottom: 1.5rem;
    }
    .sidebar-brand-name {
        font-family: 'Plus Jakarta Sans', sans-serif;
        font-size: 1rem;
        font-weight: 700;
        color: #FAFAFA;
        letter-spacing: 0.1em;
        margin: 0;
    }
    .sidebar-brand-tagline {
        font-size: 0.8rem;
        color: #71717A;
        margin: 0.25rem 0 0 0;
        font-weight: 400;
    }
    
    /* Environment Badge */
    .env-badge {
        display: inline-flex;
        align-items: center;
        padding: 0.25rem 0.75rem;
        border-radius: 9999px;
        font-size: 0.7rem;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin: 1rem 0;
    }
    .env-badge.local {
        background-color: rgba(34, 197, 94, 0.1);
        color: #22C55E;
        border: 1px solid rgba(34, 197, 94, 0.2);
    }
    .env-badge.sis {
        background-color: rgba(59, 130, 246, 0.1);
        color: #3B82F6;
        border: 1px solid rgba(59, 130, 246, 0.2);
    }
    
    /* Navigation Radio Buttons */
    [data-testid="stSidebar"] [data-testid="stRadio"] > label {
        display: none;
    }
    [data-testid="stSidebar"] [data-testid="stRadio"] > div {
        gap: 0.25rem;
    }
    [data-testid="stSidebar"] [data-testid="stRadio"] label[data-baseweb="radio"] {
        background-color: transparent;
        padding: 0.75rem 1rem;
        border-radius: 6px;
        margin: 0;
        transition: all 0.15s ease;
    }
    [data-testid="stSidebar"] [data-testid="stRadio"] label[data-baseweb="radio"]:hover {
        background-color: #141416;
    }
    [data-testid="stSidebar"] [data-testid="stRadio"] label[data-baseweb="radio"][aria-checked="true"] {
        background-color: #141416;
        border-left: 2px solid #F59E0B;
    }
    [data-testid="stSidebar"] [data-testid="stRadio"] label[data-baseweb="radio"] p {
        font-size: 0.9rem;
        font-weight: 500;
        color: #FAFAFA;
    }
    [data-testid="stSidebar"] [data-testid="stRadio"] label[data-baseweb="radio"] div[data-testid="stMarkdownContainer"] {
        margin-left: 0;
    }
    /* Hide radio circle */
    [data-testid="stSidebar"] [data-testid="stRadio"] div[role="radiogroup"] > label > div:first-child {
        display: none;
    }
    
    /* Sidebar Footer */
    .sidebar-footer {
        padding: 1rem;
        border-top: 1px solid #27272A;
        margin-top: 2rem;
    }
    .sidebar-footer-text {
        font-size: 0.7rem;
        color: #52525B;
        margin: 0;
    }
    
    /* Data Tables */
    [data-testid="stDataFrame"] {
        border: 1px solid #27272A;
        border-radius: 8px;
        overflow: hidden;
    }
    [data-testid="stDataFrame"] table {
        border: none;
    }
    
    /* Input Fields */
    [data-testid="stTextInput"] input {
        background-color: #141416;
        border: 1px solid #27272A;
        border-radius: 6px;
        color: #FAFAFA;
        font-size: 0.9rem;
    }
    [data-testid="stTextInput"] input:focus {
        border-color: #F59E0B;
        box-shadow: 0 0 0 1px #F59E0B;
    }
    [data-testid="stTextInput"] label {
        color: #71717A;
        font-size: 0.85rem;
        font-weight: 500;
    }
    
    /* Select Box */
    [data-testid="stSelectbox"] label {
        color: #71717A;
        font-size: 0.85rem;
        font-weight: 500;
    }
    
    /* Slider */
    [data-testid="stSlider"] label {
        color: #71717A;
        font-size: 0.85rem;
        font-weight: 500;
    }
    
    /* Alerts/Info boxes */
    [data-testid="stAlert"] {
        background-color: #141416;
        border: 1px solid #27272A;
        border-radius: 6px;
    }
    
    /* Caption text */
    .stCaption {
        color: #52525B;
        font-size: 0.8rem;
    }
    
    /* Insight box */
    .insight-box {
        background-color: #141416;
        border: 1px solid #27272A;
        border-left: 3px solid #F59E0B;
        border-radius: 6px;
        padding: 1rem;
        margin: 1rem 0;
    }
    .insight-box p {
        margin: 0;
        color: #FAFAFA;
        font-size: 0.9rem;
    }
    
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* Divider styling */
    hr {
        border-color: #27272A;
        margin: 1.5rem 0;
    }
</style>
""", unsafe_allow_html=True)

# =============================================================================
# Navigation
# =============================================================================
# Sidebar brand
st.sidebar.markdown("""
<div class="sidebar-brand">
    <p class="sidebar-brand-name">DK AVIATION</p>
    <p class="sidebar-brand-tagline">Flight Insights</p>
</div>
""", unsafe_allow_html=True)

# Environment indicator
if env_type == "local":
    st.sidebar.markdown('<span class="env-badge local">Local Environment</span>', unsafe_allow_html=True)
else:
    st.sidebar.markdown('<span class="env-badge sis">Snowflake Cloud</span>', unsafe_allow_html=True)

# Navigation
page = st.sidebar.radio(
    "Navigate",
    ["Operations Hub", "Fleet Overview", "Aircraft Lookup", "Traffic Analysis", "Flight Map"],
    label_visibility="collapsed"
)

# Sidebar footer
st.sidebar.markdown("""
<div class="sidebar-footer">
    <p class="sidebar-footer-text">ADS-B (KBFI & KAPA) + FAA Registry</p>
    <p class="sidebar-footer-text" style="margin-top: 0.25rem;">© 2024 DK Aviation</p>
</div>
""", unsafe_allow_html=True)

# =============================================================================
# Helper: Page Header
# =============================================================================
def render_page_header(title: str, subtitle: str):
    """Render a consistent page header."""
    st.markdown(f"""
    <div class="page-header">
        <h1 class="page-title">{title}</h1>
        <p class="page-subtitle">{subtitle}</p>
    </div>
    """, unsafe_allow_html=True)

def render_section_header(title: str):
    """Render a consistent section header."""
    st.markdown(f'<h2 class="section-header">{title}</h2>', unsafe_allow_html=True)

def render_insight(text: str):
    """Render an insight box."""
    st.markdown(f'<div class="insight-box"><p>{text}</p></div>', unsafe_allow_html=True)

def render_status_indicator(status: str, label: str):
    """Render a traffic status indicator."""
    colors = {
        'LOW': ('#22C55E', 'rgba(34, 197, 94, 0.1)'),
        'MODERATE': ('#F59E0B', 'rgba(245, 158, 11, 0.1)'),
        'HIGH': ('#DC2626', 'rgba(220, 38, 38, 0.1)')
    }
    color, bg = colors.get(status, ('#71717A', 'rgba(113, 113, 122, 0.1)'))
    st.markdown(f"""
<div style="background-color: {bg}; border: 1px solid {color}33; border-radius: 8px; padding: 1.25rem; text-align: center;">
<p style="color: {color}; font-weight: 700; margin: 0; font-size: 1.5rem;">{status}</p>
<p style="color: #71717A; margin: 0.25rem 0 0 0; font-size: 0.85rem;">{label}</p>
</div>
""", unsafe_allow_html=True)

# =============================================================================
# Page: Operations Hub
# =============================================================================
if page == "Operations Hub":
    render_page_header("Operations Hub", "Real-time operational intelligence for trip planning and delay mitigation")
    
    # Load all operational data
    with st.spinner("Loading operational data..."):
        current_stats = get_current_hour_stats()
        recent_activity = get_recent_activity_summary()
        optimal_windows = get_optimal_flight_windows()
        active_aircraft = get_currently_active_aircraft(15)
        activity_trend = get_activity_trend_24h()
    
    # Current Status Panel
    render_section_header("Current Conditions")
    
    status_col1, status_col2, status_col3 = st.columns(3)
    
    with status_col1:
        # Determine current traffic level
        if not current_stats.empty:
            pct_change = current_stats['pct_vs_average'].iloc[0]
            if pct_change > 20:
                traffic_status = "HIGH"
                status_desc = f"{pct_change:.0f}% above normal"
            elif pct_change < -20:
                traffic_status = "LOW"
                status_desc = f"{abs(pct_change):.0f}% below normal"
            else:
                traffic_status = "MODERATE"
                status_desc = "Normal levels"
            
            render_status_indicator(traffic_status, f"Traffic Level • {status_desc}")
        else:
            render_status_indicator("MODERATE", "Traffic Level • Calculating...")
    
    with status_col2:
        if not recent_activity.empty:
            aircraft_count = recent_activity['aircraft_last_hour'].iloc[0]
            st.metric(
                label="Active Aircraft",
                value=f"{aircraft_count:,.0f}",
                delta="Last hour"
            )
        else:
            st.metric(label="Active Aircraft", value="--", delta="Last hour")
    
    with status_col3:
        if not recent_activity.empty:
            last_record = recent_activity['last_record_time'].iloc[0]
            st.metric(
                label="Data Freshness",
                value="Live",
                delta=f"Last update: {last_record}"
            )
        else:
            st.metric(label="Data Freshness", value="--", delta="Checking...")
    
    # Quick Recommendation
    if not optimal_windows.empty:
        from datetime import datetime
        current_hour = datetime.utcnow().hour
        
        # Find current hour's congestion level
        current_congestion = optimal_windows[optimal_windows['hour_of_day'] == current_hour]
        if not current_congestion.empty:
            level = current_congestion['congestion_level'].iloc[0]
            
            # Find next low-traffic window
            low_windows = optimal_windows[optimal_windows['congestion_level'] == 'LOW']['hour_of_day'].tolist()
            future_low = [h for h in low_windows if h > current_hour]
            next_low = future_low[0] if future_low else (low_windows[0] if low_windows else None)
            
            if level == 'HIGH' and next_low is not None:
                render_insight(f"Current hour shows HIGH traffic. Consider delaying departure to {next_low:02d}:00 UTC for lighter conditions.")
            elif level == 'LOW':
                render_insight(f"Current conditions are favorable for departure — traffic is below average for this time of day.")
            else:
                render_insight(f"Traffic is at moderate levels. Operations should proceed normally.")
    
    # Two column layout: Departure Windows + Active Aircraft
    col_left, col_right = st.columns([1, 1])
    
    with col_left:
        render_section_header("Today's Departure Windows")
        
        if not optimal_windows.empty:
            # Color-coded hour display
            congestion_colors = {'LOW': '#22C55E', 'MODERATE': '#F59E0B', 'HIGH': '#DC2626'}
            
            fig = go.Figure()
            
            for _, row in optimal_windows.iterrows():
                fig.add_trace(go.Bar(
                    x=[row['hour_of_day']],
                    y=[row['avg_hourly_traffic']],
                    marker_color=congestion_colors[row['congestion_level']],
                    name=row['congestion_level'],
                    showlegend=False,
                    hovertemplate=f"{int(row['hour_of_day']):02d}:00 UTC<br>Traffic: {row['avg_hourly_traffic']:,.0f}<br>Level: {row['congestion_level']}<extra></extra>"
                ))
            
            fig.update_layout(
                height=250,
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font=dict(family="Plus Jakarta Sans, sans-serif", color='#FAFAFA'),
                xaxis=dict(
                    tickmode='linear',
                    tick0=0,
                    dtick=3,
                    gridcolor='#27272A',
                    title=None
                ),
                yaxis=dict(gridcolor='#27272A', title=None, showticklabels=False),
                margin=dict(l=0, r=0, t=10, b=30),
                bargap=0.1
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Legend
            st.markdown("""
<div style="display: flex; gap: 1.5rem; justify-content: center; margin-top: 0.5rem;">
<span style="color: #22C55E; font-size: 0.8rem;">● Low Traffic</span>
<span style="color: #F59E0B; font-size: 0.8rem;">● Moderate</span>
<span style="color: #DC2626; font-size: 0.8rem;">● High Traffic</span>
</div>
""", unsafe_allow_html=True)
    
    with col_right:
        render_section_header("Active Fleet")
        
        if not active_aircraft.empty:
            st.dataframe(
                active_aircraft[['TAIL_NUMBER', 'MANUFACTURER', 'STATUS', 'LAST_ALTITUDE']].head(10),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "TAIL_NUMBER": "Aircraft",
                    "MANUFACTURER": "Manufacturer",
                    "STATUS": "Status",
                    "LAST_ALTITUDE": st.column_config.NumberColumn("Alt (ft)", format="%d")
                },
                height=280
            )
        else:
            st.caption("No recent aircraft activity detected.")
    
    # 24-Hour Activity Trend
    render_section_header("24-Hour Activity Trend")
    
    if not activity_trend.empty:
        fig = px.area(
            activity_trend,
            x='hour_bucket',
            y='unique_aircraft',
            labels={
                'hour_bucket': 'Time',
                'unique_aircraft': 'Active Aircraft'
            }
        )
        fig.update_traces(
            fill='tozeroy',
            line_color='#F59E0B',
            fillcolor='rgba(245, 158, 11, 0.2)'
        )
        fig.update_layout(
            height=200,
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(family="Plus Jakarta Sans, sans-serif", color='#FAFAFA'),
            xaxis=dict(gridcolor='#27272A', title=None),
            yaxis=dict(gridcolor='#27272A', title=None),
            margin=dict(l=0, r=0, t=10, b=0),
            showlegend=False
        )
        st.plotly_chart(fig, use_container_width=True)

# =============================================================================
# Page: Fleet Overview
# =============================================================================
elif page == "Fleet Overview":
    render_page_header("Fleet Overview", "Real-time visibility into aircraft operations and data pipeline health")
    
    # Key Metrics
    with st.spinner("Loading metrics..."):
        metrics = get_overview_metrics()
        current_stats = get_current_hour_stats()
        
    if not metrics.empty:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="Total Flight Records",
                value=f"{metrics['TOTAL_RECORDS'].iloc[0]:,.0f}"
            )
        with col2:
            st.metric(
                label="Unique Aircraft Tracked",
                value=f"{metrics['UNIQUE_AIRCRAFT'].iloc[0]:,.0f}"
            )
        with col3:
            st.metric(
                label="Manufacturers",
                value=f"{metrics['UNIQUE_MANUFACTURERS'].iloc[0]:,.0f}"
            )
        with col4:
            st.metric(
                label="Registered Owners",
                value=f"{metrics['UNIQUE_OWNERS'].iloc[0]:,.0f}"
            )
        
        st.caption(f"Data coverage: {metrics['EARLIEST_RECORD'].iloc[0]} to {metrics['LATEST_RECORD'].iloc[0]}")
        
        # Current Activity Insight
        if not current_stats.empty and current_stats['current_count'].iloc[0] > 0:
            pct_change = current_stats['pct_vs_average'].iloc[0]
            current_aircraft = current_stats['current_aircraft'].iloc[0]
            if pct_change > 10:
                render_insight(f"Current Activity: {current_aircraft:,.0f} aircraft active in the last hour — {pct_change:+.0f}% above typical for this time of day")
            elif pct_change < -10:
                render_insight(f"Current Activity: {current_aircraft:,.0f} aircraft active in the last hour — {abs(pct_change):.0f}% below typical for this time of day")
            else:
                render_insight(f"Current Activity: {current_aircraft:,.0f} aircraft active in the last hour — normal activity levels for this time of day")
    
    # Two column layout for charts
    col_left, col_right = st.columns([2, 1])
    
    with col_left:
        render_section_header("Market Activity by Manufacturer")
        st.caption("Identify which manufacturers dominate your operational airspace")
        with st.spinner("Loading manufacturer data..."):
            mfr_data = get_top_manufacturers(15)
        
        if not mfr_data.empty:
            # Top manufacturer insight
            top_mfr = mfr_data.iloc[0]
            top_mfr_pct = (top_mfr['FLIGHT_RECORDS'] / mfr_data['FLIGHT_RECORDS'].sum()) * 100
            
            fig = px.bar(
                mfr_data,
                x='FLIGHT_RECORDS',
                y='MANUFACTURER',
                orientation='h',
                color='UNIQUE_AIRCRAFT',
                color_continuous_scale=[[0, '#27272A'], [0.5, '#F59E0B'], [1, '#DC2626']],
                labels={
                    'FLIGHT_RECORDS': 'Flight Records',
                    'MANUFACTURER': 'Manufacturer',
                    'UNIQUE_AIRCRAFT': 'Unique Aircraft'
                }
            )
            fig.update_layout(
                height=450,
                showlegend=False,
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font=dict(family="Plus Jakarta Sans, sans-serif", color='#FAFAFA'),
                xaxis=dict(gridcolor='#27272A', zerolinecolor='#27272A'),
                yaxis=dict(categoryorder='total ascending', gridcolor='#27272A'),
                coloraxis_colorbar=dict(
                    title=dict(text="Aircraft", font=dict(color='#71717A')),
                    tickfont=dict(color='#71717A')
                ),
                margin=dict(l=0, r=0, t=10, b=0)
            )
            st.plotly_chart(fig, use_container_width=True)
            
            render_insight(f"Market Leader: {top_mfr['MANUFACTURER']} accounts for {top_mfr_pct:.0f}% of tracked activity with {top_mfr['UNIQUE_AIRCRAFT']:,.0f} unique aircraft")
    
    with col_right:
        render_section_header("Pipeline Health")
        st.caption("Data ingestion from receiver firmware versions")
        
        with st.spinner("Loading pipeline metrics..."):
            pipeline_data = get_pipeline_health()
        
        if not pipeline_data.empty:
            fig = px.pie(
                pipeline_data,
                values='total_records',
                names='SOURCE_TYPE',
                color_discrete_sequence=['#F59E0B', '#3B82F6'],
                hole=0.5
            )
            fig.update_layout(
                height=240,
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(family="Plus Jakarta Sans, sans-serif", color='#FAFAFA'),
                legend=dict(
                    font=dict(color='#71717A'),
                    orientation='h',
                    yanchor='bottom',
                    y=-0.3
                ),
                margin=dict(l=0, r=0, t=10, b=40),
                showlegend=True
            )
            fig.update_traces(
                textfont=dict(color='#FAFAFA'),
                marker=dict(line=dict(color='#0A0A0B', width=2))
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Pipeline status metrics
            for _, row in pipeline_data.iterrows():
                freshness = row['minutes_since_last']
                status = "streaming" if freshness < 60 else f"{freshness:.0f}m ago"
                st.metric(
                    label=f"{row['SOURCE_TYPE']} ({row['unique_aircraft']:,.0f} aircraft)",
                    value=f"{row['total_records']:,.0f}",
                    delta=status
                )
            
            # Pipeline health insight
            all_fresh = all(pipeline_data['minutes_since_last'] < 60)
            if all_fresh:
                render_insight("Pipeline Status: All receiver firmware formats streaming data successfully")

# =============================================================================
# Page: Aircraft Lookup
# =============================================================================
elif page == "Aircraft Lookup":
    render_page_header("Aircraft Lookup", "Find and explore aircraft in the fleet")
    
    # Initialize session state for selected aircraft
    if 'selected_tail' not in st.session_state:
        st.session_state.selected_tail = None
    
    # Two columns: Discovery (left) and Search (right)
    col_browse, col_search = st.columns([1, 1])
    
    with col_browse:
        render_section_header("Browse by Manufacturer")
        
        # Load manufacturers
        with st.spinner("Loading manufacturers..."):
            manufacturers = get_manufacturer_list()
        
        if not manufacturers.empty:
            # Create dropdown options with aircraft counts
            mfr_options = ["Select a manufacturer..."] + [
                f"{row['MANUFACTURER']} ({row['AIRCRAFT_COUNT']:,.0f})" 
                for _, row in manufacturers.iterrows()
            ]
            
            selected_mfr_display = st.selectbox(
                "Manufacturer",
                options=mfr_options,
                label_visibility="collapsed"
            )
            
            if selected_mfr_display != "Select a manufacturer...":
                # Extract manufacturer name (remove count)
                selected_mfr = selected_mfr_display.rsplit(' (', 1)[0]
                
                with st.spinner("Loading aircraft..."):
                    mfr_aircraft = get_aircraft_by_manufacturer(selected_mfr)
                
                if not mfr_aircraft.empty:
                    st.caption(f"{len(mfr_aircraft)} aircraft from {selected_mfr}")
                    
                    # Show aircraft list as clickable dataframe
                    st.dataframe(
                        mfr_aircraft[['TAIL_NUMBER', 'AIRCRAFT_MODEL', 'OWNER_NAME', 'RECORD_COUNT']],
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "TAIL_NUMBER": "Tail Number",
                            "AIRCRAFT_MODEL": "Model",
                            "OWNER_NAME": "Owner",
                            "RECORD_COUNT": st.column_config.NumberColumn("Records", format="%d")
                        },
                        height=200
                    )
                    
                    # Quick select from this manufacturer
                    aircraft_from_mfr = mfr_aircraft['TAIL_NUMBER'].tolist()
                    quick_select = st.selectbox(
                        "Quick select",
                        options=["Choose aircraft..."] + aircraft_from_mfr,
                        label_visibility="collapsed",
                        key="mfr_select"
                    )
                    if quick_select != "Choose aircraft...":
                        st.session_state.selected_tail = quick_select
    
    with col_search:
        render_section_header("Search")
        
    search_term = st.text_input(
            "Search",
            placeholder="Tail number or callsign (e.g., N12345)",
            help="Search is case-insensitive and supports partial matches",
            label_visibility="collapsed"
    )
    
    if search_term and len(search_term) >= 2:
        with st.spinner("Searching..."):
            results = search_aircraft(search_term)
        
        if not results.empty:
            st.caption(f"Found {len(results)} matches")
            
            aircraft_options = results['TAIL_NUMBER'].unique().tolist()
            selected_from_search = st.selectbox(
                "Select aircraft",
                options=["Choose aircraft..."] + aircraft_options,
                label_visibility="collapsed",
                key="search_select"
            )
            if selected_from_search != "Choose aircraft...":
                st.session_state.selected_tail = selected_from_search
        else:
            st.caption(f"No matches for '{search_term}'")
        
        # Most Active Aircraft section
        render_section_header("Most Active Aircraft")
        
        with st.spinner("Loading top aircraft..."):
            top_aircraft = get_top_aircraft(8)
        
        if not top_aircraft.empty:
            st.dataframe(
                top_aircraft[['TAIL_NUMBER', 'AIRCRAFT_MANUFACTURER', 'RECORD_COUNT']],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "TAIL_NUMBER": "Tail",
                    "AIRCRAFT_MANUFACTURER": "Manufacturer",
                    "RECORD_COUNT": st.column_config.NumberColumn("Records", format="%d")
                },
                height=200
            )
            
            # Quick select from top aircraft
            top_options = top_aircraft['TAIL_NUMBER'].tolist()
            top_select = st.selectbox(
                "Quick select",
                options=["Choose aircraft..."] + top_options,
                label_visibility="collapsed",
                key="top_select"
            )
            if top_select != "Choose aircraft...":
                st.session_state.selected_tail = top_select
    
    # Aircraft Details Section (shown when aircraft is selected)
    if st.session_state.selected_tail:
        selected_aircraft = st.session_state.selected_tail
        
        render_section_header(f"Aircraft Details: {selected_aircraft}")
        
        # Fetch full aircraft info
        with st.spinner("Loading aircraft details..."):
            aircraft_results = search_aircraft(selected_aircraft)
        
        if not aircraft_results.empty:
            aircraft_info = aircraft_results[aircraft_results['TAIL_NUMBER'] == selected_aircraft].iloc[0]
            
            detail_col1, detail_col2 = st.columns(2)
            
            with detail_col1:
                st.markdown(f"""
| Attribute | Value |
|-----------|-------|
| **Tail Number** | {selected_aircraft} |
| **Manufacturer** | {aircraft_info['AIRCRAFT_MANUFACTURER']} |
| **Model** | {aircraft_info['AIRCRAFT_MODEL']} |
| **Year** | {aircraft_info['AIRCRAFT_YEAR'] or 'N/A'} |
| **Engine** | {aircraft_info['ENGINE_MANUFACTURER'] or 'N/A'} {aircraft_info['ENGINE_MODEL'] or ''} |
| **Owner** | {aircraft_info['OWNER_NAME'] or 'N/A'} |
| **Data Source** | {aircraft_info['SOURCE_TYPE']} |
""")
            
            with detail_col2:
                with st.spinner("Loading flight history..."):
                    activity = get_aircraft_activity(selected_aircraft, 50)
                
                if not activity.empty:
                    air_count = len(activity[activity['AIR_GROUND_STATUS'] == 'AIR'])
                    ground_count = len(activity[activity['AIR_GROUND_STATUS'] == 'GROUND'])
                    
                    m1, m2, m3 = st.columns(3)
                    m1.metric("Records", len(activity))
                    m2.metric("In Air", air_count)
                    m3.metric("On Ground", ground_count)
                    
                    st.dataframe(
                        activity[['RECORD_TS', 'FLIGHT_CALLSIGN', 'ALTITUDE_BARO', 'GROUND_SPEED', 'AIR_GROUND_STATUS']].head(10),
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "RECORD_TS": st.column_config.DatetimeColumn("Timestamp", format="MMM D, HH:mm"),
                            "FLIGHT_CALLSIGN": "Callsign",
                            "ALTITUDE_BARO": st.column_config.NumberColumn("Altitude", format="%d ft"),
                            "GROUND_SPEED": st.column_config.NumberColumn("Speed", format="%d kts"),
                            "AIR_GROUND_STATUS": "Status"
                        }
                    )
                else:
                    st.caption("No recent activity found.")
            
            # Clear selection button
            if st.button("Clear Selection", type="secondary"):
                st.session_state.selected_tail = None
                st.rerun()

# =============================================================================
# Page: Traffic Analysis
# =============================================================================
elif page == "Traffic Analysis":
    render_page_header("Traffic Analysis", "Find optimal departure windows and understand congestion patterns")
    
    # Load optimal windows data
    with st.spinner("Analyzing traffic patterns..."):
        optimal_windows = get_optimal_flight_windows()
        status_data = get_air_ground_distribution()
        day_of_week_data = get_traffic_by_day_of_week()
    
    # Optimal Departure Windows - THE KEY INSIGHT
    render_section_header("Optimal Departure Windows")
    st.caption("Based on historical traffic patterns — schedule flights during low-congestion periods")
    
    if not optimal_windows.empty:
        # Identify low traffic windows
        low_traffic_hours = optimal_windows[optimal_windows['congestion_level'] == 'LOW']['hour_of_day'].tolist()
        high_traffic_hours = optimal_windows[optimal_windows['congestion_level'] == 'HIGH']['hour_of_day'].tolist()
        
        # Format hour ranges
        def format_hour(h):
            return f"{int(h):02d}:00"
        
        low_windows_str = ", ".join([format_hour(h) for h in low_traffic_hours[:4]]) if low_traffic_hours else "None identified"
        high_windows_str = ", ".join([format_hour(h) for h in high_traffic_hours[:3]]) if high_traffic_hours else "None identified"
        
        rec_col1, rec_col2 = st.columns(2)
        with rec_col1:
            st.markdown(f"""
<div style="background-color: rgba(34, 197, 94, 0.1); border: 1px solid rgba(34, 197, 94, 0.3); border-radius: 8px; padding: 1rem; border-left: 3px solid #22C55E;">
<p style="color: #22C55E; font-weight: 600; margin: 0 0 0.5rem 0; font-size: 0.85rem;">RECOMMENDED DEPARTURE TIMES</p>
<p style="color: #FAFAFA; margin: 0; font-size: 1.1rem;">{low_windows_str} UTC</p>
<p style="color: #71717A; margin: 0.5rem 0 0 0; font-size: 0.8rem;">Lower traffic = reduced delays and congestion</p>
</div>
""", unsafe_allow_html=True)
        
        with rec_col2:
            st.markdown(f"""
<div style="background-color: rgba(220, 38, 38, 0.1); border: 1px solid rgba(220, 38, 38, 0.3); border-radius: 8px; padding: 1rem; border-left: 3px solid #DC2626;">
<p style="color: #DC2626; font-weight: 600; margin: 0 0 0.5rem 0; font-size: 0.85rem;">AVOID IF POSSIBLE</p>
<p style="color: #FAFAFA; margin: 0; font-size: 1.1rem;">{high_windows_str} UTC</p>
<p style="color: #71717A; margin: 0.5rem 0 0 0; font-size: 0.8rem;">Peak congestion periods — expect delays</p>
</div>
""", unsafe_allow_html=True)
        
        # Congestion heatmap by hour
        # Create color mapping for congestion levels
        congestion_colors = {'LOW': '#22C55E', 'MODERATE': '#F59E0B', 'HIGH': '#DC2626'}
        optimal_windows['color'] = optimal_windows['congestion_level'].map(congestion_colors)
        
        fig = px.bar(
            optimal_windows,
            x='hour_of_day',
            y='avg_hourly_traffic',
            color='congestion_level',
            color_discrete_map=congestion_colors,
            labels={
                'hour_of_day': 'Hour (UTC)',
                'avg_hourly_traffic': 'Avg Traffic',
                'congestion_level': 'Congestion'
            }
        )
        fig.update_layout(
            height=300,
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(family="Plus Jakarta Sans, sans-serif", color='#FAFAFA'),
            xaxis=dict(
                tickmode='linear', 
                tick0=0, 
                dtick=2,
                gridcolor='#27272A',
                zerolinecolor='#27272A',
                title=None
            ),
            yaxis=dict(gridcolor='#27272A', title=None),
            legend=dict(
                orientation='h',
                yanchor='bottom',
                y=1.02,
                xanchor='right',
                x=1,
                font=dict(color='#71717A')
            ),
            margin=dict(l=0, r=0, t=40, b=0),
            bargap=0.1
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Day of Week Patterns
    render_section_header("Weekly Traffic Patterns")
    st.caption("Plan ahead — some days are consistently busier")
    
    if not day_of_week_data.empty:
        col_dow1, col_dow2 = st.columns([2, 1])
        
        with col_dow1:
            fig = px.bar(
                day_of_week_data,
                x='day_name',
                y='avg_daily_records',
                color='avg_daily_records',
                color_continuous_scale=[[0, '#27272A'], [0.5, '#F59E0B'], [1, '#DC2626']],
                labels={
                    'day_name': 'Day',
                    'avg_daily_records': 'Avg Daily Traffic'
                }
            )
            fig.update_layout(
                height=280,
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font=dict(family="Plus Jakarta Sans, sans-serif", color='#FAFAFA'),
                xaxis=dict(gridcolor='#27272A', categoryorder='array', categoryarray=['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']),
                yaxis=dict(gridcolor='#27272A'),
                showlegend=False,
                coloraxis_showscale=False,
                margin=dict(l=0, r=0, t=10, b=0)
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col_dow2:
            # Find busiest and slowest days
            busiest_day = day_of_week_data.loc[day_of_week_data['avg_daily_records'].idxmax()]
            slowest_day = day_of_week_data.loc[day_of_week_data['avg_daily_records'].idxmin()]
            
            st.metric(
                label="Busiest Day",
                value=busiest_day['day_name'],
                delta=f"{busiest_day['avg_daily_records']:,.0f} avg records"
            )
            st.metric(
                label="Slowest Day", 
                value=slowest_day['day_name'],
                delta=f"{slowest_day['avg_daily_records']:,.0f} avg records"
            )
    
    # Fleet Activity Status
    render_section_header("Fleet Activity Status")
    
    if not status_data.empty:
        col1, col2 = st.columns([1, 2])
        
        with col1:
            color_map = {'AIR': '#22C55E', 'GROUND': '#F59E0B', 'UNKNOWN': '#71717A'}
            
            fig = px.pie(
                status_data,
                values='RECORD_COUNT',
                names='AIR_GROUND_STATUS',
                color='AIR_GROUND_STATUS',
                color_discrete_map=color_map,
                hole=0.5
            )
            fig.update_layout(
                height=240,
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(family="Plus Jakarta Sans, sans-serif", color='#FAFAFA'),
                legend=dict(font=dict(color='#71717A')),
                margin=dict(l=0, r=0, t=10, b=10),
                showlegend=False
            )
            fig.update_traces(
                textfont=dict(color='#FAFAFA'),
                marker=dict(line=dict(color='#0A0A0B', width=2))
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            for _, row in status_data.iterrows():
                pct = row['RECORD_COUNT'] / status_data['RECORD_COUNT'].sum() * 100
                st.metric(
                    label=row['AIR_GROUND_STATUS'],
                    value=f"{row['RECORD_COUNT']:,.0f}",
                    delta=f"{pct:.1f}% of records"
                )
            
            # Add insight about air/ground ratio
            air_records = status_data[status_data['AIR_GROUND_STATUS'] == 'AIR']['RECORD_COUNT'].sum() if 'AIR' in status_data['AIR_GROUND_STATUS'].values else 0
            total_records = status_data['RECORD_COUNT'].sum()
            if total_records > 0:
                air_pct = (air_records / total_records) * 100
                render_insight(f"Fleet Utilization: {air_pct:.0f}% of tracked positions show aircraft in flight")

# =============================================================================
# Page: Flight Map
# =============================================================================
elif page == "Flight Map":
    render_page_header("Flight Map", "Live situational awareness — see where aircraft are operating now")
    
    # Controls
    num_flights = st.slider(
        "Aircraft positions to display", 
        100, 2000, 500, 100,
        help="Adjust to load more or fewer flight positions"
    )
    
    with st.spinner(f"Loading {num_flights} recent flight positions..."):
        flight_data = get_recent_flights(num_flights)
    
    if not flight_data.empty:
        # Filter out rows with invalid size values (NaN ground speed)
        flight_data = flight_data.dropna(subset=['GROUND_SPEED', 'ALTITUDE_BARO'])
        flight_data = flight_data[flight_data['GROUND_SPEED'] > 0]
        
        # Summary metrics with operational framing
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Tracked Positions", len(flight_data))
        m2.metric("Avg Cruise Altitude", f"{flight_data['ALTITUDE_BARO'].mean():,.0f} ft")
        m3.metric("Avg Ground Speed", f"{flight_data['GROUND_SPEED'].mean():,.0f} kts")
        m4.metric("Active Aircraft", flight_data['TAIL_NUMBER'].nunique())
        
        # Map
        fig = px.scatter_mapbox(
            flight_data,
            lat='LATITUDE',
            lon='LONGITUDE',
            color='ALTITUDE_BARO',
            size='GROUND_SPEED',
            hover_name='TAIL_NUMBER',
            hover_data={
                'FLIGHT_CALLSIGN': True,
                'AIRCRAFT_MANUFACTURER': True,
                'AIRCRAFT_MODEL': True,
                'ALTITUDE_BARO': ':.0f',
                'GROUND_SPEED': ':.0f',
                'LATITUDE': False,
                'LONGITUDE': False
            },
            color_continuous_scale=[[0, '#3B82F6'], [0.5, '#F59E0B'], [1, '#DC2626']],
            size_max=12,
            zoom=3,
            center={'lat': 39.8283, 'lon': -98.5795}
        )
        fig.update_layout(
            mapbox_style='carto-darkmatter',
            height=550,
            margin={'r': 0, 't': 0, 'l': 0, 'b': 0},
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(family="Plus Jakarta Sans, sans-serif", color='#FAFAFA'),
            coloraxis_colorbar=dict(
                title=dict(text="Altitude (ft)", font=dict(color='#71717A', size=11)),
                tickfont=dict(color='#71717A', size=10),
                thickness=12,
                len=0.6
            )
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Flight distribution insight
        if len(flight_data) > 10:
            top_manufacturer = flight_data['AIRCRAFT_MANUFACTURER'].value_counts().head(1)
            if not top_manufacturer.empty:
                top_mfr_name = top_manufacturer.index[0]
                top_mfr_count = top_manufacturer.values[0]
                top_mfr_pct = (top_mfr_count / len(flight_data)) * 100
                render_insight(f"Airspace Composition: {top_mfr_name} aircraft represent {top_mfr_pct:.0f}% of currently tracked flights")
        
        st.caption("Map visualization: Color indicates altitude (blue=low, red=high). Size indicates ground speed.")
    else:
        st.caption("No flight position data available. Check data pipeline status on Fleet Overview.")


