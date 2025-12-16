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

# =============================================================================
# Styling
# =============================================================================
st.markdown("""
<style>
    .metric-card {
        background-color: #1e3a5f;
        border-radius: 10px;
        padding: 20px;
        text-align: center;
    }
    .stMetric {
        background-color: #0e1117;
        border-radius: 10px;
        padding: 15px;
        border: 1px solid #262730;
    }
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #ffffff;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.1rem;
        color: #a3a8b4;
        margin-bottom: 2rem;
    }
</style>
""", unsafe_allow_html=True)

# =============================================================================
# Navigation
# =============================================================================
st.sidebar.title("✈️ DK Aviation")
st.sidebar.markdown("**Flight Insights Platform**")

# Show environment indicator
if env_type == "local":
    st.sidebar.success("🖥️ Running Locally")
else:
    st.sidebar.info("❄️ Streamlit in Snowflake")

st.sidebar.divider()

page = st.sidebar.radio(
    "Navigate",
    ["🏠 Fleet Overview", "🔍 Aircraft Lookup", "📊 Traffic Analysis", "🗺️ Flight Map"],
    label_visibility="collapsed"
)

st.sidebar.divider()
st.sidebar.markdown("---")
st.sidebar.caption("**Data Sources:**")
st.sidebar.caption("• ADS-B (KBFI & KAPA)")
st.sidebar.caption("• FAA Aircraft Registry")
st.sidebar.caption("---")
st.sidebar.caption("Built with Snowflake + Streamlit")

# =============================================================================
# Page: Fleet Overview
# =============================================================================
if page == "🏠 Fleet Overview":
    st.markdown('<p class="main-header">Fleet Overview Dashboard</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Consolidated ADS-B + FAA flight data insights for DK Aviation</p>', unsafe_allow_html=True)
    
    # Key Metrics
    with st.spinner("Loading metrics..."):
        metrics = get_overview_metrics()
        
    if not metrics.empty:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="Total Flight Records",
                value=f"{metrics['TOTAL_RECORDS'].iloc[0]:,.0f}"
            )
        with col2:
            st.metric(
                label="Unique Aircraft",
                value=f"{metrics['UNIQUE_AIRCRAFT'].iloc[0]:,.0f}"
            )
        with col3:
            st.metric(
                label="Manufacturers",
                value=f"{metrics['UNIQUE_MANUFACTURERS'].iloc[0]:,.0f}"
            )
        with col4:
            st.metric(
                label="Aircraft Owners",
                value=f"{metrics['UNIQUE_OWNERS'].iloc[0]:,.0f}"
            )
        
        st.caption(f"📅 Data range: {metrics['EARLIEST_RECORD'].iloc[0]} to {metrics['LATEST_RECORD'].iloc[0]}")
    
    st.divider()
    
    # Two column layout for charts
    col_left, col_right = st.columns([2, 1])
    
    with col_left:
        st.subheader("Top Aircraft Manufacturers")
        with st.spinner("Loading manufacturer data..."):
            mfr_data = get_top_manufacturers(15)
        
        if not mfr_data.empty:
            fig = px.bar(
                mfr_data,
                x='FLIGHT_RECORDS',
                y='MANUFACTURER',
                orientation='h',
                color='UNIQUE_AIRCRAFT',
                color_continuous_scale='Blues',
                labels={
                    'FLIGHT_RECORDS': 'Flight Records',
                    'MANUFACTURER': 'Manufacturer',
                    'UNIQUE_AIRCRAFT': 'Unique Aircraft'
                }
            )
            fig.update_layout(
                height=500,
                yaxis={'categoryorder': 'total ascending'},
                showlegend=False,
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font_color='#ffffff'
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with col_right:
        st.subheader("Data Sources")
        with st.spinner("Loading source breakdown..."):
            source_data = get_source_breakdown()
        
        if not source_data.empty:
            fig = px.pie(
                source_data,
                values='RECORD_COUNT',
                names='SOURCE_TYPE',
                color_discrete_sequence=['#1f77b4', '#ff7f0e'],
                hole=0.4
            )
            fig.update_layout(
                height=300,
                paper_bgcolor='rgba(0,0,0,0)',
                font_color='#ffffff'
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Show source details
            for _, row in source_data.iterrows():
                st.metric(
                    label=f"{row['SOURCE_TYPE']} Records",
                    value=f"{row['RECORD_COUNT']:,.0f}",
                    delta=f"{row['UNIQUE_AIRCRAFT']:,.0f} aircraft"
                )

# =============================================================================
# Page: Aircraft Lookup
# =============================================================================
elif page == "🔍 Aircraft Lookup":
    st.markdown('<p class="main-header">Aircraft Lookup</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Search for aircraft by tail number or callsign</p>', unsafe_allow_html=True)
    
    # Search input
    search_term = st.text_input(
        "🔍 Enter tail number or callsign",
        placeholder="e.g., N12345 or UAL123",
        help="Search is case-insensitive and supports partial matches"
    )
    
    if search_term and len(search_term) >= 2:
        with st.spinner("Searching..."):
            results = search_aircraft(search_term)
        
        if not results.empty:
            st.success(f"Found {len(results)} aircraft matching '{search_term}'")
            
            # Aircraft selection
            aircraft_options = results['TAIL_NUMBER'].unique().tolist()
            selected_aircraft = st.selectbox(
                "Select an aircraft to view details",
                options=aircraft_options
            )
            
            if selected_aircraft:
                st.divider()
                
                # Aircraft details
                aircraft_info = results[results['TAIL_NUMBER'] == selected_aircraft].iloc[0]
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader(f"✈️ {selected_aircraft}")
                    st.markdown(f"""
                    | Attribute | Value |
                    |-----------|-------|
                    | **Manufacturer** | {aircraft_info['AIRCRAFT_MANUFACTURER']} |
                    | **Model** | {aircraft_info['AIRCRAFT_MODEL']} |
                    | **Year** | {aircraft_info['AIRCRAFT_YEAR'] or 'N/A'} |
                    | **Engine** | {aircraft_info['ENGINE_MANUFACTURER'] or 'N/A'} {aircraft_info['ENGINE_MODEL'] or ''} |
                    | **Owner** | {aircraft_info['OWNER_NAME'] or 'N/A'} |
                    | **Data Source** | {aircraft_info['SOURCE_TYPE']} |
                    """)
                
                with col2:
                    st.subheader("Recent Activity")
                    with st.spinner("Loading flight history..."):
                        activity = get_aircraft_activity(selected_aircraft, 50)
                    
                    if not activity.empty:
                        # Show activity metrics
                        air_count = len(activity[activity['AIR_GROUND_STATUS'] == 'AIR'])
                        ground_count = len(activity[activity['AIR_GROUND_STATUS'] == 'GROUND'])
                        
                        m1, m2, m3 = st.columns(3)
                        m1.metric("Records", len(activity))
                        m2.metric("In Air", air_count)
                        m3.metric("On Ground", ground_count)
                        
                        # Show recent positions
                        st.dataframe(
                            activity[['RECORD_TS', 'FLIGHT_CALLSIGN', 'ALTITUDE_BARO', 'GROUND_SPEED', 'AIR_GROUND_STATUS']].head(10),
                            use_container_width=True,
                            hide_index=True
                        )
                    else:
                        st.info("No recent activity found for this aircraft.")
        else:
            st.warning(f"No aircraft found matching '{search_term}'")
    elif search_term:
        st.info("Please enter at least 2 characters to search")

# =============================================================================
# Page: Traffic Analysis
# =============================================================================
elif page == "📊 Traffic Analysis":
    st.markdown('<p class="main-header">Traffic Analysis</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Analyze flight patterns to optimize trip planning and mitigate delays</p>', unsafe_allow_html=True)
    
    # Air/Ground Distribution
    st.subheader("Air vs Ground Status Distribution")
    
    with st.spinner("Loading status distribution..."):
        status_data = get_air_ground_distribution()
    
    if not status_data.empty:
        col1, col2 = st.columns([1, 2])
        
        with col1:
            fig = px.pie(
                status_data,
                values='RECORD_COUNT',
                names='AIR_GROUND_STATUS',
                color_discrete_sequence=['#2ecc71', '#e74c3c', '#95a5a6'],
                hole=0.4
            )
            fig.update_layout(
                height=300,
                paper_bgcolor='rgba(0,0,0,0)',
                font_color='#ffffff'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            for _, row in status_data.iterrows():
                pct = row['RECORD_COUNT'] / status_data['RECORD_COUNT'].sum() * 100
                st.metric(
                    label=row['AIR_GROUND_STATUS'],
                    value=f"{row['RECORD_COUNT']:,.0f}",
                    delta=f"{pct:.1f}%"
                )
    
    st.divider()
    
    # Hourly traffic pattern
    st.subheader("Hourly Traffic Pattern")
    st.caption("Flight activity by hour of day (all data)")
    
    with st.spinner("Loading hourly traffic..."):
        hourly_data = get_hourly_traffic()
    
    if not hourly_data.empty:
        fig = px.bar(
            hourly_data,
            x='HOUR_OF_DAY',
            y='FLIGHT_COUNT',
            color='UNIQUE_AIRCRAFT',
            color_continuous_scale='Viridis',
            labels={
                'HOUR_OF_DAY': 'Hour of Day (UTC)',
                'FLIGHT_COUNT': 'Flight Records',
                'UNIQUE_AIRCRAFT': 'Unique Aircraft'
            }
        )
        fig.update_layout(
            height=400,
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font_color='#ffffff',
            xaxis=dict(tickmode='linear', tick0=0, dtick=1)
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Peak hours insight
        peak_hour = hourly_data.loc[hourly_data['FLIGHT_COUNT'].idxmax()]
        st.info(f"📈 **Peak Traffic Hour:** {int(peak_hour['HOUR_OF_DAY']):02d}:00 UTC with {peak_hour['FLIGHT_COUNT']:,.0f} records")

# =============================================================================
# Page: Flight Map
# =============================================================================
elif page == "🗺️ Flight Map":
    st.markdown('<p class="main-header">Flight Map</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Visualize recent aircraft positions</p>', unsafe_allow_html=True)
    
    # Controls
    col1, col2 = st.columns([1, 3])
    with col1:
        num_flights = st.slider("Number of flights to display", 100, 2000, 500, 100)
    
    with st.spinner(f"Loading {num_flights} recent flight positions..."):
        flight_data = get_recent_flights(num_flights)
    
    if not flight_data.empty:
        # Summary metrics
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Flights Displayed", len(flight_data))
        m2.metric("Avg Altitude", f"{flight_data['ALTITUDE_BARO'].mean():,.0f} ft")
        m3.metric("Avg Speed", f"{flight_data['GROUND_SPEED'].mean():,.0f} kts")
        m4.metric("Unique Aircraft", flight_data['TAIL_NUMBER'].nunique())
        
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
            color_continuous_scale='Plasma',
            size_max=15,
            zoom=3,
            center={'lat': 39.8283, 'lon': -98.5795}  # Center of US
        )
        fig.update_layout(
            mapbox_style='carto-darkmatter',
            height=600,
            margin={'r': 0, 't': 0, 'l': 0, 'b': 0},
            paper_bgcolor='rgba(0,0,0,0)',
            font_color='#ffffff'
        )
        st.plotly_chart(fig, use_container_width=True)
        
        st.caption("💡 Tip: Color represents altitude (darker = higher). Size represents ground speed.")
    else:
        st.warning("No flight position data available.")

# =============================================================================
# Footer
# =============================================================================
st.sidebar.markdown("---")
st.sidebar.caption("© 2024 DK Aviation POC")
st.sidebar.caption("Powered by Snowflake")

