# DK Aviation Flight Insights - Deployment Guide

## Deploying to Streamlit in Snowflake

### Prerequisites
- Snowflake account with Streamlit enabled
- Access to the `CAPSTONE` database and `GOLD` schema
- Appropriate role with permissions to create Streamlit apps

### Step-by-Step Deployment

#### Option 1: Deploy via Snowsight UI (Recommended)

1. **Open Snowsight**
   - Navigate to your Snowflake account in a web browser
   - Log in to Snowsight

2. **Navigate to Streamlit**
   - Click on "Streamlit" in the left sidebar
   - Or go to: Data → Streamlit

3. **Create New Streamlit App**
   - Click "+ Streamlit App" button
   - Configure:
     - **App name:** `DK_AVIATION_FLIGHT_INSIGHTS`
     - **Warehouse:** Select your compute warehouse (e.g., `COMPUTE_WH`)
     - **Database:** `CAPSTONE`
     - **Schema:** `GOLD` (or `PUBLIC`)

4. **Upload App Code**
   - In the Streamlit editor, replace the default code with the contents of `streamlit_app.py`
   - The editor will automatically save

5. **Configure Packages**
   - Click on "Packages" in the editor
   - Add required packages:
     - `plotly`
     - `pandas`

6. **Run the App**
   - Click "Run" to start the app
   - The app will be accessible via the Snowflake URL

#### Option 2: Deploy via SQL

```sql
-- Create a stage for the Streamlit app (if not exists)
CREATE STAGE IF NOT EXISTS CAPSTONE.GOLD.STREAMLIT_STAGE;

-- Upload the app file to the stage (do this via Snowsight or SnowSQL)
-- PUT file:///path/to/streamlit_app.py @CAPSTONE.GOLD.STREAMLIT_STAGE;

-- Create the Streamlit app
CREATE OR REPLACE STREAMLIT CAPSTONE.GOLD.DK_AVIATION_FLIGHT_INSIGHTS
    ROOT_LOCATION = '@CAPSTONE.GOLD.STREAMLIT_STAGE'
    MAIN_FILE = 'streamlit_app.py'
    QUERY_WAREHOUSE = 'COMPUTE_WH';
```

### Granting Access to Others

```sql
-- Grant access to a role
GRANT USAGE ON STREAMLIT CAPSTONE.GOLD.DK_AVIATION_FLIGHT_INSIGHTS TO ROLE <role_name>;

-- Grant access to view the app
GRANT USAGE ON DATABASE CAPSTONE TO ROLE <role_name>;
GRANT USAGE ON SCHEMA CAPSTONE.GOLD TO ROLE <role_name>;
```

### App Features

| Page | Description | Key Metrics |
|------|-------------|-------------|
| 🏠 Fleet Overview | Dashboard with high-level metrics | Total records, unique aircraft, manufacturers, owners |
| 🔍 Aircraft Lookup | Search aircraft by tail number | Aircraft details, recent activity |
| 📊 Traffic Analysis | Flight pattern analysis | Hourly traffic, air/ground distribution |
| 🗺️ Flight Map | Map visualization | Recent flight positions with altitude/speed |

### Troubleshooting

**App won't load:**
- Ensure the warehouse is running
- Check that you have SELECT permissions on `CAPSTONE.GOLD.AIRCRAFT_FLIGHT_VW`

**Slow queries:**
- The view contains 500M+ records; queries use aggregations and limits
- Consider creating materialized views for frequently accessed metrics

**Package errors:**
- Ensure `plotly` and `pandas` are added to the app's package list

### Demo Tips

1. **Start with Fleet Overview** - Show the scale of data (500M+ records)
2. **Use Aircraft Lookup** - Search for a known tail number like "N5479D" 
3. **Explain Traffic Analysis** - Emphasize how dispatchers use this for delay mitigation
4. **End with Flight Map** - Visual impact of seeing aircraft positions

### Sample Aircraft to Search
- `N5479D` - Cessna 172N
- `UAL` - United Airlines flights
- `ASA` - Alaska Airlines flights
- `SWA` - Southwest Airlines flights

