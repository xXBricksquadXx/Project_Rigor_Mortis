# Project Rigor Mortis: Correlation Mapper v6.2
# Objective: Top 100 National Critical Zone Isolation + Static POI Density Mapping

import pandas as pd
import folium
from folium.plugins import HeatMap
from branca.element import Template, MacroElement
import matplotlib.pyplot as plt
import os
import warnings
import numpy as np

warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
OUTPUT_DIR = os.path.join(BASE_DIR, 'docs')

def haversine_vectorized(lat1, lon1, lat2, lon2):
    """
    Vectorized Haversine formula to calculate distances between arrays of coordinates.
    Returns distance in kilometers.
    """
    R = 6371.0 # Earth radius in km
    lat1_rad, lon1_rad = np.radians(lat1), np.radians(lon1)
    lat2_rad, lon2_rad = np.radians(lat2), np.radians(lon2)

    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    a = np.sin(dlat / 2)**2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon / 2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return R * c

def calculate_store_density(tract_lat, tract_lon, stores_df, radius_km=5.0):
    """Calculates how many stores are within the specified radius of the tract center."""
    if stores_df.empty:
        return 0
    distances = haversine_vectorized(tract_lat, tract_lon, stores_df['Lat'].values, stores_df['Lon'].values)
    return np.sum(distances <= radius_km)

def load_and_filter_data():
    print("[+] Loading Federal Datasets and POI Data...")
    usda_path = os.path.join(DATA_DIR, 'usda_food_deserts.csv')
    cdc_path = os.path.join(DATA_DIR, 'cdc_places_health.csv')
    dg_path = os.path.join(DATA_DIR, 'dollar_general_locations.csv')

    # Load Dollar General Data
    stores_df = pd.DataFrame()
    if os.path.exists(dg_path):
        try:
            temp_df = pd.read_csv(dg_path)
            lat_col = next((c for c in temp_df.columns if 'lat' in c.lower()), None)
            lon_col = next((c for c in temp_df.columns if 'lon' in c.lower()), None)
            
            if lat_col and lon_col:
                stores_df['Lat'] = pd.to_numeric(temp_df[lat_col], errors='coerce')
                stores_df['Lon'] = pd.to_numeric(temp_df[lon_col], errors='coerce')
                stores_df = stores_df.dropna()
                print(f"[+] Loaded {len(stores_df)} store locations.")
            else:
                 print("[-] Could not identify Latitude/Longitude columns in POI data.")
        except Exception as e:
            print(f"[-] Error loading POI data: {e}")
    else:
        print("[-] 'dollar_general_locations.csv' not found. Store density will be 0.")

    if os.path.exists(usda_path) and os.path.exists(cdc_path):
        usda_df = pd.read_csv(usda_path, low_memory=False)
        cdc_df = pd.read_csv(cdc_path, low_memory=False)
        
        usda_target = next((c for c in ['CensusTract', 'TractFIPS'] if c in usda_df.columns), None)
        cdc_target = next((c for c in ['LocationID', 'TractFIPS', 'CensusTract'] if c in cdc_df.columns), None)

        usda_df['FIPS_CLEAN'] = usda_df[usda_target].astype(str).str.replace(r'\.0$', '', regex=True).str.zfill(11)
        cdc_df['FIPS_CLEAN'] = cdc_df[cdc_target].astype(str).str.replace(r'\.0$', '', regex=True).str.zfill(11)
        
        merged_df = pd.merge(usda_df, cdc_df, on='FIPS_CLEAN')
        
        if 'LAT' in merged_df.columns and 'LONG' in merged_df.columns:
            merged_df['Lat'] = pd.to_numeric(merged_df['LAT'], errors='coerce')
            merged_df['Lon'] = pd.to_numeric(merged_df['LONG'], errors='coerce')
        elif 'Geolocation' in merged_df.columns:
            coords = merged_df['Geolocation'].astype(str).str.extract(r'POINT\s*\(([-.\d]+)\s+([-.\d]+)\)')
            merged_df['Lon'] = pd.to_numeric(coords[0], errors='coerce')
            merged_df['Lat'] = pd.to_numeric(coords[1], errors='coerce')
        else:
            return pd.DataFrame()

        # FIXED ALABAMA CLUMPING: Explicitly target a continuous percentage column, not a boolean flag.
        if 'PovertyRate' in merged_df.columns:
            pov_col = 'PovertyRate'
        else:
            pov_cols = [c for c in merged_df.columns if 'Poverty' in c and 'Rate' in c]
            pov_col = pov_cols[0] if pov_cols else next((c for c in merged_df.columns if 'Poverty' in c), None)
            
        health_col = 'Data_Value' if 'Data_Value' in merged_df.columns else None

        if not pov_col or not health_col:
            print("[-] Cannot identify Poverty Rate or Health metric columns.")
            return pd.DataFrame()

        payload_df = pd.DataFrame({
            'Tract_Name': merged_df['FIPS_CLEAN'],
            'Lat': merged_df['Lat'],
            'Lon': merged_df['Lon'],
            'Poverty_Rate': pd.to_numeric(merged_df[pov_col], errors='coerce'),
            'Health_Metric': pd.to_numeric(merged_df[health_col], errors='coerce'),
        }).dropna().drop_duplicates(subset=['Tract_Name'])
        
        print(f"[+] Filtering to Top 100 Highest Poverty Tracts Nationally...")
        top_100_df = payload_df.sort_values(by='Poverty_Rate', ascending=False).head(100).copy()
        
        print("[+] Calculating Store Density for Critical Zones...")
        store_counts = []
        for index, row in top_100_df.iterrows():
            count = calculate_store_density(row['Lat'], row['Lon'], stores_df)
            store_counts.append(count)
            
        top_100_df['Discount_Store_Count'] = store_counts
        return top_100_df

    return pd.DataFrame()

def get_threat_color(count):
    """Dynamic color coding based on store density."""
    if count >= 5:
        return '#FF004D' # Crimson (Critical Density)
    elif count >= 2:
        return '#FF5F1F' # Hazard Orange (Elevated)
    else:
        return '#FFC000' # Yellow (Baseline)

def generate_heatmap(df):
    m = folium.Map(location=[39.8283, -98.5795], zoom_start=5, tiles='CartoDB dark_matter')
    
    for index, row in df.iterrows():
        store_count = int(row['Discount_Store_Count'])
        color = get_threat_color(store_count)
        
        folium.CircleMarker(
            location=[row['Lat'], row['Lon']],
            radius=store_count + 4, # Slightly larger baseline for visibility
            popup=f"<b>Tract FIPS:</b> {row['Tract_Name']}<br>"
                  f"<b>Poverty Rate:</b> {row['Poverty_Rate']}%<br>"
                  f"<b>Store Density:</b> {store_count} within 5km<br>"
                  f"<b>Health Anomaly:</b> {row['Health_Metric']}%",
            color=color,
            fill=True, 
            fill_opacity=0.8
        ).add_to(m)
        
    # Injecting the Threat Level Legend directly into the HTML map
    legend_html = '''
    {% macro html(this, kwargs) %}
    <div style="position: fixed; 
                bottom: 50px; left: 50px; width: 220px; height: 110px; 
                border:1px solid grey; z-index:9999; font-size:14px;
                background-color:rgba(10, 10, 10, 0.9);
                color: #FFFFFF; padding: 10px; font-family: monospace;">
    <b>Threat Level (Store Density)</b><br>
    <span style="color:#FF004D;">&#9679;</span> Critical (5+ stores)<br>
    <span style="color:#FF5F1F;">&#9679;</span> Elevated (2-4 stores)<br>
    <span style="color:#FFC000;">&#9679;</span> Baseline (0-1 stores)<br>
    </div>
    {% endmacro %}
    '''
    macro = MacroElement()
    macro._template = Template(legend_html)
    m.get_root().add_child(macro)
    
    map_path = os.path.join(OUTPUT_DIR, 'vulnerability_map.html')
    m.save(map_path)

def generate_scatter_plot(df):
    # 5. Contrast & Legibility (Academic White/Gray Theme)
    plt.figure(figsize=(12, 8), facecolor='white')
    ax = plt.gca()
    ax.set_facecolor('#f4f4f9') # Light gray background for contrast
    
    x = df['Discount_Store_Count']
    y = df['Health_Metric']
    poverty = df['Poverty_Rate']

    # 7. Visually Separate Clusters (Jitter the discrete store counts slightly)
    # This prevents bubbles on the exact same X-integer from stacking invisibly.
    np.random.seed(42)
    x_jittered = x + np.random.uniform(-0.25, 0.25, size=len(x))
    
    # 3. Fix Color Scale (Color = Health Metric, Size = Poverty %)
    scatter = ax.scatter(
        x_jittered, y, 
        s=poverty * 15, # Bubble size multiplier
        c=y,            # Color gradient matches health metric severity
        cmap='viridis', # Professional, colorblind-safe colormap
        alpha=0.65, 
        edgecolors="white", 
        linewidth=1,
        zorder=3        # Ensure bubbles render above the gridlines
    )
    
    # 4. Improve Trendline Communication (R-Squared & Slope Math)
    if len(x) > 1 and len(y) > 1:
        # Calculate coefficients
        z = np.polyfit(x, y, 1)
        p = np.poly1d(z)
        slope = z[0]
        
        # Calculate R-squared (The statistical strength of the correlation)
        correlation_matrix = np.corrcoef(x, y)
        r_squared = correlation_matrix[0, 1]**2
        
        # Plot the trendline
        x_vals = np.array(ax.get_xlim())
        ax.plot(x_vals, p(x_vals), color="#222222", linestyle="--", linewidth=2.5, zorder=4,
                label=f"Linear Fit (R² = {r_squared:.2f}, Slope = {slope:.2f})")

    # 1 & 2. Explicit Titles and Axis Labels
    plt.suptitle("Discount Variety Store Density vs. Chronic Health Prevalence", 
                 fontsize=15, fontweight='bold', y=0.96)
    plt.title("(Top 100 High-Poverty US Census Tracts)", fontsize=12, pad=10, color='#444444')
    
    ax.set_xlabel('Discount Variety Stores within 5km (Count)', fontsize=11, fontweight='bold')
    ax.set_ylabel('Chronic Health Prevalence (%)', fontsize=11, fontweight='bold')
    
    # Gridlines
    ax.grid(True, linestyle='-', alpha=0.6, color='white', zorder=1)
    
    # Legend & Colorbar
    cbar = plt.colorbar(scatter, ax=ax, pad=0.02)
    cbar.set_label('Chronic Health Prevalence (%)', rotation=270, labelpad=20, fontweight='bold')
    
    # Build a clean legend explaining the trendline and bubble size
    handles, labels = ax.get_legend_handles_labels()
    handles.append(plt.scatter([], [], s=200, c='gray', alpha=0.5, edgecolors='white'))
    labels.append('Bubble Size = Poverty Rate (%)')
    ax.legend(handles, labels, loc='upper left', frameon=True, facecolor='white', edgecolor='lightgray')
    
    # 6. Essential Footnote Block (Data Provenance)
    footnote_text = (
        "Data Sources & Methodology:\n"
        "• CDC PLACES (2024): Tract-level chronic health anomalies.\n"
        "• USDA FARA (2019): Tract-level poverty rates (Note: Structural data lag identified in federal ingestion).\n"
        "• OpenStreetMap POI Extraction: 5km centroid radius for corporate variety distributors.\n"
        "• Sample bounded strictly to the top 100 highest poverty census tracts nationwide."
    )
    plt.figtext(0.015, 0.015, footnote_text, ha="left", fontsize=9, color='#444444',
                bbox=dict(boxstyle="square,pad=0.5", facecolor="#f4f4f9", edgecolor="#dddddd", alpha=1))
    
    # Adjust layout to prevent footnote cutoff
    plt.subplots_adjust(bottom=0.22)
    
    chart_path = os.path.join(OUTPUT_DIR, 'correlation_chart.png')
    plt.savefig(chart_path, dpi=300, bbox_inches='tight') # dpi=300 forces ultra-crisp rendering
if __name__ == "__main__":
    print("--- PROJECT RIGOR MORTIS: ANALYSIS ENGINE V6.2 ---")
    df = load_and_filter_data()
    if not df.empty:
        generate_heatmap(df)
        generate_scatter_plot(df)
        print("\n[+] Top 100 Render Complete. Receipts generated in /docs.")
    else:
        print("\n[-] Error: DataFrame is empty. Check data ingestion.")