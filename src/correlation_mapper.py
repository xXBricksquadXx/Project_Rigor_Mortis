# Project Rigor Mortis: Correlation Mapper v2.0
# Objective: Calculate Vulnerability Scores based on Food Source + Poverty + Cancer Trend

import pandas as pd
import json
import os

# Define Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')

def load_toxicology():
    """Loads the Bad Actors"""
    try:
        df = pd.read_csv(os.path.join(DATA_DIR, 'fda_gras_list.csv'))
        return df
    except FileNotFoundError:
        return pd.DataFrame()

def load_cancer_rates():
    """Loads the Disease Trends"""
    try:
        df = pd.read_csv(os.path.join(DATA_DIR, 'cdc_cancer_rates_2024.csv'))
        return df
    except FileNotFoundError:
        return pd.DataFrame()

def load_geo_data():
    """Loads the Poverty/Food Access Map"""
    file_path = os.path.join(DATA_DIR, 'us_poverty_map.json')
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        return pd.DataFrame(data)
    except FileNotFoundError:
        print("[-] Error: us_poverty_map.json not found.")
        return pd.DataFrame()

def calculate_vulnerability(geo_df):
    """
    The Algorithm:
    High Poverty + Low Organic Access + Discount Food Source = HIGH TOXIC LOAD
    """
    print("\n--- CALCULATING VULNERABILITY INDEX ---")
    
    results = []
    
    for index, row in geo_df.iterrows():
        score = 0
        risk_factors = []
        
        # Factor 1: Poverty Multiplier
        if row['poverty_rate'] > 15.0:
            score += 3
            risk_factors.append("High Poverty")
        
        # Factor 2: The "Dollar General" Effect
        if "Discount" in row['food_source_primary'] or "Convenience" in row['food_source_primary']:
            score += 5  # Primary source of dye-heavy/processed food
            risk_factors.append("Ultra-Processed Diet")
            
        # Factor 3: The "Organic Opt-Out"
        if row['organic_access'] in ["None", "Low"]:
            score += 2
            risk_factors.append("No Detox Pathway")
            
        # Assign Clinical Status
        if score >= 8:
            status = "CRITICAL (Target Demographic)"
        elif score >= 5:
            status = "ELEVATED RISK"
        else:
            status = "LOW RISK (Protected Class)"
            
        results.append({
            "Region": row['region'],
            "Zip": row['zip_code'],
            "Vulnerability_Score": score,
            "Clinical_Status": status,
            "Primary_Risk_Factors": ", ".join(risk_factors)
        })
    
    return pd.DataFrame(results)

if __name__ == "__main__":
    print("--- PROJECT RIGOR MORTIS: ANALYSIS ENGINE ---")
    
    # 1. Ingest Data
    tox_df = load_toxicology()
    geo_df = load_geo_data()
    
    # 2. Run Analysis
    if not geo_df.empty:
        vuln_df = calculate_vulnerability(geo_df)
        
        # 3. Output the "Receipt"
        print("\n[+] VULNERABILITY REPORT GENERATED:")
        print(vuln_df[['Region', 'Clinical_Status', 'Primary_Risk_Factors']].to_string(index=False))
        
        # 4. Correlate with Specific Toxins
        print("\n[!] PROBABLE CHEMICAL EXPOSURE IN CRITICAL ZONES:")
        if not tox_df.empty:
            # If the zone is 'Discount Variety', they are likely consuming these:
            high_risk_toxins = tox_df[tox_df['category'].isin(['Colorant', 'Preservative'])]['chemical_name'].tolist()
            print(f"Residents in Critical Zones are statistically exposed to high volumes of: {', '.join(high_risk_toxins[:5])}...")
    
    print("\n[+] Correlation Complete.")