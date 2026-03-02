# ==========================================================
# Project Rigor Mortis
# Phase V8.3 - CMS Part D TN Concentration & Normalized Intensity Model
# ==========================================================
# Scope:
#   - 2025 CMS Part D Prescriber Public Use File (TN subset)
#   - Automated Geographic Crosswalk (City to CMS County 'TOT_BENES')
#   - Revenue concentration (HHI)
#   - Utilization normalization (per 1,000 Medicare beneficiaries)
#   - Revenue-weighted intensity index
#   - Small cell suppression handling & Visual Receipt Generation
# ==========================================================

import pandas as pd
import numpy as np
import os
import re
import warnings
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
OUTPUT_DIR = os.path.join(BASE_DIR, 'docs')

# Updated 2025 Naming Conventions
CMS_FILE = 'cms_psych_prescribers_tn.csv'
BENEFICIARY_FILE = 'tn_medicare_beneficiaries_2025.csv'

# ----------------------------------------------------------
# GEOGRAPHIC CROSSWALK (City -> CMS County Name)
# ----------------------------------------------------------
CITY_TO_COUNTY_MAP = {
    'NASHVILLE': 'DAVIDSON COUNTY',
    'MEMPHIS': 'SHELBY COUNTY',
    'CHATTANOOGA': 'HAMILTON COUNTY',
    'KNOXVILLE': 'KNOX COUNTY',
    'CLARKSVILLE': 'MONTGOMERY COUNTY',
    'MURFREESBORO': 'RUTHERFORD COUNTY',
    'COLUMBIA': 'MAURY COUNTY',
    'LAWRENCEBURG': 'LAWRENCE COUNTY',
    'LEWISBURG': 'MARSHALL COUNTY',
    'PULASKI': 'GILES COUNTY'
}

TARGET_CITIES = list(CITY_TO_COUNTY_MAP.keys())

# ----------------------------------------------------------
# PIPELINE MAP (Neutral Naming)
# ----------------------------------------------------------
PIPELINE_MAP = {
    # Antidepressants
    'FLUOXETINE': 'Antidepressant',
    'SERTRALINE': 'Antidepressant',
    'ESCITALOPRAM': 'Antidepressant',
    'CITALOPRAM': 'Antidepressant',
    'PAROXETINE': 'Antidepressant',
    'FLUVOXAMINE': 'Antidepressant',
    'VILAZODONE': 'Antidepressant',
    'VORTIOXETINE': 'Antidepressant',
    'TRAZODONE': 'Antidepressant',

    # Antipsychotics
    'RISPERIDONE': 'Antipsychotic',
    'OLANZAPINE': 'Antipsychotic',
    'QUETIAPINE': 'Antipsychotic',
    'ARIPIPRAZOLE': 'Antipsychotic',
    'ZIPRASIDONE': 'Antipsychotic',
    'CLOZAPINE': 'Antipsychotic',
    'PALIPERIDONE': 'Antipsychotic',
    'HALOPERIDOL': 'Antipsychotic',
    'LURASIDONE': 'Antipsychotic',
    'BREXPIPRAZOLE': 'Antipsychotic',
    'CARIPRAZINE': 'Antipsychotic',
    'LUMATEPERONE': 'Antipsychotic',

    # Opioid / Neuropathic / OUD
    'HYDROCODONE': 'Opioid / Neuropathic / OUD',
    'GABAPENTIN': 'Opioid / Neuropathic / OUD',
    'PREGABALIN': 'Opioid / Neuropathic / OUD',
    'BUPRENORPHINE': 'Opioid / Neuropathic / OUD',
    'NALOXONE': 'Opioid / Neuropathic / OUD',

    # Stimulants
    'AMPHETAMINE': 'Stimulant',
    'DEXTROAMPHETAMINE': 'Stimulant',
    'LISDEXAMFETAMINE': 'Stimulant',
    'METHYLPHENIDATE': 'Stimulant',

    # Hormonal
    'MEDROXYPROGESTERONE': 'Hormonal',
    'LEVONORGESTREL': 'Hormonal',
    'NORETHINDRONE': 'Hormonal'
}

# ----------------------------------------------------------
# LOAD & SANITIZE
# ----------------------------------------------------------
def load_data():
    print("--- PROJECT RIGOR MORTIS: CMS INTENSITY AUDIT V8.3 ---")
    cms_path = os.path.join(DATA_DIR, CMS_FILE)
    bene_path = os.path.join(DATA_DIR, BENEFICIARY_FILE)

    if not os.path.exists(cms_path):
        raise FileNotFoundError(f"CMS Prescriber file not found at {cms_path}")

    print("[+] Ingesting CMS Prescriber Ledger...")
    df = pd.read_csv(cms_path, low_memory=False)

    # Standardize fields
    df['Prscrbr_City'] = df['Prscrbr_City'].astype(str).str.upper().str.strip()
    df['Gnrc_Name'] = df['Gnrc_Name'].astype(str).str.upper().str.strip()
    df['Brnd_Name'] = df['Brnd_Name'].astype(str).str.upper().str.strip() if 'Brnd_Name' in df.columns else ""

    # Filter strictly for Target Cities
    df = df[df['Prscrbr_City'].isin(TARGET_CITIES)].copy()

    df['Tot_30day_Fills'] = pd.to_numeric(df['Tot_30day_Fills'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
    df['Tot_Drug_Cst'] = pd.to_numeric(df['Tot_Drug_Cst'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)

    # Small cell suppression handling (<11 beneficiaries masked by CMS)
    if 'Bene_Count' in df.columns:
        df = df[pd.to_numeric(df['Bene_Count'].astype(str).str.replace('*', ''), errors='coerce').fillna(0) >= 11]

    if not os.path.exists(bene_path):
        raise FileNotFoundError(f"Beneficiary enrollment file missing: {bene_path}")

    print("[+] Ingesting 2025 Beneficiary Enrollment Data & Building Crosswalk...")
    raw_bene_df = pd.read_csv(bene_path, low_memory=False)
    
    # Clean the county column from the raw federal data
    raw_bene_df['BENE_COUNTY_DESC'] = raw_bene_df['BENE_COUNTY_DESC'].astype(str).str.upper().str.strip()
    # Strip quotes if they exist (e.g., '"Anderson County"' -> 'ANDERSON COUNTY')
    raw_bene_df['BENE_COUNTY_DESC'] = raw_bene_df['BENE_COUNTY_DESC'].str.replace('"', '')

    crosswalk_data = []
    for city, county in CITY_TO_COUNTY_MAP.items():
        match = raw_bene_df[raw_bene_df['BENE_COUNTY_DESC'] == county]
        if not match.empty:
            tot_benes = pd.to_numeric(match['TOT_BENES'].iloc[0], errors='coerce')
            crosswalk_data.append({'City': city, 'Medicare_Beneficiaries_2025': tot_benes})
        else:
            print(f"[-] Warning: {county} not found in beneficiary data. {city} math will fail.")

    bene_df = pd.DataFrame(crosswalk_data)
    print(f"[+] Crosswalk established for {len(bene_df)} target jurisdictions.")

    return df, bene_df

# ----------------------------------------------------------
# PIPELINE CLASSIFICATION
# ----------------------------------------------------------
def classify_pipeline(df):
    print("[+] Executing Regex Pipeline Classification...")
    keys = list(PIPELINE_MAP.keys())
    pattern = '|'.join([f"\\b{re.escape(k)}\\b" for k in keys])

    df['Search_String'] = df['Gnrc_Name'] + " " + df['Brnd_Name']
    df = df[df['Search_String'].str.contains(pattern, regex=True, na=False)].copy()

    def mapper(text):
        for k, v in PIPELINE_MAP.items():
            if re.search(f"\\b{re.escape(k)}\\b", text):
                return v
        return np.nan

    df['Pipeline'] = df['Search_String'].apply(mapper)
    return df

# ----------------------------------------------------------
# V8.3 METRIC ENGINE
# ----------------------------------------------------------
def compute_metrics(df, bene_df):
    print("[+] Computing Revenue Concentration and Intensity Metrics...")
    agg = df.groupby(['Prscrbr_City', 'Pipeline']).agg(
        Total_Fills=('Tot_30day_Fills', 'sum'),
        Total_Cost=('Tot_Drug_Cst', 'sum')
    ).reset_index()

    agg = agg.rename(columns={'Prscrbr_City': 'City'})
    agg = agg.merge(bene_df, on='City', how='inner')

    # ---------------- Normalization ----------------
    agg['Fills_per_1000_Benes'] = (agg['Total_Fills'] / agg['Medicare_Beneficiaries_2025']) * 1000
    agg['Cost_per_Beneficiary'] = (agg['Total_Cost'] / agg['Medicare_Beneficiaries_2025'])
    agg['Cost_per_Fill'] = (agg['Total_Cost'] / agg['Total_Fills']).replace([np.inf, -np.inf], 0)

    # ---------------- Revenue & Fill Shares ----------------
    city_totals = agg.groupby('City').agg(
        City_Total_Cost=('Total_Cost', 'sum'),
        City_Total_Fills=('Total_Fills', 'sum')
    ).reset_index()

    agg = agg.merge(city_totals, on='City')
    agg['Revenue_Share'] = agg['Total_Cost'] / agg['City_Total_Cost']
    agg['Fill_Share'] = agg['Total_Fills'] / agg['City_Total_Fills']

    # ---------------- Herfindahl-Hirschman Index (HHI) ----------------
    hhi = agg.groupby('City').apply(
        lambda x: pd.Series({
            'HHI_Revenue': (x['Revenue_Share'] ** 2).sum(),
            'HHI_Fills': (x['Fill_Share'] ** 2).sum()
        })
    ).reset_index()

    # ---------------- Revenue Weighted Intensity ----------------
    agg['Revenue_Weighted_Intensity'] = agg['Fills_per_1000_Benes'] * agg['Revenue_Share']
    intensity = agg.groupby('City').agg(
        Revenue_Weighted_Intensity_Index=('Revenue_Weighted_Intensity', 'sum')
    ).reset_index()

    final_city = city_totals.merge(hhi, on='City').merge(intensity, on='City')

    return agg, final_city

# ----------------------------------------------------------
# VISUAL RECEIPT (Normalized Bar Chart)
# ----------------------------------------------------------
def generate_visual_receipt(detail_df, city_df):
    print("[+] Rendering Normalized Visual Receipts...")
    
    pivot_fills = detail_df.pivot(index='City', columns='Pipeline', values='Fills_per_1000_Benes').fillna(0)
    city_df_sorted = city_df.sort_values(by='Revenue_Weighted_Intensity_Index', ascending=True)
    pivot_fills = pivot_fills.reindex(city_df_sorted['City'])

    fig, ax = plt.subplots(figsize=(14, 8), facecolor='white')
    ax.set_facecolor('#f9f9fc')
    
    colors = ['#1f77b4', '#d62728', '#2ca02c', '#ff7f0e', '#9467bd']
    
    pivot_fills.plot(kind='barh', stacked=True, ax=ax, color=colors[:len(pivot_fills.columns)], edgecolor='white', linewidth=1.2)
    
    plt.suptitle("Medicare Part D Billing Intensity: Normalized per 1,000 Beneficiaries", 
                 fontsize=14, fontweight='bold', y=0.96)
    plt.title("(Case Study: Tennessee Urban vs. Rural Clinic Footprint - 2025 Model)", 
              fontsize=11, pad=10, color='#444444')
    
    ax.set_xlabel('30-Day Fills per 1,000 Local Medicare Beneficiaries', fontsize=11, fontweight='bold')
    ax.set_ylabel('Target Service Area (Ranked by Intensity Index)', fontsize=11, fontweight='bold')
    
    ax.xaxis.set_major_formatter(ticker.StrMethodFormatter('{x:,.0f}'))
    ax.grid(True, axis='x', linestyle='--', alpha=0.5, color='gray', zorder=0)
    
    plt.legend(title='Pathology Pipeline', bbox_to_anchor=(1.02, 1), 
               loc='upper left', frameon=True, facecolor='white', edgecolor='lightgray')
    
    footnote_text = (
        "Data Source & Methodology:\n"
        "• CMS Part D Prescriber Ledger(2023) & Medicare Monthly Enrollment (2025).\n"
        "• Utilization normalized per 1,000 regional beneficiaries to remove population bias.\n"
        "• Y-Axis sorted by Herfindahl-Hirschman (HHI) Revenue-Weighted Intensity Index."
    )
    plt.figtext(0.015, -0.05, footnote_text, ha="left", fontsize=9, color='#444444',
                bbox=dict(boxstyle="square,pad=0.5", facecolor="#f4f4f9", edgecolor="#dddddd", alpha=1))
    
    plt.tight_layout()
    chart_path = os.path.join(OUTPUT_DIR, 'cms_intensity_matrix.png')
    plt.savefig(chart_path, dpi=300, bbox_inches='tight')

# ----------------------------------------------------------
# EXPORT
# ----------------------------------------------------------
def export_outputs(detail_df, city_df):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    detail_df.to_csv(os.path.join(OUTPUT_DIR, 'v8_3_pipeline_detail.csv'), index=False)
    city_df.to_csv(os.path.join(OUTPUT_DIR, 'v8_3_city_concentration.csv'), index=False)
    print("[+] Mathematical CSV Outputs exported successfully to /docs.")

# ----------------------------------------------------------
# MAIN
# ----------------------------------------------------------
if __name__ == "__main__":
    cms_df, bene_df = load_data()
    classified_df = classify_pipeline(cms_df)
    detail, city = compute_metrics(classified_df, bene_df)
    
    generate_visual_receipt(detail, city)
    export_outputs(detail, city)

    print("\n[+] V8.3 CMS TN Concentration Model Complete. Analysis ready for Phase V integration.")