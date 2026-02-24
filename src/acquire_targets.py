# Project Rigor Mortis: Target Acquisition Engine
# Objective: Bulk-download all US discount store locations to build a static local database.

import requests
import pandas as pd
import os
import time

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')

def build_poi_database():
    print("--- PROJECT RIGOR MORTIS: TARGET ACQUISITION ---")
    print("[+] Initiating Heavy Drill. Requesting full US footprint from OpenStreetMap...")
    print("[!] This is a massive query. It may take 1-3 minutes. Do not terminate.")

    # Overpass QL: Search the entire US administrative area for our targets.
    # We use timeout:300 to tell the server we are willing to wait for the payload.
    query = """
    [out:json][timeout:300];
    area["ISO3166-1"="US"][admin_level=2]->.searchArea;
    (
      node["name"~"Dollar General|Family Dollar|Dollar Tree",i](area.searchArea);
      way["name"~"Dollar General|Family Dollar|Dollar Tree",i](area.searchArea);
    );
    out center;
    """
    
    url = "https://overpass-api.de/api/interpreter"
    headers = {'User-Agent': 'ProjectRigorMortis_Data_Acquisition/1.0'}

    try:
        response = requests.post(url, data={'data': query}, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            elements = data.get('elements', [])
            print(f"[+] Payload Received: {len(elements)} raw targets identified.")
            
            # Parse the JSON into a clean Lat/Lon list
            store_list = []
            for el in elements:
                name = el.get('tags', {}).get('name', 'Unknown Discount Store')
                if el['type'] == 'node':
                    store_list.append({'Name': name, 'Lat': el['lat'], 'Lon': el['lon']})
                elif el['type'] == 'way' and 'center' in el:
                    store_list.append({'Name': name, 'Lat': el['center']['lat'], 'Lon': el['center']['lon']})

            # Save to CSV
            df = pd.DataFrame(store_list)
            
            # Ensure the data directory exists
            os.makedirs(DATA_DIR, exist_ok=True)
            output_path = os.path.join(DATA_DIR, 'dollar_general_locations.csv')
            
            df.to_csv(output_path, index=False)
            print(f"[+] Database locked. Extracted {len(df)} locations to {output_path}")
            print("[+] You are now cleared to run correlation_mapper.py V6.0")
            
        elif response.status_code == 429:
            print("[-] Error 429: The server is currently too busy to handle a heavy drill. Wait 5 minutes and try again.")
        else:
            print(f"[-] API Error: {response.status_code}")
            print(response.text[:200])

    except Exception as e:
        print(f"[-] Critical Error during extraction: {e}")

if __name__ == "__main__":
    build_poi_database()