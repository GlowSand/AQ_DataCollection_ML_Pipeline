import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np
import requests
import json5

FRS_URL = "https://ofmpub.epa.gov/frs_public2/frs_rest_services.get_facilities"


def parse_args():
    p = argparse.ArgumentParser(description="Dump ZIP->road density CSV (static dimension table).")
    p.add_argument("--cities", required=True, help='Semicolon-separated list like "Austin,TX;Houston,TX"')
    p.add_argument("--uszips", default="uszips.csv",
                   help="Path to simplemaps uszips.csv, you can get this at https://simplemaps.com/data/us-zips")
    p.add_argument("--batch-size", type=int, default=25, help="ZIPs per OSM batch (smaller is safer)")
    p.add_argument("--out-dir", default="data", help="Output directory")
    p.add_argument("--out-prefix", default="zip_pollution_sources", help="Output filename prefix")
    return p.parse_args()




def get_zip_centroids(city: str, state_id: str, uszips_csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(uszips_csv_path)

    city_norm = city.strip().lower()
    state_norm = state_id.strip().upper()

    sub = df[
        (df["city"].astype(str).str.strip().str.lower() == city_norm)
        & (df["state_id"].astype(str).str.strip().str.upper() == state_norm)
        ][["zip", "lat", "lng"]].copy()

    sub = sub.rename(columns={"lat": "latitude", "lng": "longitude"})
    sub = sub.dropna(subset=["latitude", "longitude"]).drop_duplicates(subset=["zip"]).reset_index(drop=True)

    return sub



def get_frs_data(zip: int, pgm_sys_acrnm:str):
    url = f"{FRS_URL}?zip_code={zip}&pgm_sys_acrnm={pgm_sys_acrnm}&output=JSON"
    
    response = requests.get (url)
    
    if response.status_code == 200:
        data = json5.loads(response.text)
        df = pd.DataFrame(data['Results']['FRSFacility']) #Added FRSFacility to access correct level foor a proper table where each facility fields its own column
        print(df.head())
    else:
        print(f"Failed to fetch data: {response.status_code} - {response.text}")

 
# Need to define what exactly counts as a "pollution source" using the FRS programs.
# If we implement this method, we will definetly need web enrichment.   

def main():
    ZIP = 60085
    program = "AIR"
    
    get_frs_data(ZIP, program)


if __name__ == "__main__":
    main()