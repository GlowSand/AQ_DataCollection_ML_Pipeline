import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
import json5

FRS_URL = "https://ofmpub.epa.gov/frs_public2/frs_rest_services.get_facilities"

# Modified from dump_zip_road_density to get zip coodes from all over the state of Texas
def get_state_zips(state_id: str, uszips_csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(uszips_csv_path)
    sub = df[df["state_id"].astype(str).str.strip().str.upper() == state_id.upper()]
    sub = sub[["zip", "lat", "lng"]].rename(columns={"lat": "latitude", "lng": "longitude"})
    sub = sub.dropna(subset=["latitude", "longitude"]).drop_duplicates(subset=["zip"]).reset_index(drop=True)
    return sub




def parse_args():
    p = argparse.ArgumentParser(description="Dump ZIP->pollution sources CSV for a given state.")
    p.add_argument("--state", default="TX", help='Two-letter state abbreviation, e.g. "TX"')
    p.add_argument("--uszips", default="uszips.csv",
                   help="Path to simplemaps uszips.csv")
    p.add_argument("--batch-size", type=int, default=25, help="ZIPs per batch")
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



def get_frs_data(zip_code: int, pgm_sys_acrnm: str, page_size: int = 100) -> pd.DataFrame:
    all_records = []
    start_record = 1

    while True:
        url = (f"{FRS_URL}?zip_code={zip_code}"
               f"&pgm_sys_acrnm={pgm_sys_acrnm}" #This determines what EPA programs we're querying facilities from
               f"&output=JSON"
               f"&start_record={start_record}"
               f"&count={page_size}")


        #Better HTTP failure handling from stack overflow
        response = requests.get(url)
        if response.status_code != 200:
            print(f"WARNING: FRS API returned {response.status_code} for ZIP {zip_code}, program {pgm_sys_acrnm}. Skipping.")
            break

        data = json5.loads(response.text)
        results = data.get("Results", {})
        facilities  = results.get("FRSFacility") or []
        total = int(results.get("TotalQueryResults") or  0)

        all_records.extend(facilities)


        if not facilities or  len(all_records) >= total:
            break

        start_record += page_size

    return pd.DataFrame(all_records) if all_records else pd.DataFrame()




"""
Need to define what exactly counts as a "pollution source" using the FRS programs.
FRS programs I've noted of interest include...
AIR - Air Emissions
AIRS/AFS - Air Facility System
CAMDBS - Clean Air Markets Division Business System - includes toxic chemical releases however not air-only related
EIS - Emissions Inventory System
NEI - National Emissions Inventory
TRIS - Toxics Release Inventory System

If we implement this method, we will definetly need web enrichment.   
"""
POLLUTION_SOURCE_PROGRAMS = ["AIR", "AIRS%2FAFS", "TRIS", "EIS", "NEI", "CAMDBS"]

def get_zip_pollution_sources(zip_code: int)  -> pd.DataFrame:
    frames = []

    # For each pollution source program, query the FRS API and collect results
    for program in POLLUTION_SOURCE_PROGRAMS:
        df = get_frs_data(zip_code, program)  
        if df is not None and not df.empty:
            df["matched_program"] = program   # tag which program found this facility
            frames.append(df)

    if not frames: #Early exit if nothing  found
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)

    # Deduplicate cuz this shit gotta alot of duplicates, its a US gov institute after all
    combined = (
        combined
        .groupby("RegistryId", as_index=False)
        .agg(lambda col: col.iloc[0] if col.name != "matched_program" else ",".join(col.unique()))
    )



    #Remember to loop through OONLY Texas zip codes
    combined["zip"] = zip_code
    return combined

def main():
    args = parse_args()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.out_dir)
    out_dir.mkdir(exist_ok=True)
    output_file = out_dir / f"{args.out_prefix}_{args.state}_{timestamp}.csv"
    
    
    
    loc_df = get_zip_centroids("Houston", "TX", args.uszips)
    if loc_df.empty:
        raise SystemExit(f"No ZIP codes found for state: {args.state}")
    
    
    
    first_write = True
    for _, row in loc_df.iterrows():
        zip_code = int(row["zip"])
        df = get_zip_pollution_sources(zip_code)
        if not df.empty:
            df.to_csv(output_file, mode="a", header=first_write, index=False)
            first_write = False
            print(f"ZIP {zip_code}: {len(df)} facilities written.")
        else:
            print(f"ZIP {zip_code}: no pollution sources found.")

    print(f"\nDONE: saved to {output_file}")

if __name__ == "__main__":
    main()