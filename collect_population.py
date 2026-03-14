import json
import urllib.request
from pathlib import Path
import pandas as pd

# Population data source: ACS 2022 5-year ZCTA estimates + simplemaps coordinates
# Stored externally in houston_zips.csv (edit that file to add/remove ZIPs)
ZIPS_CSV = Path(__file__).parent / "houston_zips.csv"
OUTPUT   = Path("data/population_density.csv")

# Census ACS 5-year endpoint for ZCTA population
CENSUS_URL = (
    "https://api.census.gov/data/2023/acs/acs5"
    "?get=B01003_001E,NAME"
    "&for=zip%20code%20tabulation%20area:*"
)


def fetch_census_population() -> dict:
    """Returns {zip_str: population} for Texas ZCTAs from Census ACS API."""
    try:
        req = urllib.request.Request(CENSUS_URL, headers={"User-Agent": "aq-research/1.0"})
        with urllib.request.urlopen(req, timeout=30) as r:
            data = json.loads(r.read().decode())

        pop = {}
        for row in data[1:]:
            zcta = str(row[-1]).zfill(5)
            if zcta.startswith("77"):  # Houston-area ZIPs
                try:
                    pop[zcta] = int(row[0])
                except (ValueError, TypeError):
                    pass

        print(f"Census API: got {len(pop)} Texas ZCTAs")
        return pop

    except Exception as e:
        print(f"Census API unavailable ({e}), using values from houston_zips.csv")
        return {}


def estimate_area_km2(lat: float, lon: float) -> float:
    """
    Rough ZIP area estimate based on location within the metro.
    Outer suburban/rural ZIPs are much larger than urban ones.
    """
    if lat > 30.05 or lon < -95.60:
        return 80.0  # exurban
    elif lat > 29.90 or lon < -95.45:
        return 40.0  # suburban
    return 15.0      # urban core


def main():
    if not ZIPS_CSV.exists():
        print(f"Missing: {ZIPS_CSV}")
        print("houston_zips.csv has to be in the same folder as this script")
        return

    base_df = pd.read_csv(ZIPS_CSV, dtype={"zip": str})
    base_df["zip"] = base_df["zip"].str.zfill(5)
    print(f"Loaded {len(base_df)} ZIPs from {ZIPS_CSV.name}")

    census_pop = fetch_census_population()

    rows = []
    for _, row in base_df.iterrows():
        z   = row["zip"]
        lat = float(row["latitude"])
        lon = float(row["longitude"])
        pop = census_pop.get(z, int(row["population"]))  # Census first, CSV as fallback

        area    = estimate_area_km2(lat, lon)
        density = round(pop / area, 1) if area > 0 else 0.0
        source  = "ACS_2023_5yr" if z in census_pop else "ACS_2022_5yr_estimate"

        rows.append({
            "zip":             z,
            "city":            row["city"],
            "county":          row["county"],
            "latitude":        lat,
            "longitude":       lon,
            "population":      pop,
            "area_km2_approx": area,
            "pop_density_km2": density,
            "data_source":     source,
        })

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows).sort_values(["county", "zip"]).reset_index(drop=True)
    df.to_csv(OUTPUT, index=False)

    print(f"\nSaved {len(df)} ZIPs -> {OUTPUT}")
    print(df["county"].value_counts().to_string())
    print(f"\nPopulation: {df['population'].min():,} – {df['population'].max():,}")
    print(f"Density:    {df['pop_density_km2'].min():.0f} – {df['pop_density_km2'].max():.0f} people/km²")


if __name__ == "__main__":
    main()