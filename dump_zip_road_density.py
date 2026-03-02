# pip install osmnx geopandas shapely pyproj rtree pandas

import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd
import osmnx as ox

ox.settings.use_cache = True
ox.settings.log_console = False


# City schemas: "Austin,TX;Houston,TX"
def parse_city_state_list(s: str):
    pairs = []
    for part in s.split(";"):
        part = part.strip()
        if not part:
            continue
        if "," not in part:
            raise ValueError(f"Bad --cities entry '{part}'. Expected 'City,ST' (comma-separated).")
        city, st = part.split(",", 1)
        city = city.strip()
        st = st.strip().upper()
        if not city or not st:
            raise ValueError(f"Bad --cities entry '{part}'. Expected 'City,ST'.")
        pairs.append((city, st))
    if not pairs:
        raise ValueError("No valid city/state pairs provided in --cities.")
    return pairs


# Get the center lat/lon for a zip using data from https://simplemaps.com/data/us-zips
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


def chunked(df: pd.DataFrame, size: int):
    for i in range(0, len(df), size):
        yield df.iloc[i: i + size]


def dump_zip_road_density(loc_df: pd.DataFrame, output_file: Path, radius_m: int, batch_size: int):
    feature_name = f"road_len_m_{radius_m}"
    first_write = True

    for batch in chunked(loc_df, batch_size):
        rows = []
        for _, row in batch.iterrows():
            lat = float(row["latitude"])
            lon = float(row["longitude"])

            try:
                G = ox.graph_from_point((lat, lon), dist=radius_m, network_type="drive", simplify=True)
                edges = ox.graph_to_gdfs(G, nodes=False, edges=True)
                total_len_m = float(edges["length"].sum())
            except Exception:
                total_len_m = float("nan")

            rows.append({
                "zip": int(row["zip"]),
                "latitude": row["latitude"],
                "longitude": row["longitude"],
                feature_name: total_len_m,
            })

        out_df = pd.DataFrame(rows)

        out_df.to_csv(output_file, mode="a", header=first_write, index=False)
        first_write = False

        print(f"Saved batch of {len(batch)} ZIPs -> {output_file.name}")

    print(f"\nDONE: saved to {output_file}")


def parse_args():
    p = argparse.ArgumentParser(description="Dump ZIP->road density CSV (static dimension table).")
    p.add_argument("--cities", required=True, help='Semicolon-separated list like "Austin,TX;Houston,TX"')
    p.add_argument("--uszips", default="uszips.csv",
                   help="Path to simplemaps uszips.csv, you can get this at https://simplemaps.com/data/us-zips")
    p.add_argument("--radius-m", type=int, default=1000, help="Radius in meters")
    p.add_argument("--batch-size", type=int, default=25, help="ZIPs per OSM batch (smaller is safer)")
    p.add_argument("--out-dir", default="data", help="Output directory")
    p.add_argument("--out-prefix", default="zip_road_density", help="Output filename prefix")
    return p.parse_args()


def main():
    args = parse_args()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.out_dir)
    out_dir.mkdir(exist_ok=True)
    output_file = out_dir / f"{args.out_prefix}_{args.radius_m}m_{timestamp}.csv"

    pairs = parse_city_state_list(args.cities)

    all_frames = []
    for city, st in pairs:
        sub = get_zip_centroids(city, st, uszips_csv_path=args.uszips)
        if sub.empty:
            print(f"WARNING: No ZIP centroids found for the city: {city} in the state: {st}. Skipping gracefully...")
            continue
        all_frames.append(sub)

    if not all_frames:
        raise SystemExit("No ZIP centroids found for all provided cities and states.")

    loc_df = pd.concat(all_frames, ignore_index=True)

    dump_zip_road_density(
        loc_df=loc_df,
        output_file=output_file,
        radius_m=args.radius_m,
        batch_size=args.batch_size,
    )


if __name__ == "__main__":
    main()