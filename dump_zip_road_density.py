# pip install osmnx geopandas shapely pyproj rtree pandas

import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd
import osmnx as ox
import numpy as np

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


def build_latlon_grid(lat_min, lat_max, lon_min, lon_max, step_deg=0.02) -> pd.DataFrame:
    lats = np.arange(lat_min, lat_max + 1e-12, step_deg)
    lons = np.arange(lon_min, lon_max + 1e-12, step_deg)

    lat_grid, lon_grid = np.meshgrid(lats, lons, indexing="ij")
    df = pd.DataFrame({"latitude": lat_grid.ravel(), "longitude": lon_grid.ravel()})

    df["grid_id"] = (
            "lat=" + df["latitude"].round(5).astype(str) +
            "_lon=" + df["longitude"].round(5).astype(str)
    )
    return df[["grid_id", "latitude", "longitude"]]


def dump_grid_road_density(loc_df: pd.DataFrame, output_file: Path, radius_m: int, batch_size: int, tag: str):
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
                "tag": tag,
                "grid_id": row["grid_id"],
                "latitude": lat,
                "longitude": lon,
                feature_name: total_len_m,
            })

        out_df = pd.DataFrame(rows)
        out_df.to_csv(output_file, mode="a", header=first_write, index=False)
        first_write = False

        print(f"Saved batch of {len(batch)} locations -> {output_file.name}")

    print(f"\nDONE: saved to {output_file}")


def parse_args():
    p = argparse.ArgumentParser(description="Dump GRID->road density CSV (static dimension table).")

    p.add_argument("--lat-min", type=float, default=29.40, help="Bounding box south latitude (default Houston metro)")
    p.add_argument("--lat-max", type=float, default=30.15, help="Bounding box north latitude (default Houston metro)")
    p.add_argument("--lon-min", type=float, default=-95.90, help="Bounding box west longitude (default Houston metro)")
    p.add_argument("--lon-max", type=float, default=-94.95, help="Bounding box east longitude (default Houston metro)")
    p.add_argument("--step-deg", type=float, default=0.06, help="Grid step in degrees (0.01~1.1km, 0.02~2.2km)")
    p.add_argument("--tag", default="houston_grid", help="Label for output tag/city column")

    p.add_argument("--radius-m", type=int, default=1000, help="Radius in meters")
    p.add_argument("--batch-size", type=int, default=25, help="Locations per OSM batch (smaller is safer)")
    p.add_argument("--out-dir", default="data", help="Output directory")
    p.add_argument("--out-prefix", default="grid_road_density", help="Output filename prefix")
    return p.parse_args()


def main():
    args = parse_args()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.out_dir)
    out_dir.mkdir(exist_ok=True)
    output_file = out_dir / f"{args.out_prefix}_{args.radius_m}m_{timestamp}.csv"

    loc_df = build_latlon_grid(
        lat_min=args.lat_min,
        lat_max=args.lat_max,
        lon_min=args.lon_min,
        lon_max=args.lon_max,
        step_deg=args.step_deg,
    )

    dump_grid_road_density(
        loc_df=loc_df,
        output_file=output_file,
        radius_m=args.radius_m,
        batch_size=args.batch_size,
        tag=args.tag,
    )


if __name__ == "__main__":
    main()
