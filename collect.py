# pip install requests_cache retry_requests openmeteo-requests pandas
import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests_cache
from retry_requests import retry
import openmeteo_requests

# Open Meteo client setup
cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

AQ_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

HOURLY_VARS = [
    "us_aqi",
    "pm10",
    "pm2_5",
    "carbon_monoxide",
    "nitrogen_dioxide",
    "sulphur_dioxide",
    "ozone",
    "uv_index_clear_sky",
    "uv_index",
    "dust",
    "aerosol_optical_depth",
    "ammonia",
    "alder_pollen",
    "birch_pollen",
    "grass_pollen",
    "mugwort_pollen",
    "olive_pollen",
    "ragweed_pollen"
]


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


def fetch_and_save_csv(
        loc_df: pd.DataFrame,
        start_date: str,
        end_date: str,
        output_file: Path,
        timezone: str,
        batch_size: int,
):
    first_write = True

    has_traffic = "traffic_density" in loc_df.columns

    for batch in chunked(loc_df, batch_size):
        params = {
            "latitude": batch["latitude"].tolist(),
            "longitude": batch["longitude"].tolist(),
            "hourly": HOURLY_VARS,
            "start_date": start_date,
            "end_date": end_date,
            "timezone": timezone,
        }

        responses = openmeteo.weather_api(AQ_URL, params=params)

        batch_frames = []
        for i, resp in enumerate(responses):
            hourly = resp.Hourly()

            times = pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left",
            ).tz_convert(timezone)

            row = batch.iloc[i]
            data = {
                "city": row["city"],
                "state": row["state"],
                "zip": row["zip"],
                "latitude": row["latitude"],
                "longitude": row["longitude"],
                "time": times,
            }

            if has_traffic:
                data["traffic_density"] = row["traffic_density"]

            for j, var in enumerate(HOURLY_VARS):
                data[var] = hourly.Variables(j).ValuesAsNumpy()

            batch_frames.append(pd.DataFrame(data))

        batch_df = pd.concat(batch_frames, ignore_index=True)

        batch_df.to_csv(output_file, mode="a", header=first_write, index=False)
        first_write = False

        print(f"Saved batch of {len(batch)} ZIPs -> {output_file.name}")

    print(f"\nDONE: saved to {output_file}")


def parse_args():
    p = argparse.ArgumentParser(
        description="Bulk historical air quality pull from Open-Meteo for all ZIP centroids in one or more City,ST pairs."
    )
    p.add_argument(
        "--cities",
        required=True,
        help='Semicolon-separated list like "Austin,TX;Houston,TX"',
    )
    p.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    p.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    p.add_argument("--timezone", default="America/Chicago", help="IANA timezone")
    p.add_argument("--batch-size", type=int, default=50, help="ZIPs per API request (25-100 recommended)")
    p.add_argument("--uszips", default="uszips.csv",
                   help="Path to simplemaps uszips.csv, you can get this at https://simplemaps.com/data/us-zips")
    p.add_argument("--zip-traffic", default=None,
                   help="Optional: CSV with columns zip,traffic_density to augment features (your precomputed static file).")
    p.add_argument("--out-dir", default="data", help="Output directory")
    p.add_argument("--out-prefix", default="multi", help="Output filename prefix")
    return p.parse_args()


def main():
    args = parse_args()

    # timestamped output file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.out_dir)
    out_dir.mkdir(exist_ok=True)
    output_file = out_dir / f"{args.out_prefix}_air_quality_hourly_{timestamp}.csv"

    pairs = parse_city_state_list(args.cities)

    # Load ZIPs for each city/state and combine
    all_frames = []
    for city, st in pairs:
        sub = get_zip_centroids(city, st, uszips_csv_path=args.uszips)

        if sub.empty:
            print(f"WARNING: No ZIP centroids found for the city: {city} in the state: {st}. Skipping gracefully...")
            continue

        sub["city"] = city
        sub["state"] = st
        all_frames.append(sub)

    if not all_frames:
        raise SystemExit("No ZIP centroids found for all provided cities and states.")

    loc_df = pd.concat(all_frames, ignore_index=True)

    if args.zip_traffic:
        traffic_df = pd.read_csv(args.zip_traffic)
        if "zip" not in traffic_df.columns or "traffic_density" not in traffic_df.columns:
            raise SystemExit("Your --zip-traffic file must have at least columns: zip, traffic_density")

        traffic_df = traffic_df[["zip", "traffic_density"]].drop_duplicates(subset=["zip"])
        loc_df = loc_df.merge(traffic_df, on="zip", how="left")

    fetch_and_save_csv(
        loc_df=loc_df,
        start_date=args.start_date,
        end_date=args.end_date,
        output_file=output_file,
        timezone=args.timezone,
        batch_size=args.batch_size,
    )


if __name__ == "__main__":
    main()
