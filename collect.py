# pip install requests_cache retry_requests openmeteo-requests pandas
import argparse
from datetime import datetime
from pathlib import Path
from metadata_tracker import PipelineRunTracker
import time
from collections import deque
import pandas as pd
import requests_cache
from retry_requests import retry
import openmeteo_requests
import math

# Open Meteo client setup
cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

AQ_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
WEATHER_URL = "https://archive-api.open-meteo.com/v1/archive" #added by rparaula to implement open meteo's weather API

MAX_WEIGHT_PER_MIN = 600.0
WINDOW_SECONDS = 60

# Remember these are for ARCHIVE endpoints, forecast endpoints will be implemented later

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

WEATHER_HOURLY_VARS = [
    "temperature_2m",        # Air temperature at 2 meters above ground
    "relative_humidity_2m",  # Relative humidity at 2 meters above ground
    "precipitation",         # Total precipitation (rain + snow) sum of the preceding hour
    "wind_speed_10m",        # Wind speed at 10 meters above ground (standard level)
    "wind_speed_100m",       # Wind speed at 100 meters above ground (archive-supported; replaces 80m/180m)
    "wind_direction_10m",    # Wind direction at 10 meters above ground
    "wind_direction_100m",   # Wind direction at 100 meters above ground (archive-supported; replaces 80m/180m)
    "wind_gusts_10m",        # Wind gusts at 10 meters above ground (max of preceding hour)
    "shortwave_radiation",   # Shortwave solar radiation as average of the preceding hour
    "diffuse_radiation",     # Diffuse solar radiation as average of the preceding hour
    "cloud_cover",           # Total cloud cover as an area fraction
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


class WeightRateLimiter:
    """
    Sliding-window limiter: total weight over the last `window_seconds`
    must not exceed `max_weight`.
    """

    def __init__(self, max_weight: float = 600.0, window_seconds: int = 60):
        self.max_weight = float(max_weight)
        self.window_seconds = int(window_seconds)
        self.events = deque()  # (timestamp_monotonic, weight)

    def _prune(self, now: float) -> None:
        cutoff = now - self.window_seconds
        while self.events and self.events[0][0] <= cutoff:
            self.events.popleft()

    def used_weight(self) -> float:
        now = time.monotonic()
        self._prune(now)
        return sum(w for _, w in self.events)

    def acquire(self, weight: float) -> None:
        weight = float(weight)
        if weight <= 0:
            return

        while True:
            now = time.monotonic()
            self._prune(now)

            used = sum(w for _, w in self.events)
            if used + weight <= self.max_weight:
                self.events.append((now, weight))
                return

            # Need to wait until enough weight expires from the window
            # Compute minimal sleep based on earliest event(s).
            # Simple strategy: sleep until the oldest event expires.
            oldest_t, _ = self.events[0]
            sleep_for = (oldest_t + self.window_seconds) - now
            sleep_for = max(sleep_for, 0.01)
            time.sleep(sleep_for)


def compute_request_weight(num_vars: int, days: int, locations: int) -> float:
    per_loc = max(num_vars / 10.0, (num_vars / 10.0) * (days / 7.0))
    return per_loc * locations



# rparaula replaced everything above to reimplemnt zipcode-based logic

def compute_safe_batch_size(hourly_vars: list[str], start_date: str, end_date: str,
                            max_weight_per_min: int = 600, safety: float = 0.9) -> int:
    """
    Weight rule: weight = max(V/10, (V/10)*(days/7)) * locations
    """
    V = len(hourly_vars)

    d0 = pd.to_datetime(start_date)
    d1 = pd.to_datetime(end_date)
    # Open-Meteo uses inclusive date ranges in many endpoints; treat as inclusive
    days = int((d1 - d0).days) + 1
    days = max(days, 1)

    weight_per_location = max(V / 10.0, (V / 10.0) * (days / 7.0))
    max_locations = (max_weight_per_min / weight_per_location) * safety
    return max(1, int(math.floor(max_locations)))


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
        url: str, # Added by rparaula for dynamic open meteo queries
        hourly_vars: list[str],
):
    first_write = True

    limiter = WeightRateLimiter(max_weight=600.0, window_seconds=60)

    d0 = pd.to_datetime(start_date)
    d1 = pd.to_datetime(end_date)
    days = int((d1 - d0).days) + 1
    days = max(days, 1)

    for batch in chunked(loc_df, batch_size):
        req_weight = compute_request_weight(
            num_vars=len(hourly_vars),
            days=days,
            locations=len(batch),
        )

        limiter.acquire(req_weight)

        params = {
            "latitude": batch["latitude"].tolist(),
            "longitude": batch["longitude"].tolist(),
            "hourly": hourly_vars,
            "start_date": start_date,
            "end_date": end_date,
            "timezone": timezone,
        }

        responses = openmeteo.weather_api(url, params=params)

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

            for j, var in enumerate(hourly_vars):
                data[var] = hourly.Variables(j).ValuesAsNumpy()

            batch_frames.append(pd.DataFrame(data))

        batch_df = pd.concat(batch_frames, ignore_index=True)

        batch_df.to_csv(output_file, mode="a", header=first_write, index=False)
        first_write = False

        print(f"Saved batch of {len(batch)} locations -> {output_file.name}")

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
    output_file_weather = out_dir / f"{args.out_prefix}_weather_hourly_{timestamp}.csv" # added by rparaula to create output file for weather data

    tracker = PipelineRunTracker(out_dir=out_dir)
    tracker.start(args)

    pairs = parse_city_state_list(args.cities)

    # Load ZIPs for each city/state and combine
    all_frames = []
    skipped_cities = []
    for city, st in pairs:
        sub = get_zip_centroids(city, st, uszips_csv_path=args.uszips)

        if sub.empty:
            print(f"WARNING: No ZIP centroids found for the city: {city} in the state: {st}. Skipping gracefully...")
            skipped_cities.append(f"{city},{st}")
            continue

        sub["city"] = city
        sub["state"] = st
        all_frames.append(sub)

    if not all_frames:
        tracker.finish(status="error", error="No ZIP centroids found for all provided cities and states.")
        raise SystemExit("No ZIP centroids found for all provided cities and states.")

    loc_df = pd.concat(all_frames, ignore_index=True)
    tracker.record_locations(loc_df, skipped_cities)

    if args.zip_traffic:
        traffic_df = pd.read_csv(args.zip_traffic)
        if "zip" not in traffic_df.columns or "traffic_density" not in traffic_df.columns:
            raise SystemExit("Your --zip-traffic file must have at least columns: zip, traffic_density")

        traffic_df = traffic_df[["zip", "traffic_density"]].drop_duplicates(subset=["zip"])
        loc_df = loc_df.merge(traffic_df, on="zip", how="left")

    # added by rparaula to implement separate batch size computation for air quality and weather variables, since they have different variable counts and thus different weights
    safe_bs = compute_safe_batch_size(HOURLY_VARS, args.start_date, args.end_date)
    safe_bs_weather = compute_safe_batch_size(WEATHER_HOURLY_VARS, args.start_date, args.end_date) # added by rparaula to compute safe batch size for weather variables
    batch_size = min(args.batch_size, safe_bs)
    batch_size_weather = min(args.batch_size, safe_bs_weather) # added by rparaula to compute batch size for weather variables

   

    try:
        fetch_and_save_csv(
            loc_df=loc_df,
            start_date=args.start_date,
            end_date=args.end_date,
            output_file=output_file,
            timezone=args.timezone,
            batch_size=batch_size,
            url=AQ_URL,
            hourly_vars=HOURLY_VARS,
        )
        tracker.record_output("air_quality", output_file, HOURLY_VARS, AQ_URL, batch_size)

        # second pass to fetch weather data for the same locations and time range, using the same batching and rate limiting logic
        fetch_and_save_csv(
            loc_df=loc_df,
            start_date=args.start_date,
            end_date=args.end_date,
            output_file=output_file_weather,
            timezone=args.timezone,
            batch_size=batch_size_weather,
            url=WEATHER_URL,
            hourly_vars=WEATHER_HOURLY_VARS,
        )
        tracker.record_output("weather", output_file_weather, WEATHER_HOURLY_VARS, WEATHER_URL, batch_size_weather)

        tracker.finish(status="success")
    except Exception as e:
        tracker.finish(status="error", error=str(e))
        raise

if __name__ == "__main__":
    main()
