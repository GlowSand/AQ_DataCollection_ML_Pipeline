# pip install requests_cache retry_requests openmeteo-requests pandas
import argparse
from datetime import datetime
from pathlib import Path
import time
from collections import deque
import pandas as pd
import requests_cache
from retry_requests import retry
import openmeteo_requests
import math
import numpy as np

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


def build_latlon_grid(
        lat_min: float, lat_max: float,
        lon_min: float, lon_max: float,
        step_deg: float = 0.02
) -> pd.DataFrame:
    # include endpoints with a tiny epsilon
    lats = np.arange(lat_min, lat_max + 1e-12, step_deg)
    lons = np.arange(lon_min, lon_max + 1e-12, step_deg)

    lat_grid, lon_grid = np.meshgrid(lats, lons, indexing="ij")
    df = pd.DataFrame({
        "lat": lat_grid.ravel(),
        "lon": lon_grid.ravel(),
    })

    # stable id: rounded coordinates
    df["grid_id"] = (
            "lat=" + df["lat"].round(5).astype(str) +
            "_lon=" + df["lon"].round(5).astype(str)
    )
    return df[["grid_id", "lat", "lon"]]


def build_bbox_loc_df(lat_min: float, lat_max: float, lon_min: float, lon_max: float,
                      step_deg: float = 0.02, city_tag: str = "grid", state_tag: str = "TX") -> pd.DataFrame:
    g = build_latlon_grid(lat_min, lat_max, lon_min, lon_max, step_deg=step_deg)

    loc_df = pd.DataFrame({
        "city": city_tag,
        "state": state_tag,
        "zip": pd.NA,
        "latitude": g["lat"].astype(float),
        "longitude": g["lon"].astype(float),
        "grid_id": g["grid_id"],
    })
    return loc_df


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
                "latitude": row["latitude"],
                "longitude": row["longitude"],
                "time": times,
            }

            if "grid_id" in row.index:
                data["grid_id"] = row["grid_id"]

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
    p.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    p.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    p.add_argument("--lat-min", type=float, default=29.40, help="Bounding box south latitude (default Houston metro)")
    p.add_argument("--lat-max", type=float, default=30.15, help="Bounding box north latitude (default Houston metro)")
    p.add_argument("--lon-min", type=float, default=-95.90, help="Bounding box west longitude (default Houston metro)")
    p.add_argument("--lon-max", type=float, default=-94.95, help="Bounding box east longitude (default Houston metro)")
    p.add_argument("--step-deg", type=float, default=0.02, help="Grid step in degrees (0.01~1.1km, 0.02~2.2km)")
    p.add_argument("--tag", default="grid", help="Label for city column when using grid sampling")
    p.add_argument("--timezone", default="America/Chicago", help="IANA timezone")
    p.add_argument("--out-dir", default="data", help="Output directory")
    p.add_argument("--out-prefix", default="multi", help="Output filename prefix")
    p.add_argument("--batch-size", type=int, default=40, help="Locations per API request (capped by weight rule)")
    return p.parse_args()


def main():
    args = parse_args()

    # timestamped output file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.out_dir)
    out_dir.mkdir(exist_ok=True)
    output_file = out_dir / f"{args.out_prefix}_air_quality_hourly_{timestamp}.csv"
    output_file_weather = out_dir / f"{args.out_prefix}_weather_hourly_{timestamp}.csv" # added by rparaula to create output file for weather data

    loc_df = build_bbox_loc_df(
        lat_min=args.lat_min,
        lat_max=args.lat_max,
        lon_min=args.lon_min,
        lon_max=args.lon_max,
        step_deg=args.step_deg,
        city_tag="Houston",
        state_tag="TX",
    )

    # added by rparaula to implement separate batch size computation for air quality and weather variables, since they have different variable counts and thus different weights
    safe_bs = compute_safe_batch_size(HOURLY_VARS, args.start_date, args.end_date)
    safe_bs_weather = compute_safe_batch_size(WEATHER_HOURLY_VARS, args.start_date, args.end_date) # added by rparaula to compute safe batch size for weather variables
    batch_size = min(args.batch_size, safe_bs)
    batch_size_weather = min(args.batch_size, safe_bs_weather) # added by rparaula to compute batch size for weather variables

    print(
        f"bbox=({args.lat_min},{args.lon_min})..({args.lat_max},{args.lon_max}) "
        f"step={args.step_deg} locations={len(loc_df)} vars={len(HOURLY_VARS)} "
        f"safe_batch<={safe_bs} using_batch={batch_size}"
    )

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

if __name__ == "__main__":
    main()
