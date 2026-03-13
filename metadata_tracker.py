"""
Metadata Tracker for the AQ Data Collection Pipeline.

Appends a structured JSON record to `data/pipeline_metadata.json` for every
pipeline run, capturing:
  - Run identity & timing
  - Input parameters (cities, date range, batch config)
  - Location coverage (ZIP centroids found / skipped)
  - Output files (path, row count, file size, variables, API source)
  - Final status and any error message

Usage (from collect.py):
    tracker = PipelineRunTracker(out_dir=Path("data"))
    tracker.start(args)
    tracker.record_locations(loc_df, skipped_cities)
    tracker.record_output("air_quality", output_file, HOURLY_VARS, AQ_URL, batch_size)
    tracker.record_output("weather", output_file_weather, WEATHER_HOURLY_VARS, WEATHER_URL, batch_size_weather)
    tracker.finish(status="success")   # or tracker.finish(status="error", error=str(e))
"""

import json
from datetime import datetime
from pathlib import Path


METADATA_FILE = "pipeline_metadata.json"


class PipelineRunTracker:
    def __init__(self, out_dir: Path):
        self._out_dir = out_dir
        self._metadata_path = out_dir / METADATA_FILE
        self._record: dict = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self, args) -> None:
        """Call at the very beginning of main(), before any work starts."""
        now = datetime.now()
        self._record = {
            "run_id": now.strftime("%Y%m%d_%H%M%S"),
            "status": "in_progress",
            "started_at": now.isoformat(),
            "finished_at": None,
            "duration_seconds": None,
            "parameters": {
                "cities_raw": args.cities,
                "start_date": args.start_date,
                "end_date": args.end_date,
                "timezone": args.timezone,
                "batch_size_requested": args.batch_size,
                "zip_traffic_file": args.zip_traffic,
                "uszips_file": args.uszips,
                "out_dir": str(args.out_dir),
                "out_prefix": args.out_prefix,
            },
            "locations": {
                "total_zip_centroids": None,
                "cities_found": [],
                "cities_skipped": [],
            },
            "outputs": [],
            "error": None,
        }
        self._start_time = now

    def record_locations(self, loc_df, skipped_cities: list[str]) -> None:
        """
        Call after the ZIP centroid loading loop.

        Parameters
        ----------
        loc_df : pd.DataFrame
            The combined DataFrame of all ZIP centroids that will be queried.
        skipped_cities : list[str]
            City/state strings (e.g. "Houston,TX") for which no ZIPs were found.
        """
        cities_found = [
            f"{city},{state}"
            for city, state in zip(loc_df["city"].unique(), loc_df["state"].unique())
        ]
        self._record["locations"] = {
            "total_zip_centroids": len(loc_df),
            "cities_found": sorted(set(
                f"{r['city']},{r['state']}" for _, r in loc_df[["city", "state"]].drop_duplicates().iterrows()
            )),
            "cities_skipped": skipped_cities,
        }

    def record_output(
        self,
        output_type: str,
        output_file: Path,
        variables: list[str],
        api_url: str,
        batch_size_used: int,
    ) -> None:
        """
        Call once per fetch_and_save_csv() call, after it completes.

        Parameters
        ----------
        output_type : str
            Human-readable label, e.g. "air_quality" or "weather".
        output_file : Path
            The Path object that was written to.
        variables : list[str]
            The list of hourly variable names that were fetched.
        api_url : str
            The Open-Meteo endpoint URL used.
        batch_size_used : int
            The final (rate-limit-capped) batch size.
        """
        row_count = None
        size_bytes = None

        if output_file.exists():
            size_bytes = output_file.stat().st_size
            # Count data rows (exclude header)
            with output_file.open("r") as f:
                row_count = sum(1 for _ in f) - 1

        self._record["outputs"].append({
            "type": output_type,
            "file": str(output_file),
            "row_count": row_count,
            "size_bytes": size_bytes,
            "variable_count": len(variables),
            "variables": variables,
            "api_url": api_url,
            "batch_size_used": batch_size_used,
        })

    def finish(self, status: str = "success", error: str | None = None) -> None:
        """
        Call at the very end of main() (in a finally block is ideal).

        Parameters
        ----------
        status : str
            "success", "error", or "partial".
        error : str | None
            Exception message if status == "error".
        """
        now = datetime.now()
        self._record["status"] = status
        self._record["finished_at"] = now.isoformat()
        self._record["duration_seconds"] = round(
            (now - self._start_time).total_seconds(), 2
        )
        self._record["error"] = error

        self._append_to_log()
        print(f"\n[Metadata] Run '{self._record['run_id']}' logged -> {self._metadata_path}")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _append_to_log(self) -> None:
        """Load existing log (if any), append this run's record, and save."""
        self._out_dir.mkdir(exist_ok=True)

        if self._metadata_path.exists():
            try:
                existing = json.loads(self._metadata_path.read_text(encoding="utf-8"))
                if not isinstance(existing, list):
                    existing = [existing]
            except (json.JSONDecodeError, OSError):
                existing = []
        else:
            existing = []

        existing.append(self._record)
        self._metadata_path.write_text(
            json.dumps(existing, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
