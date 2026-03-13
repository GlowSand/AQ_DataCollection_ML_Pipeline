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

    # 1st method - Initiallization of all metadata captured for every record within the json file.
    def start(self, args, script: str = "unknown") -> None:
        """Call at the very beginning of main(), before any work starts."""
        now = datetime.now() 
        
        # Serializing CLI arguments into a dictionary
        params = {
            k: str(v) if isinstance(v, Path) else v
            for k, v in vars(args).items()
        }
        
        
        
        # Skeleton of the metadata record(s).
        """ Atm, we are only recording metadata for data ingestion. 
        
        Later, we need to expand this to include metadata for the data processing and analysis steps as well. """
        self._record = {
            "run_id": now.strftime("%Y%m%d_%H%M%S"), # Unique ID is based on timestamp
            "script": script,
            "status": "in_progress",
            "started_at": now.isoformat(), # when pipeline run started
            "finished_at": None,
            "duration_seconds": None,
            "parameters": params,
            "locations": {
                "total_zip_centroids": None,
                "cities_found": [],
                "cities_skipped": [],
            },
            "outputs": [],
            "error": None,
        }
        self._start_time = now


    # 2nnd method - When this run happened, what geographic locations did we actually end up querying, and did we miss any?
    def record_locations(
        self,
        loc_df, #The combined DataFrame of all ZIP centroids that will be queried.
        skipped_cities: list[str], #City/state strings (e.g. "Houston,TX") for which no ZIPs were found.
        cities_found: list[str] | None = None, #Explicit list of resolved city/state strings. 
                                                # When None, the method tries to derive them from "city" and "state" columns in loc_df. Pass explicitly for scripts whose DataFrames lack those columns.
    ) -> None:
        
        
        # First, we have to figure oout what cities we actually found ZIP centroids for.
        if cities_found is None:
            if "city" in loc_df.columns and "state" in loc_df.columns:
                cities_found = sorted(set(
                    f"{r['city']},{r['state']}"
                    for _, r in loc_df[["city", "state"]].drop_duplicates().iterrows()
                ))
            else:
                cities_found = []

        # Then we overwrite the "locations" placeholder within start() with the actual values.
        self._record["locations"] = {
            "total_zip_centroids": len(loc_df),
            "cities_found": cities_found,
            "cities_skipped": skipped_cities,
        }


    # 3rd method - For each file that was produced, what's in it, how big is it, and where did the data come from?
    def record_output(
        self,
        output_type: str, # Human-readable label, e.g. "air_quality" or "weather".
        output_file: Path, # file path oof the CSV that was written to.
        variables: list[str], # The list of hourly variable names that were fetched.
        api_url: str, # API URL used
        batch_size_used: int, # how many ZIP codes were queried at once per API request
    ) -> None:
        """ After a program actually fetches data and saves a CSV, we call record_output() to log the details about that output file and the API call that produced it. 
    
        We can call this multiple times per run (e.g. once for air quality data, once for weather data). """
        
        
        
        row_count = None
        size_bytes = None

        # Check if the file exist, then measure the file size (in bytes) and the number of row (excluding header).
        if output_file.exists():
            size_bytes = output_file.stat().st_size
            # Count data rows (exclude header)
            with output_file.open("r") as f:
                row_count = sum(1 for _ in f) - 1

        
        
        # Appends new record to the "outputs" list within the self.record dictionary. Each record contains details about the output file and the API call that produced it.
        self._record["outputs"].append({
            "type": output_type,
            "file": str(output_file),
            "row_count": row_count,
            "size_bytes": size_bytes,
            "variable_count": len(variables),
            "variables": variables,
            "api_url": api_url,
            "batch_size_used": batch_size_used,
            # Variables names as is, you luck I didnt use anything like suckIt1 or stuffname2 ;)
        })

    # 4th method - fills in the last few fields (end time, duration, status), then triggers the actual write to disk.
    def finish(self, status: str = "success", error: str | None = None) -> None:
        """
        Call at the very end of main() (in a finally block is ideal).

        Parameters
        ----------
        status : str
            a string describing how the ru ended: "success", "error", or "partial".
        error : str | None
            Exception message if status == "error".
        """
        
        
        # Calculates the duration of run by subtracting the timestamp of the curret time from self._start_time.
        now = datetime.now()
        self._record["status"] = status #overwrites "in_progress" pllaceholder
        self._record["finished_at"] = now.isoformat()
        self._record["duration_seconds"] = round(
            (now - self._start_time).total_seconds(), 2
        )
        self._record["error"] = error #ngl I copied the error method from some stack overflow thread not sure if it uses HTTP error codes or some other shit

        self._append_to_log()
        print(f"\n[Metadata] Run '{self._record['run_id']}' logged -> {self._metadata_path}")

    # ------------------------------------------------------------------
    # Internal helpers ----->> where did we get these again?
    # ------------------------------------------------------------------


    # writes record to the pipeline_metadata.json file
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
