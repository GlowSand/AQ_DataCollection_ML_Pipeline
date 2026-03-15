import argparse
import subprocess
import sys
from pathlib import Path
from state import compute_window, save_state, load_state, mark_in_progress, clear_in_progress
from metadata_tracker import PipelineRunTracker


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Incremental AQ pipeline orchestrator")
    p.add_argument("--cities",        default="Houston,TX",  help="Semicolon-separated City,ST pairs")
    p.add_argument("--out-dir",       default="data",        help="Output directory for CSVs")
    p.add_argument("--out-prefix",    default="aq",          help="Prefix for output CSV filenames")
    p.add_argument("--timezone",      default="America/Chicago")
    p.add_argument("--batch-size",    type=int, default=50)
    p.add_argument("--uszips",        default="uszips.csv")
    p.add_argument("--zip-traffic",   default=None,          help="Optional road-density CSV to join")
    p.add_argument("--refresh-static", dest="refresh_static", action="store_true",
                   help="Also regenerate static dimension tables (slow)")
    return p.parse_args()


def run(cmd: list[str], label: str) -> None:
    # Run a subprocess command and print its output, raising an error if it fails
    print(f"\n[pipeline] Starting : {label}")
    print(f"Command: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error running {label}: {result.stderr}")
        sys.exit(result.returncode)
    print(f"{label} output:\n{result.stdout}")
    
    
def main():
    args = parse_args()
    python = sys.executable # reusing the same venv that used for this script
    
    # First, we have to compute the date window to fetch data for, based on our state
    start, end = compute_window()
    
    
    if  start is None:
        print("[pipeline] Already up to date — nothing to ingest.")
    else:
        print(f"[pipeline] Date window: {start}  →  {end}")
        out_dir = Path(args.out_dir)

        # Build the collect command
        collect_cmd = [
            python, "collect.py",
            "--start-date", start,
            "--end-date", end,
            "--cities", args.cities,
            "--out-dir", args.out_dir,
            "--out-prefix", args.out_prefix,
            "--timezone", args.timezone,
            "--batch-size", str(args.batch_size),
            "--uszips", args.uszips,
        ]
        if args.zip_traffic:
            collect_cmd += ["--zip-traffic", args.zip_traffic]

        # --- Layer 1: metadata-based duplicate check ---
        if PipelineRunTracker.is_window_already_collected(out_dir, start, end):
            print(f"[pipeline] Window {start} → {end} already successfully collected. Skipping.")
        else:
            # --- Layer 2: crash-recovery lock ---
            prior = load_state().get("in_progress")
            if prior:
                print(
                    f"[pipeline] WARNING: A previous run for {prior['start']} → {prior['end']} "
                    f"started at {prior['started_at']} did not finish cleanly.\n"
                    f"           Partial output files may exist in {args.out_dir}.\n"
                    f"           Proceeding with a fresh run for the same window."
                )

            mark_in_progress(start, end)
            try:
                run(collect_cmd, "collect.py (AQI + weather collection)")
                # Update state to reflect we've ingested data up to 'end' date
                save_state(end)
                print(f"[pipeline] Checkpoint saved. Next run starts from {end} + 1 day.")
            finally:
                clear_in_progress()
        
        
    # ── Optional: static dimension tables (slow, run on demand) ─────────────
    # Video mentioned use of static dimension tables, not sure if this is correct though
    if args.refresh_static:
        run([python, "collect_population.py"],
            "collect_population.py")

        run([
            python, "dump_pollution_sources.py",
            "--state",   "TX",
            "--uszips",  args.uszips,
            "--out-dir", args.out_dir,
        ], "dump_pollution_sources.py")

        run([
            python, "dump_zip_road_density.py",
            "--cities",  args.cities,
            "--uszips",  args.uszips,
            "--out-dir", args.out_dir,
        ], "dump_zip_road_density.py")


if __name__ == "__main__":
    main()