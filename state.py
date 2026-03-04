#Small helper module to manage the state of the last ingested date, so that we can run this script daily and only fetch new data.
import json
from pathlib import Path
from datetime import date, timedelta

STATE_FILE = Path("state.json")
BACKFILL_START = "2026-02-01"  # first-ever run starts here

def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"last_ingested_date": None}

def save_state(end_date: str) -> None:
    STATE_FILE.write_text(json.dumps({
        "last_ingested_date": end_date,
        "last_run_timestamp": date.today().isoformat(),
    }, indent=2))

def compute_window() -> tuple[str, str]:
    state = load_state()
    last = state.get("last_ingested_date")

    if last is None:
        start = BACKFILL_START
    else:
        start = (date.fromisoformat(last) + timedelta(days=1)).isoformat()

    end = (date.today() - timedelta(days=1)).isoformat()  # yesterday

    if start > end:
        return None, None  # already up to date, nothing to fetch

    return start, end