import json
from pathlib import Path
from datetime import date, timedelta, datetime, datetime

STATE_FILE = Path("state.json")
BACKFILL_START = "2026-02-01"  # first-ever run starts here  

def load_state() -> dict:  
    if STATE_FILE.exists():         
        try:
            return json.loads(STATE_FILE.read_text())
        except json.JSONDecodeError:
            return {"last_ingested_date": None}                 
    return {"last_ingested_date": None}

def save_state(end_date: str) -> None:
    STATE_FILE.write_text(json.dumps({
        "last_ingested_date": end_date,
        "last_run_timestamp": date.today().isoformat(),
    }, indent=2))

def  mark_in_progress(start: str, end: str) -> None:
    """Write a lock so crashed runs can be detected on the next startup."""
    state = load_state()
    state["in_progress"] = {
        "start": start,   
        "end": end, 
        "started_at": datetime.now().isoformat(),       
    }
    STATE_FILE.write_text(json.dumps(state, indent=2))  

def clear_in_progress() -> None:
    """Remove the in-progress lock after a successful run."""
    state = load_state()
    state.pop("in_progress", None)
    STATE_FILE.write_text(json.dumps(state, indent=2))

def  compute_window() -> tuple[str, str]:
    state = load_state()
    last =  state.get("last_ingested_date")

    # Added by rparaula, hardcoding forecast windows to be only yesterday and today since idfk whats wrong with the automation at this point
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    end = date.today().isoformat()

    if last is None:
        start = yesterday  # no prior state: only collect yesterday + today
    else:
        start = (date.fromisoformat(last) + timedelta(days=1)).isoformat()

    # Never look back further than yesterday — keep window to at most 2 days
    if start <  yesterday:
        start = yesterday

    if start > end:  
        return None, None  # already up to date, nothing to fetch 

    return start, end

