import datetime
import json
import subprocess
import time
import sys
import os

DRY_RUN = True  # Set to False to actually trigger the Airflow runs

def generate_ranges():
    # 1. Video Library: 5-day ranges from 2026-01-01 to 2026-05-23
    start_date = datetime.date(2026, 1, 1)
    end_limit = datetime.date(2026, 5, 23)
    lib_ranges = []
    curr = start_date
    while curr <= end_limit:
        next_end = min(curr + datetime.timedelta(days=4), end_limit)
        lib_ranges.append((curr.strftime('%Y-%m-%d'), next_end.strftime('%Y-%m-%d')))
        curr = next_end + datetime.timedelta(days=1)
        
    # 2. Hashtags: weekly ranges from 2026-02-23 to 2026-05-23
    start_hash = datetime.date(2026, 2, 23)
    hash_ranges = []
    curr = start_hash
    while curr <= end_limit:
        next_end = curr + datetime.timedelta(days=6)
        hash_ranges.append(f"{curr.strftime('%Y%m%d')}-{next_end.strftime('%Y%m%d')}")
        curr = next_end + datetime.timedelta(days=1)
        
    # 3. Video Selling: monthly ranges from 2025-12-01 to 2026-05-23
    sell_ranges = [
        "20251201-20251231",
        "20260101-20260131",
        "20260201-20260228",
        "20260301-20260331",
        "20260401-20260430",
        "20260501-20260523"
    ]
    
    return lib_ranges, hash_ranges, sell_ranges

def build_runs():
    lib_ranges, hash_ranges, sell_ranges = generate_ranges()
    total_runs = len(lib_ranges)
    
    runs = []
    for i in range(total_runs):
        lib_start, lib_end = lib_ranges[i]
        
        hashtag_val = hash_ranges[i] if i < len(hash_ranges) else "skip"
        selling_val = sell_ranges[i] if i < len(sell_ranges) else "skip"
        
        run_config = {
            "library_start_date": lib_start,
            "library_end_date": lib_end,
            "hashtag_week_range": hashtag_val,
            "selling_month_range": selling_val
        }
        
        runs.append((i+1, lib_start, run_config))
        
    return runs

def check_previous_runs(run_prefix):
    try:
        from airflow.models import DagRun
        from airflow.utils.session import create_session
        with create_session() as session:
            runs = session.query(DagRun).filter(
                DagRun.dag_id == 'echotik_data_collection',
                DagRun.run_id.like(f"{run_prefix}%")
            ).all()
            return [
                {
                    'run_id': r.run_id,
                    'state': str(r.state) if r.state else ''
                }
                for r in runs
            ]
    except Exception as e:
        print(f"Error listing runs for prefix {run_prefix}: {e}", file=sys.stderr, flush=True)
    return []

def get_dag_run_state(run_id):
    try:
        from airflow.models import DagRun
        from airflow.utils.session import create_session
        with create_session() as session:
            run = session.query(DagRun).filter(
                DagRun.dag_id == 'echotik_data_collection',
                DagRun.run_id == run_id
            ).first()
            if run:
                return str(run.state) if run.state else None
    except Exception as e:
        print(f"Error checking state for {run_id}: {e}", file=sys.stderr, flush=True)
    return None

def trigger_dag_run(run_id, config):
    conf_str = json.dumps(config)
    cmd = [
        "airflow", "dags", "trigger", "echotik_data_collection",
        "-r", run_id,
        "-c", conf_str
    ]
    print(f"Executing: {' '.join(cmd)}", flush=True)
    if DRY_RUN:
        return True
    try:
        env = os.environ.copy()
        env["AIRFLOW__LOGGING__LOGGING_LEVEL"] = "WARNING"
        subprocess.check_call(cmd, env=env)
        return True
    except Exception as e:
        print(f"Error triggering DAG: {e}", file=sys.stderr, flush=True)
        return False

def main():
    global DRY_RUN
    if len(sys.argv) > 1 and sys.argv[1] == "--execute":
        DRY_RUN = False
        
    print(f"Starting pipeline orchestration (DRY_RUN = {DRY_RUN})", flush=True)
    runs = build_runs()
    print(f"Generated {len(runs)} sequential runs to execute.", flush=True)
    
    if DRY_RUN:
        for idx, (run_num, lib_start, config) in enumerate(runs):
            run_id = f"backfill_run_{run_num:02d}_lib_{lib_start.replace('-', '')}"
            print(f"\n[{idx+1}/{len(runs)}] Run ID: {run_id}", flush=True)
            print(f"  Conf: {json.dumps(config, indent=2)}", flush=True)
        print("\nDry-run complete. Run with --execute option to trigger actual DAG runs.", flush=True)
        return

    for idx, (run_num, lib_start, config) in enumerate(runs):
        print(f"\n==========================================", flush=True)
        print(f"Processing Run {run_num}/{len(runs)} (index {idx+1})", flush=True)
        print(f"==========================================", flush=True)
        
        prefix = f"backfill_run_{run_num:02d}_"
        matching = check_previous_runs(prefix)
        
        # Check if any matching run succeeded
        if any(m['state'] == 'success' for m in matching):
            print(f"Run {run_num} has already succeeded in a previous execution. Skipping.", flush=True)
            continue
            
        # Check if any matching run is currently running or queued
        active_runs = [m for m in matching if m['state'] in ('running', 'queued')]
        if active_runs:
            run_id = active_runs[0]['run_id']
            print(f"Run {run_num} is already active/running with ID: {run_id}. Waiting for it...", flush=True)
        else:
            # All previous runs for this range failed or none exist. Trigger new attempt.
            attempt = len(matching) + 1
            run_id = f"backfill_run_{run_num:02d}_lib_{lib_start.replace('-', '')}_att{attempt}"
            print(f"Triggering Run {run_num} (Attempt {attempt}) with ID: {run_id}", flush=True)
            success = trigger_dag_run(run_id, config)
            if not success:
                print("Failed to trigger run. Exiting orchestrator.", file=sys.stderr, flush=True)
                sys.exit(1)
            time.sleep(5)  # Wait for Airflow to register it
            
        # Poll state until it finishes
        while True:
            state = get_dag_run_state(run_id)
            print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Run ID '{run_id}' current state: {state}", flush=True)
            
            if state in ('success', 'failed'):
                if state == 'failed':
                    print(f"DAG Run {run_id} failed! Stopping orchestration to prevent corrupt or partial data.", file=sys.stderr, flush=True)
                    sys.exit(1)
                print(f"DAG Run {run_id} completed successfully. Proceeding to next run.", flush=True)
                break
                
            time.sleep(15)  # Poll every 15 seconds

if __name__ == "__main__":
    main()
