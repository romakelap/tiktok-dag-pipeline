import json
import os
import sys
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


LOCAL_TZ = pendulum.timezone("Asia/Jakarta")

DAG_ID = "echotik_ml_daily_inference"

ML_SERVICE_DIR = Variable.get(
    "ML_SERVICE_DIR",
    default_var="/opt/airflow/ml-service",
)

ML_PYTHON_BIN = Variable.get(
    "ML_PYTHON_BIN",
    default_var=f"{ML_SERVICE_DIR}/.venv_airflow/bin/python",
)

ML_ENV_FILE = Variable.get(
    "ML_ENV_FILE",
    default_var=f"{ML_SERVICE_DIR}/.env.airflow",
)

ML_MIN_FEATURE_ROWS = Variable.get(
    "ML_MIN_FEATURE_ROWS",
    default_var="1",
)

MODEL_VERSION = Variable.get(
    "ML_MODEL_VERSION",
    default_var="v1.0",
)

# File path untuk simpan summary antar task (dibaca oleh notify_finish)
ML_SUMMARY_FILE = Variable.get(
    "ML_SUMMARY_FILE",
    default_var="/tmp/echotik_ml_daily_inference_summary.json",
)


def get_discord_notifier_safe():
    """
    Load Discord notifier dari plugin project:
    /opt/airflow/plugins/utils/monitoring/discord_notifier.py
    """
    try:
        plugin_path = "/opt/airflow/plugins"

        if os.path.exists(plugin_path) and plugin_path not in sys.path:
            sys.path.append(plugin_path)

        from utils.monitoring.discord_notifier import get_notifier

        return get_notifier()

    except Exception as error:
        print(f"[DISCORD] Failed to load DiscordNotifier: {error}")
        return None


def _read_summary_file():
    """Baca summary JSON yang ditulis oleh BashOperator. Aman kalau file ga ada."""
    try:
        if not os.path.exists(ML_SUMMARY_FILE):
            return {}

        with open(ML_SUMMARY_FILE, "r") as f:
            return json.load(f) or {}

    except Exception as error:
        print(f"[SUMMARY] Failed to read summary file: {error}")
        return {}


def _reset_summary_file():
    """Hapus summary file lama biar ga ke-carry dari run sebelumnya."""
    try:
        if os.path.exists(ML_SUMMARY_FILE):
            os.remove(ML_SUMMARY_FILE)
    except Exception as error:
        print(f"[SUMMARY] Failed to reset summary file: {error}")


def notify_start(**context):
    dag_run = context.get("dag_run")
    run_id = dag_run.run_id if dag_run else "manual"

    # Reset summary file di awal supaya notify_finish baca data run ini, bukan run lama
    _reset_summary_file()

    notifier = get_discord_notifier_safe()

    if notifier:
        notifier.send_task_started(
            task_name="ML Daily Inference Pipeline",
            dag_id=DAG_ID,
            run_id=run_id,
        )
    else:
        print("[DISCORD] Notifier unavailable. Skip start notification.")


def notify_finish(**context):
    dag_run = context.get("dag_run")
    run_id = dag_run.run_id if dag_run else "manual"

    summary = _read_summary_file()

    # Susun extra_info gabungan: metadata DAG + summary feature store + summary output
    extra_info = {
        "DAG": DAG_ID,
        "Run ID": run_id,
        "Model Version": MODEL_VERSION,
    }

    feature_check = summary.get("feature_check", {})
    if feature_check:
        extra_info["Feature Store Rows"] = feature_check.get("feature_rows", "-")
        extra_info["Fact Engagement Rows"] = feature_check.get("fact_rows", "-")
        extra_info["Active Tracked Accounts"] = feature_check.get(
            "active_tracked_accounts", "-"
        )

    recommendation_output = summary.get("recommendation_output", {})
    if recommendation_output:
        extra_info["Content Recommendations"] = recommendation_output.get(
            "content_count", "-"
        )
        extra_info["Hashtag Recommendations"] = recommendation_output.get(
            "hashtag_count", "-"
        )
        extra_info["Posting Schedule Recommendations"] = recommendation_output.get(
            "schedule_count", "-"
        )
        extra_info["Total Output"] = recommendation_output.get("total_output", "-")

    # Total records: prioritaskan total output rekomendasi
    records_count = recommendation_output.get("total_output") if recommendation_output else None

    notifier = get_discord_notifier_safe()

    if notifier:
        notifier.send_task_success(
            task_name="ML Daily Inference Pipeline",
            duration_sec=0,
            records_count=records_count,
            extra_info=extra_info,
        )
    else:
        print("[DISCORD] Notifier unavailable. Skip finish notification.")
        print(f"[SUMMARY] {json.dumps(extra_info, indent=2, default=str)}")


def notify_failure(context):
    task_instance = context.get("task_instance")
    exception = context.get("exception")

    task_id = task_instance.task_id if task_instance else "unknown_task"
    retry_count = task_instance.try_number if task_instance else 0

    notifier = get_discord_notifier_safe()

    if notifier:
        notifier.send_task_failed(
            task_name=f"{DAG_ID}.{task_id}",
            error_msg=str(exception),
            retry_count=retry_count,
        )
    else:
        print("[DISCORD] Notifier unavailable. Skip failure notification.")
        print(f"[ERROR] Task failed: {task_id}")
        print(f"[ERROR] {exception}")


default_args = {
    "owner": "nico",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": notify_failure,
}


COMMON_ENV = {
    "ML_SERVICE_DIR": ML_SERVICE_DIR,
    "ML_PYTHON_BIN": ML_PYTHON_BIN,
    "ML_ENV_FILE": ML_ENV_FILE,
    "ML_MIN_FEATURE_ROWS": ML_MIN_FEATURE_ROWS,
    "MODEL_VERSION": MODEL_VERSION,
    "ML_SUMMARY_FILE": ML_SUMMARY_FILE,
    "PYTHONUNBUFFERED": "1",
}


with DAG(
    dag_id=DAG_ID,
    description="DAG 3 - ML feature validation and daily inference recommendation pipeline",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1, 1, tz=LOCAL_TZ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["echotik", "tiktok-bi", "ml", "inference"],
) as dag:

    notify_start_task = PythonOperator(
        task_id="notify_start",
        python_callable=notify_start,
    )

    check_feature_store = BashOperator(
        task_id="check_feature_store",
        env=COMMON_ENV,
        append_env=True,
        bash_command=r'''
set -e

cd "${ML_SERVICE_DIR}"

export ML_ENV_FILE="${ML_ENV_FILE}"
export ML_MIN_FEATURE_ROWS="${ML_MIN_FEATURE_ROWS}"
export MODEL_VERSION="${MODEL_VERSION}"
export ML_SUMMARY_FILE="${ML_SUMMARY_FILE}"
export PYTHONUNBUFFERED=1

"${ML_PYTHON_BIN}" - <<'PY'
import json
import os
import sys
from sqlalchemy import text

sys.path.append(os.getcwd())

from db import get_engine

min_feature_rows = int(os.getenv("ML_MIN_FEATURE_ROWS", "1"))
summary_file = os.getenv("ML_SUMMARY_FILE", "/tmp/echotik_ml_daily_inference_summary.json")

engine = get_engine()

with engine.connect() as conn:
    feature_rows = conn.execute(
        text("SELECT COUNT(*) FROM vw_ml_feature_store")
    ).scalar()

    fact_rows = conn.execute(
        text("SELECT COUNT(*) FROM vw_fact_video_engagement")
    ).scalar()

    active_tracked_accounts = conn.execute(
        text("SELECT COUNT(*) FROM tracked_accounts WHERE is_active = 1")
    ).scalar()

# Validasi minimum
if feature_rows is None or feature_rows < min_feature_rows:
    raise Exception(
        f"Feature store row is too low. feature_rows={feature_rows}, min={min_feature_rows}"
    )

if fact_rows is None or fact_rows < min_feature_rows:
    raise Exception(
        f"Fact engagement row is too low. fact_rows={fact_rows}, min={min_feature_rows}"
    )

if active_tracked_accounts is None or active_tracked_accounts < 1:
    raise Exception("No active tracked accounts found.")

# Tulis ke summary file untuk dibaca notify_finish (dikirim ke Discord)
summary = {}
if os.path.exists(summary_file):
    try:
        with open(summary_file, "r") as f:
            summary = json.load(f) or {}
    except Exception:
        summary = {}

summary["feature_check"] = {
    "feature_rows": int(feature_rows),
    "fact_rows": int(fact_rows),
    "active_tracked_accounts": int(active_tracked_accounts),
}

with open(summary_file, "w") as f:
    json.dump(summary, f)
PY
''',
    )

    run_ml_inference_script = BashOperator(
        task_id="run_ml_inference_script",
        env=COMMON_ENV,
        append_env=True,
        bash_command=r'''
set -e

cd "${ML_SERVICE_DIR}"

export ML_ENV_FILE="${ML_ENV_FILE}"
export MODEL_VERSION="${MODEL_VERSION}"
export PYTHONUNBUFFERED=1

"${ML_PYTHON_BIN}" inference/run_ml_inference.py
''',
    )

    validate_recommendation_output = BashOperator(
        task_id="validate_recommendation_output",
        env=COMMON_ENV,
        append_env=True,
        bash_command=r'''
set -e

cd "${ML_SERVICE_DIR}"

export ML_ENV_FILE="${ML_ENV_FILE}"
export MODEL_VERSION="${MODEL_VERSION}"
export ML_SUMMARY_FILE="${ML_SUMMARY_FILE}"
export PYTHONUNBUFFERED=1

"${ML_PYTHON_BIN}" - <<'PY'
import json
import os
import sys
from sqlalchemy import text

sys.path.append(os.getcwd())

from db import get_engine

model_version = os.getenv("MODEL_VERSION", "v1.0")
summary_file = os.getenv("ML_SUMMARY_FILE", "/tmp/echotik_ml_daily_inference_summary.json")

engine = get_engine()

with engine.connect() as conn:
    content_count = conn.execute(
        text(
            """
            SELECT COUNT(*)
            FROM content_recommendations
            WHERE valid_until >= CURDATE()
              AND rationale LIKE '%[ml_daily_inference]%'
            """
        )
    ).scalar()

    hashtag_count = conn.execute(
        text(
            """
            SELECT COUNT(*)
            FROM hashtag_recommendations
            WHERE valid_until >= CURDATE()
              AND model_version = :model_version
            """
        ),
        {"model_version": model_version},
    ).scalar()

    schedule_count = conn.execute(
        text(
            """
            SELECT COUNT(*)
            FROM posting_schedule_recommendations
            WHERE valid_from = CURDATE()
            """
        )
    ).scalar()

content_count = int(content_count or 0)
hashtag_count = int(hashtag_count or 0)
schedule_count = int(schedule_count or 0)
total_output = content_count + hashtag_count + schedule_count

if total_output < 1:
    raise Exception("ML inference completed but no recommendation output found.")

# Tulis ke summary file untuk dibaca notify_finish (dikirim ke Discord)
summary = {}
if os.path.exists(summary_file):
    try:
        with open(summary_file, "r") as f:
            summary = json.load(f) or {}
    except Exception:
        summary = {}

summary["recommendation_output"] = {
    "content_count": content_count,
    "hashtag_count": hashtag_count,
    "schedule_count": schedule_count,
    "total_output": total_output,
}

with open(summary_file, "w") as f:
    json.dump(summary, f)
PY
''',
    )

    notify_finish_task = PythonOperator(
        task_id="notify_finish",
        python_callable=notify_finish,
    )

    (
        notify_start_task
        >> check_feature_store
        >> run_ml_inference_script
        >> validate_recommendation_output
        >> notify_finish_task
    )