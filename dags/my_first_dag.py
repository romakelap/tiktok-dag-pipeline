from datetime import datetime, timedelta
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

# Discord Webhook URL
WEBHOOK_URL = "https://discord.com/api/webhooks/1504847346339549315/pPd5AnmTjQLc5xA-0fMFCOoIIC7I-IgdP7c352Ud-4W7MoX5w0OK8Rsgw7eiR0qLhbHI"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def task_start(**context):
    run_id = context["run_id"]
    print(f"DAG mulai berjalan - run_id: {run_id}")
    return "start_done"

def task_process(**context):
    print("Proses data berjalan...")
    result = {"rows_processed": 42, "status": "success"}
    context["ti"].xcom_push(key="result", value=result)
    return result

def task_notify(**context):
    ti = context["ti"]
    result = ti.xcom_pull(task_ids="process", key="result")
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    ts = context["ts"]

    # Discord menggunakan key "content", bukan "text"
    message = {
        "content": (
            f"✅ **DAG Successs!**\n"
            f"• DAG   : `{dag_id}`\n"
            f"• Run   : `{run_id}`\n"
            f"• Waktu : {ts}\n"
            f"• Hasil : {result}"
        )
    }

    resp = requests.post(WEBHOOK_URL, json=message, timeout=10)
    resp.raise_for_status()
    print(f"Notifikasi terkirim - HTTP {resp.status_code}")

with DAG(
    dag_id="my_first_dag",
    default_args=default_args,
    description="DAG pertama dengan notifikasi Discord webhook",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "webhook"],
) as dag:

    t1 = PythonOperator(task_id="start", python_callable=task_start)
    t2 = PythonOperator(task_id="process", python_callable=task_process)
    t3 = PythonOperator(task_id="notify", python_callable=task_notify)

    t1 >> t2 >> t3