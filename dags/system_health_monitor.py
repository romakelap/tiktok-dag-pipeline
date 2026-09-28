"""
DAG: system_health_monitor
Deskripsi: Scheduled & On-Demand System Health, Resource Monitoring, and Auto-Pruning.
Memantau Disk Space, Memory, Database Connectivity, dan Services Health.
Mengirim alert otomatis ke Google Chat / Discord jika terjadi server down atau disk hampir penuh.
"""

import os
import sys
import shutil
import logging
import socket
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, '/opt/airflow/plugins')
sys.path.insert(0, '/opt/airflow')

# Import notifier
try:
    from utils.monitoring.discord_notifier import get_notifier
except Exception as e:
    logging.warning(f"Failed to import notifier: {e}")
    get_notifier = lambda: None

# Import DB Hook
try:
    from hooks.db_hook import get_db_hook
except Exception as e:
    logging.warning(f"Failed to import get_db_hook: {e}")
    get_db_hook = lambda: None


default_args = {
    'owner': 'nico',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
}


def get_memory_info():
    """Membaca pemakaian RAM & Swap dari /proc/meminfo secara native tanpa library external"""
    mem_info = {}
    try:
        with open('/proc/meminfo', 'r') as f:
            for line in f:
                parts = line.split(':')
                if len(parts) == 2:
                    key = parts[0].strip()
                    val = parts[1].strip().split()[0]
                    mem_info[key] = int(val)
        
        total_kb = mem_info.get('MemTotal', 1)
        avail_kb = mem_info.get('MemAvailable', mem_info.get('MemFree', 0))
        used_kb = total_kb - avail_kb
        used_pct = round((used_kb / total_kb) * 100, 1)
        
        swap_total = mem_info.get('SwapTotal', 0)
        swap_free = mem_info.get('SwapFree', 0)
        swap_used = swap_total - swap_free
        swap_pct = round((swap_used / swap_total) * 100, 1) if swap_total > 0 else 0
        
        return {
            'total_mb': round(total_kb / 1024),
            'used_mb': round(used_kb / 1024),
            'avail_mb': round(avail_kb / 1024),
            'used_pct': used_pct,
            'swap_total_mb': round(swap_total / 1024),
            'swap_used_mb': round(swap_used / 1024),
            'swap_pct': swap_pct
        }
    except Exception as e:
        logging.warning(f"Could not read /proc/meminfo: {e}")
        return None


def auto_prune_logs_and_cache():
    """Auto cleanup logs & cache untuk mencegah disk 100% full"""
    freed_mb = 0
    airflow_home = os.environ.get('AIRFLOW_HOME', '/home/ubuntu/airflow')
    logs_dir = Path(airflow_home) / 'logs'
    
    # 1. Hapus task logs lebih dari 5 hari
    if logs_dir.exists():
        now_ts = datetime.utcnow().timestamp()
        for p in list(logs_dir.rglob('*')):
            if p.is_file():
                try:
                    file_age_days = (now_ts - p.stat().st_mtime) / 86400
                    if file_age_days > 5:
                        freed_mb += p.stat().st_size / (1024 * 1024)
                        p.unlink(missing_ok=True)
                except Exception:
                    pass
    
    # 2. Truncate standalone.log jika > 25MB
    standalone_log = Path(airflow_home) / 'standalone.log'
    if standalone_log.exists():
        try:
            size_mb = standalone_log.stat().st_size / (1024 * 1024)
            if size_mb > 25:
                freed_mb += size_mb
                with open(standalone_log, 'w') as f:
                    f.truncate(0)
        except Exception:
            pass

    return round(freed_mb, 1)


def check_server_resources_task(**context):
    """Memeriksa kapasitas Disk & Memory. Mengirim alert jika disk atau RAM mendekati batas."""
    notifier = get_notifier()
    
    # 1. Disk Space
    disk = shutil.disk_usage('/')
    total_gb = round(disk.total / (1024**3), 2)
    used_gb = round(disk.used / (1024**3), 2)
    free_mb = round(disk.free / (1024**2), 1)
    used_pct = round((disk.used / disk.total) * 100, 1)
    
    logging.info(f"Disk Usage: {used_pct}% ({used_gb}GB / {total_gb}GB), Free: {free_mb}MB")
    
    # Auto-prune jika disk > 80%
    freed_mb = 0
    if used_pct >= 80:
        logging.warning(f"Disk usage is high ({used_pct}%). Running auto-prune...")
        freed_mb = auto_prune_logs_and_cache()
        # Re-evaluate
        disk = shutil.disk_usage('/')
        used_pct = round((disk.used / disk.total) * 100, 1)
        free_mb = round(disk.free / (1024**2), 1)
        logging.info(f"After auto-prune: {used_pct}%, Free: {free_mb}MB (Freed: {freed_mb}MB)")

    # 2. RAM & Swap
    mem = get_memory_info()
    
    # Alert conditions
    alerts = []
    severity = "info"
    
    if used_pct >= 90:
        severity = "critical"
        alerts.append(f"CRITICAL: Disk usage is {used_pct}% (Hanya tersisa {free_mb}MB)")
    elif used_pct >= 80:
        severity = "warning"
        alerts.append(f"WARNING: Disk usage is {used_pct}% ({free_mb}MB free)")

    if mem and mem.get('used_pct', 0) >= 92:
        if severity != "critical":
            severity = "warning"
        alerts.append(f"HIGH RAM USAGE: {mem['used_pct']}% ({mem['used_mb']}MB / {mem['total_mb']}MB)")

    details = {
        "Disk Status": f"{used_pct}% Used ({used_gb}GB / {total_gb}GB) — Free: {free_mb}MB",
        "Memory Status": f"{mem['used_pct']}% Used ({mem['used_mb']}MB / {mem['total_mb']}MB)" if mem else "N/A",
        "Swap Status": f"{mem['swap_pct']}% Used ({mem['swap_used_mb']}MB / {mem['swap_total_mb']}MB)" if mem else "N/A",
    }
    if freed_mb > 0:
        details["Auto-Pruned Space"] = f"{freed_mb} MB cleaned automatically"

    if alerts and notifier:
        notifier.send_system_health_alert(
            title="Server Resource Alert",
            details=details,
            severity=severity
        )
    
    return {
        "disk": {"used_pct": used_pct, "free_mb": free_mb, "total_gb": total_gb},
        "memory": mem,
        "alerts": alerts
    }


def check_services_health_task(**context):
    """Memeriksa apakah DB Aiven MySQL, Backend Service, dan ML Service aktif."""
    notifier = get_notifier()
    service_status = {}
    failed_services = []

    # 1. Check Aiven MySQL Database
    try:
        db = get_db_hook()
        if db and db.test_connection():
            service_status['Aiven MySQL DB'] = 'Healthy (Connected)'
            db.close()
        else:
            service_status['Aiven MySQL DB'] = 'Failed (Connection test returned False)'
            failed_services.append('Aiven MySQL DB')
    except Exception as e:
        service_status['Aiven MySQL DB'] = f'Error: {str(e)[:150]}'
        failed_services.append('Aiven MySQL DB')

    # 2. Check Local Backend (Port 8082)
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(3)
        result = sock.connect_ex(('127.0.0.1', 8082))
        sock.close()
        if result == 0:
            service_status['Backend Java Service (8082)'] = 'Healthy (Port Open)'
        else:
            service_status['Backend Java Service (8082)'] = 'Inactive / Port Closed'
    except Exception as e:
        service_status['Backend Java Service (8082)'] = f'Check Failed: {str(e)[:100]}'

    # 3. Check ML Service (Port 8000)
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(3)
        result = sock.connect_ex(('127.0.0.1', 8000))
        sock.close()
        if result == 0:
            service_status['ML Service (8000)'] = 'Healthy (Port Open)'
        else:
            service_status['ML Service (8000)'] = 'Inactive / Port Closed'
    except Exception as e:
        service_status['ML Service (8000)'] = f'Check Failed: {str(e)[:100]}'

    # If critical services like Database failed, send urgent alert
    if failed_services and notifier:
        notifier.send_system_health_alert(
            title="Service Down Alert",
            details=service_status,
            severity="critical"
        )

    logging.info(f"Services Health Status: {service_status}")
    return service_status


def cleanup_maintenance_task(**context):
    """Routine pruning & disk maintenance task"""
    freed_mb = auto_prune_logs_and_cache()
    logging.info(f"Maintenance routine completed. Cleaned {freed_mb}MB.")
    return {"freed_mb": freed_mb}


with DAG(
    dag_id='system_health_monitor',
    default_args=default_args,
    description='Server health monitoring, resource alerts, and log auto-pruning',
    schedule='0 */6 * * *',  # Every 6 hours (00:00, 06:00, 12:00, 18:00 UTC)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['monitoring', 'health', 'maintenance', 'alerts'],
) as dag:

    t1_check_resources = PythonOperator(
        task_id='check_server_resources',
        python_callable=check_server_resources_task,
    )

    t2_check_services = PythonOperator(
        task_id='check_services_health',
        python_callable=check_services_health_task,
    )

    t3_cleanup = PythonOperator(
        task_id='routine_log_cleanup',
        python_callable=cleanup_maintenance_task,
    )

    [t1_check_resources, t2_check_services] >> t3_cleanup
