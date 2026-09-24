import requests
import logging
from datetime import datetime
from typing import Dict, Optional
import ssl
from requests.adapters import HTTPAdapter


class TLSAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        context = ssl.create_default_context()
        # Bypass Python 3.10+ OpenSSL 3.0+ UNEXPECTED_EOF_WHILE_READING strictness
        if hasattr(ssl, "OP_IGNORE_UNEXPECTED_EOF"):
            context.options |= ssl.OP_IGNORE_UNEXPECTED_EOF
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        kwargs['ssl_context'] = context
        return super().init_poolmanager(*args, **kwargs)


class DiscordNotifier:

    # Color codes untuk Discord embed
    COLOR_BLUE = 3447003      # Task started
    COLOR_GREEN = 3066993     # Success
    COLOR_ORANGE = 15105570   # Warning / partial success
    COLOR_RED = 15158332      # Failed
    COLOR_PURPLE = 10181046   # Summary
    
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
        self.session = requests.Session()
        # Mount TLSAdapter to handle SSL UNEXPECTED_EOF_WHILE_READING errors
        self.session.mount('https://', TLSAdapter())
    
    def send_task_started(self, task_name: str, dag_id: str, run_id: str = ""):
        """Notifikasi saat task dimulai"""
        embed = {
            "title": "Task Started",
            "description": f"`{task_name}`",
            "fields": [
                {"name": "DAG", "value": dag_id, "inline": True},
                {"name": "Run ID", "value": run_id or "manual", "inline": True},
                {"name": "Time", "value": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"), "inline": False},
            ],
            "color": self.COLOR_BLUE,
        }
        self._send_embed(embed)
    
    def send_task_success(self, task_name: str, duration_sec: float, 
                          records_count: Optional[int] = None,
                          extra_info: Optional[Dict] = None):
        """Notifikasi saat task sukses"""
        fields = [
            {"name": "Duration", "value": f"{duration_sec:.1f}s", "inline": True},
            {"name": "Status", "value": "✅ Success", "inline": True},
        ]
        
        if records_count is not None:
            fields.append({"name": "Records", "value": str(records_count), "inline": True})
        
        if extra_info:
            for key, value in extra_info.items():
                fields.append({"name": key, "value": str(value), "inline": True})
        
        embed = {
            "title": f"✅ Task Success: {task_name}",
            "fields": fields,
            "color": self.COLOR_GREEN,
        }
        self._send_embed(embed)
    
    def send_task_failed(self, task_name: str, error_msg: str, 
                         retry_count: int = 0):
        """Notifikasi saat task gagal — URGENT"""
        embed = {
            "title": "❌ Task Failed",
            "description": f"`{task_name}`",
            "fields": [
                {"name": "Error", "value": error_msg[:1000], "inline": False},
                {"name": "Retry Count", "value": str(retry_count), "inline": True},
                {"name": "Time", "value": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"), "inline": True},
            ],
            "color": self.COLOR_RED,
        }
        self._send_embed(embed)
    
    def send_category_summary(self, api_name: str, category: str,
                              total_records: int, pages_fetched: int,
                              duration_sec: float, status: str = "success"):
        """Summary per kategori setelah fetch selesai"""
        color = self.COLOR_GREEN if status == "success" else self.COLOR_ORANGE
        status_emoji = "✅" if status == "success" else "⚠️"
        
        embed = {
            "title": f"{status_emoji} Category Complete: {category}",
            "fields": [
                {"name": "API", "value": api_name, "inline": True},
                {"name": "Records", "value": str(total_records), "inline": True},
                {"name": "Pages", "value": str(pages_fetched), "inline": True},
                {"name": "Duration", "value": f"{duration_sec:.1f}s", "inline": True},
            ],
            "color": color,
        }
        self._send_embed(embed)
    
    def send_api_summary(self, api_name: str, categories_data: Dict):
        """Summary setelah seluruh API (multi-category) selesai"""
        total_records = sum(c.get("records", 0) for c in categories_data.values())
        total_categories = len(categories_data)
        success_count = sum(1 for c in categories_data.values() 
                           if c.get("status") == "success")
        
        fields = [
            {"name": "Total Records", "value": str(total_records), "inline": True},
            {"name": "Categories", "value": f"{success_count}/{total_categories}", "inline": True},
        ]
        
        # Tambah detail per category
        for slug, info in categories_data.items():
            emoji = "✅" if info.get("status") == "success" else "❌"
            fields.append({
                "name": f"{emoji} {slug}",
                "value": f"{info.get('records', 0)} records",
                "inline": True
            })
        
        embed = {
            "title": f"📊 API Summary: {api_name}",
            "fields": fields,
            "color": self.COLOR_GREEN if success_count == total_categories else self.COLOR_ORANGE,
        }
        self._send_embed(embed)
    
    def send_parser_summary(self, total_records: int, valid_records: int,
                            invalid_records: int, error_breakdown: Dict[str, int],
                            excel_filename: str):
        """Summary setelah parser selesai"""
        error_details = "\n".join([f"  • {k}: {v}" for k, v in error_breakdown.items()]) or "None"
        
        embed = {
            "title": "🔍 Parser & Validation Summary",
            "fields": [
                {"name": "Total Records", "value": str(total_records), "inline": True},
                {"name": "✅ Valid", "value": str(valid_records), "inline": True},
                {"name": "❌ Invalid", "value": str(invalid_records), "inline": True},
                {"name": "Validation Errors", "value": error_details[:1000], "inline": False},
                {"name": "📁 Excel Output", "value": f"`{excel_filename}`", "inline": False},
            ],
            "color": self.COLOR_GREEN if invalid_records == 0 else self.COLOR_ORANGE,
        }
        self._send_embed(embed)
    
    def send_ingest_summary(self, inserted: int, updated: int, failed: int,
                            table_breakdown: Dict[str, Dict[str, int]]):
        """Summary setelah DB ingest selesai"""
        total = inserted + updated
        
        fields = [
            {"name": "Inserted", "value": str(inserted), "inline": True},
            {"name": "Updated", "value": str(updated), "inline": True},
            {"name": "Failed", "value": str(failed), "inline": True},
            {"name": "Total", "value": str(total), "inline": True},
        ]
        
        for table, counts in table_breakdown.items():
            fields.append({
                "name": f"📦 {table}",
                "value": f"INS: {counts.get('inserted', 0)} | UPD: {counts.get('updated', 0)}",
                "inline": False
            })
        
        embed = {
            "title": "💾 Database Ingest Summary",
            "fields": fields,
            "color": self.COLOR_GREEN if failed == 0 else self.COLOR_ORANGE,
        }
        self._send_embed(embed)
    
    def send_dag_summary(self, dag_id: str, run_id: str, status: str,
                         duration_sec: float, total_records: int,
                         next_run: Optional[str] = None):
        """Final DAG summary — dikirim setelah seluruh pipeline selesai"""
        color_map = {
            "success": self.COLOR_GREEN,
            "partial": self.COLOR_ORANGE,
            "failed": self.COLOR_RED,
        }
        emoji_map = {
            "success": "🎯",
            "partial": "⚠️",
            "failed": "❌",
        }
        
        fields = [
            {"name": "Status", "value": f"**{status.upper()}**", "inline": True},
            {"name": "Duration", "value": f"{duration_sec/60:.1f} min", "inline": True},
            {"name": "Records Processed", "value": str(total_records), "inline": True},
            {"name": "Run ID", "value": run_id, "inline": False},
        ]
        
        if next_run:
            fields.append({"name": "⏰ Next Run", "value": next_run, "inline": False})
        
        embed = {
            "title": f"{emoji_map.get(status, '📈')} Pipeline Complete: {dag_id}",
            "fields": fields,
            "color": color_map.get(status, self.COLOR_PURPLE),
            "footer": {"text": "Echotik Data Collection Pipeline"},
            "timestamp": datetime.utcnow().isoformat(),
        }
        self._send_embed(embed)
    
    def send_rate_limit_alert(self, api_endpoint: str, retry_after: int):
        """Alert khusus saat rate limit terdeteksi"""
        embed = {
            "title": "⚠️ Rate Limit Detected",
            "description": "API mengembalikan 429 Too Many Requests",
            "fields": [
                {"name": "Endpoint", "value": api_endpoint, "inline": False},
                {"name": "Retry After", "value": f"{retry_after}s", "inline": True},
                {"name": "Action", "value": "Auto-retry dengan backoff", "inline": True},
            ],
            "color": self.COLOR_ORANGE,
        }
        self._send_embed(embed)
    
    def send_message(self, message: str):
        """Kirim simple text message dengan retry"""
        import time
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = self.session.post(
                    self.webhook_url,
                    json={"content": message},
                    timeout=15
                )
                response.raise_for_status()
                return
            except Exception as e:
                if attempt == max_retries - 1:
                    logging.error(f'Failed to send Discord message after {max_retries} attempts: {e}')
                else:
                    logging.warning(f'Discord message send failed (attempt {attempt + 1}/{max_retries}): {e}. Retrying...')
                    time.sleep(2)
    
    def _send_embed(self, embed: Dict):
        """Internal — kirim Discord embed dengan retry"""
        import time
        payload = {"embeds": [embed]}
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = self.session.post(
                    self.webhook_url,
                    json=payload,
                    timeout=15
                )
                response.raise_for_status()
                return
            except Exception as e:
                if attempt == max_retries - 1:
                    logging.error(f'Failed to send Discord notification after {max_retries} attempts: {e}')
                else:
                    logging.warning(f'Discord notification send failed (attempt {attempt + 1}/{max_retries}): {e}. Retrying...')
                    time.sleep(2)


def get_notifier():
    """Helper untuk get Discord notifier dari Airflow Variable"""
    from airflow.models import Variable
    webhook_url = Variable.get('DISCORD_WEBHOOK_URL', default_var=None)
    if not webhook_url:
        logging.warning("DISCORD_WEBHOOK_URL not set in Airflow Variables")
        return None
    return DiscordNotifier(webhook_url)
