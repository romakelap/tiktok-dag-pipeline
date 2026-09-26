import requests
import logging
import ssl
from datetime import datetime
from typing import Dict, Optional
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


DEFAULT_WEBHOOK_URL = "https://chat.googleapis.com/v1/spaces/AAQAb4EY2xU/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=BQzvW6PqwnJmeJelVkOtr2nNuOW5mtkCJjZyM2xM0QE"


class AlertNotifier:
    """
    Unified Alert Notifier supporting both Google Chat Webhooks and Discord Webhooks.
    Clean text formatting without emojis.
    """

    # Color codes untuk Discord embed
    COLOR_BLUE = 3447003      # Task started
    COLOR_GREEN = 3066993     # Success
    COLOR_ORANGE = 15105570   # Warning / partial success
    COLOR_RED = 15158332      # Failed
    COLOR_PURPLE = 10181046   # Summary
    
    def __init__(self, webhook_url: Optional[str] = None):
        self.webhook_url = webhook_url or DEFAULT_WEBHOOK_URL
        self.is_gchat = "chat.googleapis.com" in self.webhook_url
        self.session = requests.Session()
        # Mount TLSAdapter to handle SSL UNEXPECTED_EOF_WHILE_READING errors
        self.session.mount('https://', TLSAdapter())
    
    def send_task_started(self, task_name: str, dag_id: str, run_id: str = ""):
        """Notifikasi saat task dimulai"""
        if self.is_gchat:
            text = (
                f"*Task Started*\n"
                f"• *Task:* `{task_name}`\n"
                f"• *DAG:* `{dag_id}`\n"
                f"• *Run ID:* `{run_id or 'manual'}`\n"
                f"• *Time:* {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"
            )
            self.send_message(text)
        else:
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
        if self.is_gchat:
            lines = [
                f"*Task Success:* `{task_name}`",
                f"• *Duration:* {duration_sec:.1f}s",
                f"• *Status:* Success",
            ]
            if records_count is not None:
                lines.append(f"• *Records:* {records_count}")
            if extra_info:
                for k, v in extra_info.items():
                    lines.append(f"• *{k}:* {v}")
            self.send_message("\n".join(lines))
        else:
            fields = [
                {"name": "Duration", "value": f"{duration_sec:.1f}s", "inline": True},
                {"name": "Status", "value": "Success", "inline": True},
            ]
            if records_count is not None:
                fields.append({"name": "Records", "value": str(records_count), "inline": True})
            if extra_info:
                for key, value in extra_info.items():
                    fields.append({"name": key, "value": str(value), "inline": True})
            embed = {
                "title": f"Task Success: {task_name}",
                "fields": fields,
                "color": self.COLOR_GREEN,
            }
            self._send_embed(embed)
    
    def send_task_failed(self, task_name: str, error_msg: str, 
                         retry_count: int = 0):
        """Notifikasi saat task gagal"""
        if self.is_gchat:
            text = (
                f"*Task Failed:* `{task_name}`\n"
                f"• *Retry Count:* {retry_count}\n"
                f"• *Time:* {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
                f"• *Error:*\n```{error_msg[:1000]}```"
            )
            self.send_message(text)
        else:
            embed = {
                "title": "Task Failed",
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
        if self.is_gchat:
            text = (
                f"*Category Complete:* `{category}`\n"
                f"• *API:* {api_name}\n"
                f"• *Records:* {total_records}\n"
                f"• *Pages:* {pages_fetched}\n"
                f"• *Duration:* {duration_sec:.1f}s\n"
                f"• *Status:* {status.capitalize()}"
            )
            self.send_message(text)
        else:
            color = self.COLOR_GREEN if status == "success" else self.COLOR_ORANGE
            embed = {
                "title": f"Category Complete: {category}",
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
        success_count = sum(1 for c in categories_data.values() if c.get("status") == "success")
        
        if self.is_gchat:
            lines = [
                f"*API Summary:* `{api_name}`",
                f"• *Total Records:* {total_records}",
                f"• *Categories Processed:* {success_count}/{total_categories}",
                "",
                "*Category Breakdown:*",
            ]
            for slug, info in categories_data.items():
                status_text = "OK" if info.get("status") == "success" else "Failed"
                lines.append(f"• *{slug}* [{status_text}]: {info.get('records', 0)} records")
            self.send_message("\n".join(lines))
        else:
            fields = [
                {"name": "Total Records", "value": str(total_records), "inline": True},
                {"name": "Categories", "value": f"{success_count}/{total_categories}", "inline": True},
            ]
            for slug, info in categories_data.items():
                fields.append({
                    "name": f"{slug}",
                    "value": f"{info.get('records', 0)} records",
                    "inline": True
                })
            embed = {
                "title": f"API Summary: {api_name}",
                "fields": fields,
                "color": self.COLOR_GREEN if success_count == total_categories else self.COLOR_ORANGE,
            }
            self._send_embed(embed)
    
    def send_parser_summary(self, total_records: int, valid_records: int,
                            invalid_records: int, error_breakdown: Dict[str, int],
                            excel_filename: str):
        """Summary setelah parser selesai"""
        error_details = "\n".join([f"  • {k}: {v}" for k, v in error_breakdown.items()]) or "None"
        if self.is_gchat:
            text = (
                f"*Parser & Validation Summary*\n"
                f"• *Total Records:* {total_records}\n"
                f"• *Valid:* {valid_records}\n"
                f"• *Invalid:* {invalid_records}\n"
                f"• *Excel Output:* `{excel_filename}`\n"
                f"• *Validation Errors:*\n{error_details}"
            )
            self.send_message(text)
        else:
            embed = {
                "title": "Parser & Validation Summary",
                "fields": [
                    {"name": "Total Records", "value": str(total_records), "inline": True},
                    {"name": "Valid", "value": str(valid_records), "inline": True},
                    {"name": "Invalid", "value": str(invalid_records), "inline": True},
                    {"name": "Validation Errors", "value": error_details[:1000], "inline": False},
                    {"name": "Excel Output", "value": f"`{excel_filename}`", "inline": False},
                ],
                "color": self.COLOR_GREEN if invalid_records == 0 else self.COLOR_ORANGE,
            }
            self._send_embed(embed)
    
    def send_ingest_summary(self, inserted: int, updated: int, failed: int,
                            table_breakdown: Dict[str, Dict[str, int]]):
        """Summary setelah DB ingest selesai"""
        total = inserted + updated
        if self.is_gchat:
            lines = [
                f"*Database Ingest Summary*",
                f"• *Inserted:* {inserted}",
                f"• *Updated:* {updated}",
                f"• *Failed:* {failed}",
                f"• *Total:* {total}",
                "",
                "*Table Breakdown:*",
            ]
            for table, counts in table_breakdown.items():
                lines.append(f"• *{table}*: INS: {counts.get('inserted', 0)} | UPD: {counts.get('updated', 0)}")
            self.send_message("\n".join(lines))
        else:
            fields = [
                {"name": "Inserted", "value": str(inserted), "inline": True},
                {"name": "Updated", "value": str(updated), "inline": True},
                {"name": "Failed", "value": str(failed), "inline": True},
                {"name": "Total", "value": str(total), "inline": True},
            ]
            for table, counts in table_breakdown.items():
                fields.append({
                    "name": f"{table}",
                    "value": f"INS: {counts.get('inserted', 0)} | UPD: {counts.get('updated', 0)}",
                    "inline": False
                })
            embed = {
                "title": "Database Ingest Summary",
                "fields": fields,
                "color": self.COLOR_GREEN if failed == 0 else self.COLOR_ORANGE,
            }
            self._send_embed(embed)
    
    def send_dag_summary(self, dag_id: str, run_id: str, status: str,
                         duration_sec: float, total_records: int,
                         next_run: Optional[str] = None):
        """Final DAG summary — dikirim setelah seluruh pipeline selesai"""
        if self.is_gchat:
            lines = [
                f"*Pipeline Complete:* `{dag_id}`",
                f"• *Status:* *{status.upper()}*",
                f"• *Duration:* {duration_sec/60:.1f} min",
                f"• *Records Processed:* {total_records}",
                f"• *Run ID:* `{run_id}`",
            ]
            if next_run:
                lines.append(f"• *Next Run:* {next_run}")
            lines.append(f"• *Time:* {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
            self.send_message("\n".join(lines))
        else:
            color_map = {
                "success": self.COLOR_GREEN,
                "partial": self.COLOR_ORANGE,
                "failed": self.COLOR_RED,
            }
            fields = [
                {"name": "Status", "value": f"**{status.upper()}**", "inline": True},
                {"name": "Duration", "value": f"{duration_sec/60:.1f} min", "inline": True},
                {"name": "Records Processed", "value": str(total_records), "inline": True},
                {"name": "Run ID", "value": run_id, "inline": False},
            ]
            if next_run:
                fields.append({"name": "Next Run", "value": next_run, "inline": False})
            embed = {
                "title": f"Pipeline Complete: {dag_id}",
                "fields": fields,
                "color": color_map.get(status, self.COLOR_PURPLE),
                "footer": {"text": "Echotik Data Collection Pipeline"},
                "timestamp": datetime.utcnow().isoformat(),
            }
            self._send_embed(embed)
    
    def send_rate_limit_alert(self, api_endpoint: str, retry_after: int):
        """Alert khusus saat rate limit terdeteksi"""
        if self.is_gchat:
            text = (
                f"*Rate Limit Detected*\n"
                f"API mengembalikan 429 Too Many Requests\n"
                f"• *Endpoint:* `{api_endpoint}`\n"
                f"• *Retry After:* {retry_after}s\n"
                f"• *Action:* Auto-retry dengan backoff"
            )
            self.send_message(text)
        else:
            embed = {
                "title": "Rate Limit Detected",
                "description": "API mengembalikan 429 Too Many Requests",
                "fields": [
                    {"name": "Endpoint", "value": api_endpoint, "inline": False},
                    {"name": "Retry After", "value": f"{retry_after}s", "inline": True},
                    {"name": "Action", "value": "Auto-retry dengan backoff", "inline": True},
                ],
                "color": self.COLOR_ORANGE,
            }
            self._send_embed(embed)
    
    def send_login_started(self, email: str = ""):
        """Notifikasi saat proses auto-login ke Echotik dimulai"""
        if self.is_gchat:
            text = (
                f"*Echotik Auto-Login Started*\n"
                f"• *Status:* Requesting fresh Bearer access token...\n"
                f"• *Time:* {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"
            )
            self.send_message(text)
        else:
            embed = {
                "title": "Echotik Auto-Login Started",
                "description": "Requesting fresh Bearer access token...",
                "color": self.COLOR_BLUE,
            }
            self._send_embed(embed)
    
    def send_login_success(self, email: str = "", token: str = "", user_name: str = ""):
        """Notifikasi saat login dan update Bearer token berhasil"""
        masked_token = token[:10] + "..." + token[-6:] if len(token) > 16 else (token[:6] + "..." if token else "")
        if self.is_gchat:
            text = (
                f"*Echotik Bearer Token Updated Successfully*\n"
                f"• *Token:* `{masked_token}`\n"
                f"• *Airflow Variable:* `ECHOTIK_BEARER_TOKEN` updated\n"
                f"• *Time:* {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"
            )
            self.send_message(text)
        else:
            embed = {
                "title": "Echotik Token Refresh Success",
                "fields": [
                    {"name": "Token", "value": f"`{masked_token}`", "inline": True},
                    {"name": "Status", "value": "Airflow Variable Updated", "inline": False},
                ],
                "color": self.COLOR_GREEN,
            }
            self._send_embed(embed)
    
    def send_token_valid(self, token: str = ""):
        """Notifikasi saat Bearer token dicek dan masih aktif/valid"""
        masked_token = token[:10] + "..." + token[-6:] if len(token) > 16 else (token[:6] + "..." if token else "")
        if self.is_gchat:
            text = (
                f"*Echotik Bearer Token Status*\n"
                f"• *Status:* Token aktif & valid (siap digunakan)\n"
                f"• *Token:* `{masked_token}`\n"
                f"• *Time:* {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"
            )
            self.send_message(text)
        else:
            embed = {
                "title": "Echotik Token Active",
                "description": "Bearer token aktif & valid",
                "fields": [
                    {"name": "Token", "value": f"`{masked_token}`", "inline": True},
                    {"name": "Status", "value": "Ready to crawl", "inline": False},
                ],
                "color": self.COLOR_GREEN,
            }
            self._send_embed(embed)
    
    def send_login_failed(self, email: str = "", error_msg: str = ""):
        """Notifikasi saat login gagal"""
        if self.is_gchat:
            text = (
                f"*Echotik Auto-Login Failed*\n"
                f"• *Error:* ```{error_msg[:1000]}```\n"
                f"• *Time:* {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"
            )
            self.send_message(text)
        else:
            embed = {
                "title": "Echotik Login Failed",
                "description": "Failed to auto-login to Echotik",
                "fields": [
                    {"name": "Error", "value": error_msg[:1000], "inline": False},
                ],
                "color": self.COLOR_RED,
            }
            self._send_embed(embed)
    
    def send_message(self, message: str):
        """Kirim simple text message dengan retry"""
        import time
        max_retries = 3
        payload = {"text": message} if self.is_gchat else {"content": message}
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
                    logging.error(f'Failed to send webhook message after {max_retries} attempts: {e}')
                else:
                    logging.warning(f'Webhook message send failed (attempt {attempt + 1}/{max_retries}): {e}. Retrying...')
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


# Backwards compatibility aliases
DiscordNotifier = AlertNotifier
GChatNotifier = AlertNotifier


def get_notifier():
    """Helper untuk get notifier dari Airflow Variable, Environment, atau fallback default"""
    import os
    webhook_url = None
    try:
        from airflow.models import Variable
        webhook_url = Variable.get('GCHAT_WEBHOOK_URL', default_var=None) or Variable.get('DISCORD_WEBHOOK_URL', default_var=None)
    except Exception:
        pass

    if not webhook_url:
        webhook_url = os.environ.get('GCHAT_WEBHOOK_URL') or os.environ.get('DISCORD_WEBHOOK_URL')
    
    if not webhook_url:
        webhook_url = DEFAULT_WEBHOOK_URL

    return AlertNotifier(webhook_url)
