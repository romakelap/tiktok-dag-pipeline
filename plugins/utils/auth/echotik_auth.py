"""
============================================================
ECHOTIK AUTHENTICATION & AUTO-REFRESH MODULE
============================================================
Module ini menangani:
1. Login otomatis ke Echotik API menggunakan Email & Password.
2. Mendapatkan Bearer access token terbaru.
3. Otomatis mengupdate Airflow Variable `ECHOTIK_BEARER_TOKEN`.
4. Mengirim notifikasi Google Chat / Discord saat login dimulai dan saat token berhasil diupdate.
5. Validasi keaktifan token (health-check) & auto-refresh jika expired (401).
"""

import os
import random
import logging
import requests
from typing import Optional, Dict


USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
]


class EchotikAuthException(Exception):
    """Exception raised when authentication fails."""
    pass


class EchotikAuthenticator:
    """
    Service untuk login otomatis ke Echotik dan mengelola Bearer token.
    """

    LOGIN_URL = "https://echotik.live/api/v1/users/login"
    VALIDATE_URL = "https://echotik.live/api/v1/data/videos"

    def __init__(self, email: Optional[str] = None, password: Optional[str] = None, notifier=None):
        self.email = email
        self.password = password
        self.session = requests.Session()
        self.notifier = notifier
        if not self.notifier:
            try:
                from utils.monitoring.discord_notifier import get_notifier
                self.notifier = get_notifier()
            except Exception:
                self.notifier = None

    def set_notifier(self, notifier):
        self.notifier = notifier

    def _get_credentials(self):
        """Ambil email & password dari argumen, Airflow Variable, atau env var"""
        email = self.email
        password = self.password

        # Coba ambil dari Airflow Variable jika belum ada
        if not email or not password:
            try:
                from airflow.models import Variable
                email = email or Variable.get('ECHOTIK_EMAIL', default_var=None)
                password = password or Variable.get('ECHOTIK_PASSWORD', default_var=None)
            except Exception:
                pass

        # Coba ambil dari Environment Variable
        if not email:
            email = os.environ.get('ECHOTIK_EMAIL')
        if not password:
            password = os.environ.get('ECHOTIK_PASSWORD')

        if not email or not password:
            raise EchotikAuthException(
                "Kredensial login Echotik belum di-set! "
                "Harap set Airflow Variable 'ECHOTIK_EMAIL' dan 'ECHOTIK_PASSWORD'."
            )

        return email, password

    def login(self, update_airflow_var: bool = True) -> str:
        """
        Lakukan login ke Echotik API dan kembalikan access_token.
        Jika update_airflow_var=True, otomatis update Airflow Variable `ECHOTIK_BEARER_TOKEN`.
        """
        email, password = self._get_credentials()

        # Kirim notifikasi login dimulai
        if self.notifier:
            try:
                self.notifier.send_login_started(email)
            except Exception as notif_err:
                logging.warning(f"Gagal mengirim notifikasi login started: {notif_err}")

        ua = random.choice(USER_AGENTS)

        headers = {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "Referer": "https://echotik.live/login",
            "User-Agent": ua,
            "x-region": "ID",
            "x-currency": "IDR",
            "x-lang": "en-US",
        }

        payload = {
            "email": email,
            "password": password
        }

        logging.info(f"Mengirim request login ke Echotik untuk user: {email}...")

        try:
            response = self.session.post(
                self.LOGIN_URL,
                json=payload,
                headers=headers,
                timeout=15
            )

            if response.status_code != 200:
                raise EchotikAuthException(
                    f"Login Echotik gagal (HTTP {response.status_code}): {response.text[:200]}"
                )

            try:
                data = response.json()
            except Exception:
                raise EchotikAuthException(f"Response login bukan JSON valid: {response.text[:200]}")

            if data.get("code") != 0 or not data.get("data") or not data.get("data").get("access_token"):
                err_msg = data.get("msg") or data.get("errors") or "Akses token tidak ditemukan di response"
                raise EchotikAuthException(f"Autentikasi Echotik ditolak: {err_msg}")

            token = data["data"]["access_token"]
            user_info = data["data"].get("user", {})
            user_name = user_info.get("name", email)
            logging.info(f"✅ Login Echotik berhasil! User: {user_name} (Token: {token[:10]}...)")

            # Update Airflow Variable jika running di lingkungan Airflow
            if update_airflow_var:
                try:
                    from airflow.models import Variable
                    Variable.set("ECHOTIK_BEARER_TOKEN", token)
                    logging.info("✅ Airflow Variable 'ECHOTIK_BEARER_TOKEN' berhasil diupdate otomatis.")
                except Exception as e:
                    logging.warning(f"Tidak dapat mengupdate Airflow Variable (mungkin di luar Airflow runner): {e}")

            # Kirim notifikasi login & token berhasil diupdate
            if self.notifier:
                try:
                    self.notifier.send_login_success(email=email, token=token, user_name=user_name)
                except Exception as notif_err:
                    logging.warning(f"Gagal mengirim notifikasi login success: {notif_err}")

            return token

        except Exception as e:
            if self.notifier:
                try:
                    self.notifier.send_login_failed(email=email, error_msg=str(e))
                except Exception:
                    pass
            raise

    def is_token_valid(self, token: str) -> bool:
        """
        Cek apakah token saat ini masih aktif dengan request test ringan.
        """
        if not token or token == "PASTE_YOUR_TOKEN_HERE":
            return False

        headers = {
            "Accept": "application/json, text/plain, */*",
            "Authorization": f"Bearer {token}",
            "User-Agent": random.choice(USER_AGENTS),
            "x-region": "ID",
            "x-currency": "IDR",
            "x-lang": "en-US",
        }

        params = {"page": 1, "page_size": 1, "region": "ID", "currency": "IDR"}

        try:
            res = self.session.get(
                self.VALIDATE_URL,
                headers=headers,
                params=params,
                timeout=10
            )
            if res.status_code == 200:
                res_data = res.json()
                if res_data.get("code") == 0:
                    return True
            return False
        except Exception:
            return False

    def get_valid_token(self, force_refresh: bool = False) -> str:
        """
        Ambil token yang valid.
        Cek token yang ada di Airflow Variable terlebih dahulu:
        - Jika valid & tidak force_refresh -> gunakan token tersebut.
        - Jika expired/tidak ada -> login ulang dan return token baru.
        """
        current_token = None
        if not force_refresh:
            try:
                from airflow.models import Variable
                current_token = Variable.get("ECHOTIK_BEARER_TOKEN", default_var=None)
            except Exception:
                current_token = os.environ.get("ECHOTIK_BEARER_TOKEN")

            if current_token and self.is_token_valid(current_token):
                logging.info("Token saat ini masih valid. Tidak perlu login ulang.")
                return current_token

        logging.info("Token tidak valid / expired / belum ada. Memulai proses login ulang otomatis...")
        return self.login(update_airflow_var=True)


def get_authenticated_token(force_refresh: bool = False, notifier=None) -> str:
    """Helper global untuk mendapatkan token yang valid"""
    auth = EchotikAuthenticator(notifier=notifier)
    return auth.get_valid_token(force_refresh=force_refresh)
