"""
Echotik API Client v3.

UPDATE: Berdasarkan cURL real dari browser Nico:
- Auth: Bearer token (di header Authorization)
- Regional headers: x-region, x-currency, x-lang, x-secondary-currency
- Pagination strict: page 1-7, per_page=20
- Full browser headers untuk avoid bot detection
"""
import requests
import time
import random
import logging
from typing import Dict, Optional, List
from datetime import datetime


class EchotikAuthException(Exception):
    """Exception raised when Echotik API token is expired or invalid."""
    pass


class EchotikAPIClient:
    """
    Client untuk Echotik API dengan Bearer token + regional headers.
    """
    
    USER_AGENTS = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
    ]
    
    BASE_URL = "https://echotik.live"
    
    def __init__(self, bearer_token: str, regional_headers: Dict = None,
                 cookies: str = "", timeout: int = 30, max_retries: int = 3):
        """
        Args:
            bearer_token: Bearer token dari browser (format: "XXXXXXX|YYYYYYYYY...")
                          Dapat dari: F12 → Network → request → Headers → Authorization
            regional_headers: Dict berisi x-region, x-currency, dll
            cookies: Optional, cookie string lengkap kalau Bearer tidak cukup
            timeout: Timeout per request
            max_retries: Maximum retry attempt
        """
        self.bearer_token = bearer_token
        self.regional_headers = regional_headers or {
            "x-region": "ID",
            "x-currency": "IDR",
            "x-lang": "en-US",
            "x-secondary-currency": "CNY",
        }
        self.cookies = cookies
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = requests.Session()
        self.notifier = None
    
    def set_notifier(self, notifier):
        self.notifier = notifier
    
    def _try_auto_refresh(self) -> bool:
        """Coba auto-login dan update bearer_token jika expired"""
        try:
            from utils.auth.echotik_auth import EchotikAuthenticator
            auth = EchotikAuthenticator()
            new_token = auth.login(update_airflow_var=True)
            if new_token:
                self.bearer_token = new_token
                logging.info(f"Bearer token refreshed successfully: {new_token[:10]}...")
                return True
        except Exception as e:
            logging.warning(f"Auto-refresh token failed: {e}")
        return False
    
    def _build_headers(self, referer_path: str = "/") -> Dict[str, str]:
        """
        Build headers persis seperti cURL dari browser Nico.
        """
        ua = random.choice(self.USER_AGENTS)
        
        headers = {
            # Standard headers
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
            "Content-Type": "application/json",
            "Referer": f"{self.BASE_URL}{referer_path}",
            "User-Agent": ua,
            
            # Auth (PENTING!)
            "Authorization": f"Bearer {self.bearer_token}",
            
            # Regional headers (PENTING untuk dapat GMV data lengkap!)
            **self.regional_headers,
            
            # Sec-Ch-Ua (modern browser fingerprint)
            "Sec-Ch-Ua": '"Chromium";v="148", "Google Chrome";v="148", "Not/A)Brand";v="99"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"macOS"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        }
        
        # Cookies (optional fallback)
        if self.cookies:
            headers["Cookie"] = self.cookies
        
        return headers
    
    def fetch_with_retry(self, url: str, page_num: int = 1,
                         referer_path: str = "/") -> Optional[Dict]:
        """Fetch URL dengan retry logic + rate limit handling."""
        for attempt in range(1, self.max_retries + 1):
            try:
                headers = self._build_headers(referer_path=referer_path)
                logging.info(f"Fetching page {page_num}, attempt {attempt}: {url[:120]}...")
                
                response = self.session.get(
                    url,
                    headers=headers,
                    timeout=self.timeout
                )
                
                # Rate limit
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logging.warning(f"Rate limit hit (429). Waiting {retry_after}s...")
                    if self.notifier:
                        self.notifier.send_rate_limit_alert(url[:100], retry_after)
                    time.sleep(retry_after)
                    continue
                
                # Auth error (HTTP Status)
                if response.status_code in (401, 403):
                    logging.warning(f"Auth error {response.status_code}: Bearer token expired. Attempting auto-login...")
                    if self._try_auto_refresh():
                        logging.info("Auto-login succeeded! Retrying request with new token...")
                        continue
                    logging.error(f"Auth error {response.status_code}: Auto-login failed.")
                    raise EchotikAuthException(
                        f"Authentication failed ({response.status_code}). "
                        f"Bearer token expired dan auto-login gagal."
                    )
                
                response.raise_for_status()
                res_data = response.json()
                
                # Auth error (Embedded in JSON body)
                if isinstance(res_data, dict):
                    code = res_data.get('code')
                    msg = str(res_data.get('msg', res_data.get('message', ''))).lower()
                    if code in (401, 403) or 'unauthorized' in msg or 'expired' in msg or ('token' in msg and ('invalid' in msg or 'expire' in msg)):
                        logging.warning(f"Auth error in API response body (code={code}): {msg}. Attempting auto-login...")
                        if self._try_auto_refresh():
                            logging.info("Auto-login succeeded! Retrying request with new token...")
                            continue
                        logging.error(f"Auth error in API response body (code={code}): {msg}")
                        raise EchotikAuthException(
                            f"Authentication failed in response body (code={code}). Msg: {msg}"
                        )
                        
                return res_data
            
            except requests.exceptions.Timeout:
                logging.warning(f"Timeout on attempt {attempt}")
                if attempt < self.max_retries:
                    time.sleep(10 * attempt)
            
            except requests.exceptions.RequestException as e:
                logging.error(f"Request error on attempt {attempt}: {e}")
                if attempt < self.max_retries:
                    time.sleep(10 * attempt)
            
            except Exception as e:
                logging.error(f"Unexpected error: {e}")
                raise
        
        logging.error(f"All {self.max_retries} retries failed for: {url[:100]}")
        return None
    
    def _paginate_fetch(self, url_builder, start_page: int, end_page: int,
                        per_page: int, referer_path: str,
                        delay_min: int, delay_max: int) -> List[Dict]:
        """
        Generic paginator — fetch page start sampai end.
        
        Args:
            url_builder: function(page_num) → URL string
            start_page: page awal (e.g. 1)
            end_page: page akhir (e.g. 7)
            per_page: rows per page (untuk detect last page)
            referer_path: Referer path
            delay_min, delay_max: delay range antar page
        """
        all_records = []
        
        for page in range(start_page, end_page + 1):
            url = url_builder(page)
            
            response = self.fetch_with_retry(
                url, 
                page_num=page, 
                referer_path=referer_path
            )
            
            if not response:
                logging.warning(f"Failed page {page}, continuing to next")
                continue
            
            records = self._extract_records(response)
            
            if not records:
                logging.info(f"Empty response on page {page}, stopping")
                break
            
            all_records.extend(records)
            logging.info(f"Page {page}/{end_page}: fetched {len(records)} records (total: {len(all_records)})")
            
            # Last page detection (kalau response < per_page, sudah habis)
            if len(records) < per_page:
                logging.info(f"Last page detected (got {len(records)} < {per_page})")
                break
            
            # Throttle: random delay sebelum next page (tapi tidak setelah page terakhir)
            if page < end_page:
                delay = random.uniform(delay_min, delay_max)
                logging.info(f"Throttle: waiting {delay:.1f}s...")
                time.sleep(delay)
        
        return all_records
    
    # ============================================
    # API 1: Video Library
    # ============================================
    def fetch_video_library(self, start_time: int, end_time: int,
                            per_page: int = 20,
                            start_page: int = 1, end_page: int = 7,
                            delay_min: int = 3, delay_max: int = 10) -> List[Dict]:
        """Fetch video library page 1-7"""
        
        def url_builder(page):
            return (
                f"{self.BASE_URL}/api/v1/data/videos"
                f"?page={page}"
                f"&per_page={per_page}"
                f"&influencer_categories="
                f"&product_categories="
                f"&start_time={start_time}"
                f"&end_time={end_time}"
                f"&sort=desc"
                f"&order=views_count"
            )
        
        return self._paginate_fetch(
            url_builder=url_builder,
            start_page=start_page,
            end_page=end_page,
            per_page=per_page,
            referer_path="/data/videos",
            delay_min=delay_min,
            delay_max=delay_max,
        )
    
    # ============================================
    # API 2: Hashtag Library
    # ============================================
    def fetch_hashtags(self, time_range: str = None,
                       time_type: str = "weekly",
                       per_page: int = 20,
                       start_page: int = 1, end_page: int = 7,
                       delay_min: int = 3, delay_max: int = 10) -> List[Dict]:
        """
        Fetch hashtag leaderboard (weekly).
        
        Args:
            time_range: Format "YYYYMMDD-YYYYMMDD" untuk weekly (7 hari)
                       Contoh: "20260223-20260301"
            time_type: "weekly" (default)
        """
        if not time_range:
            raise ValueError("time_range required untuk hashtag leaderboard")
        
        def url_builder(page):
            return (
                f"{self.BASE_URL}/api/v1/data/tags/leaderboard/top-hashtag"
                f"?time_type={time_type}"
                f"&time_range={time_range}"
                f"&page={page}"
                f"&influencer_categories="
                f"&product_categories="
                f"&per_page={per_page}"
            )
        
        referer_path = (
            f"/videos/leaderboard/top-hashtags"
            f"?time_type={time_type}"
            f"&time_range={time_range}"
            f"&page={start_page}"
        )
        
        return self._paginate_fetch(
            url_builder=url_builder,
            start_page=start_page,
            end_page=end_page,
            per_page=per_page,
            referer_path=referer_path,
            delay_min=delay_min,
            delay_max=delay_max,
        )
    
    # ============================================
    # API 3: Video Selling (sesuai cURL Nico)
    # ============================================
    def fetch_video_selling(self, time_range: str,
                            time_type: str = "monthly",
                            per_page: int = 20,
                            start_page: int = 1, end_page: int = 7,
                            delay_min: int = 3, delay_max: int = 10) -> List[Dict]:
        """
        Fetch video selling page 1-7.
        
        Args:
            time_range: 
                - "YYYYMMDD-YYYYMMDD" untuk monthly (e.g. "20260401-20260430")
                - "YYYYMMDD" untuk daily
            time_type: "monthly" atau "daily"
        """
        
        def url_builder(page):
            return (
                f"{self.BASE_URL}/api/v1/data/videos/leaderboard/sell-videos"
                f"?time_type={time_type}"
                f"&time_range={time_range}"
                f"&page={page}"
                f"&influencer_categories="
                f"&product_categories="
                f"&per_page={per_page}"
            )
        
        # Referer harus include time_type & time_range (sesuai cURL Nico)
        referer_path = (
            f"/videos/leaderboard/top-selling-videos"
            f"?time_type={time_type}"
            f"&time_range={time_range}"
            f"&page={start_page}"
        )
        
        return self._paginate_fetch(
            url_builder=url_builder,
            start_page=start_page,
            end_page=end_page,
            per_page=per_page,
            referer_path=referer_path,
            delay_min=delay_min,
            delay_max=delay_max,
        )
    
    def _extract_records(self, response):
        """Extract records dari API response (handle multiple structures)"""
        if isinstance(response, list):
            return response
        
        if isinstance(response, dict):
            # Echotik biasanya pakai 'data'
            for key in ['data', 'items', 'results', 'records', 'list']:
                if key in response and isinstance(response[key], list):
                    return response[key]
            
            # Nested data
            if 'data' in response and isinstance(response['data'], dict):
                for key in ['items', 'list', 'records', 'data']:
                    if key in response['data'] and isinstance(response['data'][key], list):
                        return response['data'][key]
        
        return []
