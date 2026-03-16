import asyncio
import io
import json
import re
import time
from typing import Optional, Tuple, Dict, Any

import cloudscraper
from bs4 import BeautifulSoup

from ..ai.qrcode import QrCodeGenerator


class SaweriaError(Exception):
    pass


class SaweriaScraper(QrCodeGenerator):
    BACKEND = "https://backend.saweria.co"
    FRONTEND = "https://saweria.co"

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Referer": "https://saweria.co/",
        "Origin": "https://saweria.co",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }

    def __init__(self, timeout: int = 15):
        super().__init__()
        self.timeout = timeout
        self.scraper = cloudscraper.create_scraper()

    def _get_json(self, url: str, retries: int = 3) -> Dict[str, Any]:
        last_error = None
        for attempt in range(retries):
            try:
                res = self.scraper.get(url, headers=self.HEADERS, timeout=self.timeout)
                if not res.ok:
                    raise SaweriaError(f"GET {res.status_code}: {res.text}")
                return res.json()
            except Exception as e:
                last_error = e
                time.sleep(2 ** attempt)
        raise SaweriaError(f"GET request failed after retries: {last_error}")

    def _post_json(self, url: str, payload: Dict[str, Any], retries: int = 3) -> Dict[str, Any]:
        last_error = None
        for attempt in range(retries):
            try:
                res = self.scraper.post(
                    url,
                    json=payload,
                    headers=self.HEADERS,
                    timeout=self.timeout,
                )
                if not res.ok:
                    raise SaweriaError(f"POST {res.status_code}: {res.text}")
                return res.json()
            except Exception as e:
                last_error = e
                time.sleep(2 ** attempt)
        raise SaweriaError(f"POST request failed after retries: {last_error}")

    async def get_user_id(
        self,
        username: str,
        retries: int = 3,
        delay: int = 3,
    ) -> Optional[str]:
        if not isinstance(username, str) or not username.strip():
            raise ValueError("Username tidak valid")

        def _sync() -> Optional[str]:
            url = f"{self.FRONTEND}/{username.strip()}"
            for attempt in range(retries):
                try:
                    res = self.scraper.get(url, headers=self.HEADERS, timeout=self.timeout)
                    if res.ok:
                        soup = BeautifulSoup(res.text, "html.parser")
                        next_data = soup.find(id="__NEXT_DATA__")
                        if next_data:
                            data = json.loads(next_data.text)
                            page = data.get("props", {}).get("pageProps", {})
                            creator = page.get("data")
                            if creator and "id" in creator:
                                return creator["id"]
                except Exception:
                    pass
                time.sleep(delay * (attempt + 1))
            return None

        return await asyncio.to_thread(_sync)

    async def create_payment(
        self,
        user_id: str,
        amount: int,
        name: str,
        email: str,
        message: str,
        creator_name: str = "nsdev",
    ) -> Tuple[str, str, io.BytesIO, int]:
        if amount < 1000:
            raise ValueError("Jumlah minimum donasi adalah 1000")

        if "@" not in email:
            raise ValueError("Email tidak valid")

        payload = {
            "agree": True,
            "notUnderage": True,
            "message": message,
            "amount": amount,
            "payment_type": "qris",
            "vote": "",
            "currency": "IDR",
            "customer_info": {
                "first_name": name,
                "email": email,
                "phone": "",
            },
        }

        def _sync() -> Dict[str, Any]:
            return self._post_json(f"{self.BACKEND}/donations/{user_id}", payload).get("data", {})

        data = await asyncio.to_thread(_sync)

        qr_string = data.get("qr_string")
        transaction_id = data.get("id")
        amount_raw = data.get("amount_raw")

        if not qr_string or not transaction_id:
            raise SaweriaError("Invalid response from Saweria API")

        qr_bytes = await self.generate(
            data=qr_string,
            use_dots=True,
            glow_background=False,
            bottom_text="SCAN ME",
            creator_text=f"Created by: {creator_name}",
        )

        stream = io.BytesIO(qr_bytes)
        stream.name = f"{transaction_id}.png"

        return qr_string, transaction_id, stream, int(amount_raw or 0)

    async def check_paid_status(self, transaction_id: str) -> bool:
        def _sync() -> bool:
            data = self._get_json(
                f"{self.BACKEND}/donations/qris/{transaction_id}"
            ).get("data", {})
            status = str(data.get("status", "")).upper()
            if data.get("paid_at"):
                return True
            return status == "PAID"

        return await asyncio.to_thread(_sync)

    def get_amount(self, qr_text: str) -> Optional[int]:
        i = qr_text.find("54")
        if i == -1:
            return None

        try:
            length = int(qr_text[i + 2 : i + 4])
            value = qr_text[i + 4 : i + 4 + length]
            return int(value)
        except Exception:
            return None


__all__ = ["SaweriaScraper", "SaweriaError"]
