import asyncio
import io
import json
import time
import re
import logging
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
    }

    def __init__(self, timeout: int = 15):
        super().__init__()
        self.timeout = timeout
        self.scraper = cloudscraper.create_scraper()

    def _get_json(self, url: str) -> Dict[str, Any]:
        res = self.scraper.get(url, headers=self.HEADERS, timeout=self.timeout)
        if not res.ok:
            raise SaweriaError(f"GET {res.status_code}: {res.text}")
        return res.json()

    def _post_json(self, url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        res = self.scraper.post(
            url,
            json=payload,
            headers=self.HEADERS,
            timeout=self.timeout,
        )
        if not res.ok:
            raise SaweriaError(f"POST {res.status_code}: {res.text}")
        return res.json()

    async def get_user_id(
        self,
        username: str,
        retries: int = 3,
        delay: int = 5,
    ) -> Optional[str]:
        if not isinstance(username, str) or not username.strip():
            raise ValueError("Username tidak valid")

        def _sync() -> Optional[str]:
            url = f"{self.FRONTEND}/{username}"
            for _ in range(retries):
                try:
                    res = self.scraper.get(
                        url,
                        headers=self.HEADERS,
                        timeout=self.timeout,
                    )
                    if res.ok:
                        soup = BeautifulSoup(res.text, "html.parser")
                        next_data = soup.find(id="__NEXT_DATA__")
                        if not next_data:
                            continue
                        data = json.loads(next_data.text)
                        return (
                            data.get("props", {})
                            .get("pageProps", {})
                            .get("data", {})
                            .get("id")
                        )
                except Exception:
                    pass
                time.sleep(delay)
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
            data = self._post_json(
                f"{self.BACKEND}/donations/{user_id}",
                payload,
            )
            return data.get("data", {})

        data = await asyncio.to_thread(_sync)

        qr_string = data["qr_string"]
        transaction_id = data["id"]
        amount_raw = data["amount_raw"]

        qr_bytes = await self.generate(
            data=qr_string,
            use_dots=True,
            glow_background=False,
            bottom_text="SCAN ME",
            creator_text=f"Created by: {creator_name}",
        )

        stream = io.BytesIO(qr_bytes)
        stream.name = f"{transaction_id}.png"

        return qr_string, transaction_id, stream, amount_raw

    async def check_paid_status(self, transaction_id: str) -> bool:
        def _sync() -> bool:
            data = self._get_json(
                f"{self.BACKEND}/donations/qris/{transaction_id}"
            ).get("data", {})
            if data.get("paid_at"):
                return True
            if data.get("status"):
                return data["status"].upper() == "PAID"
            return False

        return await asyncio.to_thread(_sync)

    def get_amount(self, qr_text: str) -> Optional[int]:
        matches = re.findall(r"54(\d{2})(\d+)", qr_text)
        if not matches:
            return None
        length, value = matches[-1]
        try:
            return int(value[: int(length)])
        except Exception:
            return None
