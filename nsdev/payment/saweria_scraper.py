import asyncio
import contextlib
import io
import json
import re
import time
from typing import Optional

import cloudscraper25 as cloudscraper
from bs4 import BeautifulSoup

from ..ai.qrcode import QrCodeGenerator


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
        "Referer": "https://saweria.co/",
    }

    def __init__(self):
        super().__init__()
        self.scraper = cloudscraper.create_scraper()
        self._lock = asyncio.Lock()
        self._closed = False

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        with contextlib.suppress(Exception):
            self.scraper.close()

    async def aclose(self) -> None:
        self.close()

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("SaweriaScraper sudah ditutup.")

    async def get_user_id(self, username: str) -> Optional[str]:
        username = str(username or "").strip().lstrip("@")

        if not username:
            raise ValueError("Username Saweria tidak boleh kosong.")

        def request_user_id(retries: int = 3, delay: int = 5):
            self._ensure_open()
            url = f"{self.FRONTEND}/{username}"

            for attempt in range(retries):
                try:
                    with self.scraper.get(
                        url,
                        headers=self.HEADERS,
                        timeout=15,
                    ) as res:
                        if res.status_code != 200:
                            continue

                        soup = BeautifulSoup(res.text, "html.parser")
                        next_data = soup.find(id="__NEXT_DATA__")

                        if not next_data:
                            continue

                        data = json.loads(next_data.text)
                        user_id = (
                            data.get("props", {})
                            .get("pageProps", {})
                            .get("data", {})
                            .get("id")
                        )

                        if user_id:
                            return str(user_id)

                except Exception:
                    pass

                if attempt < retries - 1:
                    time.sleep(delay)

            return None

        async with self._lock:
            return await asyncio.to_thread(request_user_id)

    async def create_payment(
        self,
        user_id: str,
        amount: int,
        name: str,
        email: str,
        message: str,
        creator_name: str = "nsdev",
    ) -> dict:
        if int(amount) < 1000:
            raise ValueError("Jumlah minimum donasi adalah 1000.")

        payload = {
            "agree": True,
            "notUnderage": True,
            "message": str(message),
            "amount": int(amount),
            "payment_type": "qris",
            "vote": "",
            "currency": "IDR",
            "customer_info": {
                "first_name": str(name or "User"),
                "email": str(email),
                "phone": "",
            },
        }

        def create():
            self._ensure_open()

            with self.scraper.post(
                f"{self.BACKEND}/donations/{user_id}",
                json=payload,
                headers=self.HEADERS,
                timeout=15,
            ) as res:
                if not res.ok:
                    raise RuntimeError(
                        f"Gagal membuat pembayaran Saweria: "
                        f"{res.status_code} {res.text[:300]}"
                    )

                body = res.json()
                data = body.get("data")

                if not isinstance(data, dict):
                    raise RuntimeError(
                        "Response Saweria tidak memiliki data pembayaran."
                    )

                return data

        async with self._lock:
            data = await asyncio.to_thread(create)

        qr_string = str(data.get("qr_string") or "").strip()
        transaction_id = str(data.get("id") or "").strip()
        amount_raw = int(data.get("amount_raw") or amount)

        if not transaction_id:
            raise RuntimeError(
                "Saweria tidak mengembalikan transaction ID."
            )

        if not qr_string:
            raise RuntimeError(
                "Saweria tidak mengembalikan QR string."
            )

        qr_url = self.generate_stylish_qr(
            qr_string,
            size="700x700",
            style=1,
            color="000000",
        )

        qr_bytes = await self.generate(
            data=qr_string,
            use_dots=True,
            glow_background=False,
            bottom_text="SCAN ME",
            creator_text=f"Created by: {creator_name}",
        )

        qr_stream = io.BytesIO(qr_bytes)
        qr_stream.name = f"{transaction_id}.png"
        qr_stream.seek(0)

        return {
            "trx_id": transaction_id,
            "transaction_id": transaction_id,
            "qr_string": qr_string,
            "qr_url": qr_url,
            "qr_stream": qr_stream,
            "amount": amount_raw,
            "amount_raw": amount_raw,
            "raw": data,
        }

    async def check_paid_status(
        self,
        transaction_id: str,
    ) -> bool:
        transaction_id = str(transaction_id or "").strip()

        if not transaction_id:
            raise ValueError(
                "Transaction ID tidak boleh kosong."
            )

        def check():
            self._ensure_open()

            with self.scraper.get(
                f"{self.BACKEND}/donations/qris/{transaction_id}",
                headers=self.HEADERS,
                timeout=15,
            ) as res:
                if not res.ok:
                    raise RuntimeError(
                        f"Transaction ID tidak ditemukan: "
                        f"{res.status_code}"
                    )

                data = res.json().get("data") or {}
                return str(data.get("qr_string") or "") == ""

        async with self._lock:
            return bool(
                await asyncio.to_thread(check)
            )

    def get_amount(
        self,
        qr_text: str,
    ) -> Optional[int]:
        match = re.search(
            r"54(\d{2})(\d+)",
            str(qr_text or ""),
        )

        if not match:
            return None

        length = int(match.group(1))
        value = match.group(2)[:length]

        try:
            return int(value)
        except ValueError:
            return None
