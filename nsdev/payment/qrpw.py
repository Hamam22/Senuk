from __future__ import annotations

import asyncio
from typing import Any, Dict, Optional

import httpx
from ..data.ymlreder import YamlHandler


class QRPWError(Exception):
    pass


class QRPWHTTPError(QRPWError):
    pass


class QRPWConnectionError(QRPWError):
    pass


class QRPWStatus:
    PENDING = "pending"
    PAID = "paid"
    EXPIRED = "expired"
    FAILED = "failed"
    CANCELLED = "cancelled"


class PaymentQRPW:
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        *,
        timeout: float = 30.0,
        max_retries: int = 3,
    ):
        if not api_key:
            raise ValueError("api_key wajib diisi")
        if not api_secret:
            raise ValueError("api_secret wajib diisi")

        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = "https://qris.pw/api"

        self.timeout = httpx.Timeout(timeout)
        self.max_retries = max_retries
        self.convert = YamlHandler()

        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self):
        self._client = httpx.AsyncClient(timeout=self.timeout)
        return self

    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()

    def _headers(self) -> Dict[str, str]:
        return {
            "X-API-Key": self.api_key,
            "X-API-Secret": self.api_secret,
            "Content-Type": "application/json",
        }

    async def _request(
        self,
        method: str,
        endpoint: str,
        *,
        json: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        raw: bool = False,
    ):
        url = endpoint if endpoint.startswith("http") else f"{self.base_url}{endpoint}"

        for attempt in range(self.max_retries):
            client = self._client or httpx.AsyncClient(timeout=self.timeout)
            try:
                if method.upper() == "POST":
                    res = await client.post(
                        url,
                        headers=self._headers(),
                        json=json,
                        params=params,
                    )
                else:
                    res = await client.get(
                        url,
                        headers=self._headers(),
                        params=params,
                    )

                res.raise_for_status()
                return res.content if raw else self.convert._convertToNamespace(res.json())

            except httpx.HTTPStatusError as e:
                raise QRPWHTTPError(
                    f"{e.response.status_code} - {e.response.text}"
                ) from e

            except httpx.RequestError as e:
                if attempt == self.max_retries - 1:
                    raise QRPWConnectionError(str(e)) from e
                await asyncio.sleep(2**attempt)

            finally:
                if not self._client:
                    await client.aclose()

    async def create_payment(
        self,
        amount: int,
        order_id: Optional[str] = None,
        customer_name: Optional[str] = None,
        customer_phone: Optional[str] = None,
        callback_url: Optional[str] = None,
    ):
        if amount < 1000:
            raise ValueError("amount minimal 1000")

        payload = {"amount": amount}

        if order_id:
            payload["order_id"] = order_id
        if customer_name:
            payload["customer_name"] = customer_name
        if customer_phone:
            payload["customer_phone"] = customer_phone
        if callback_url:
            payload["callback_url"] = callback_url

        return await self._request(
            "POST",
            "/create-payment.php",
            json=payload,
        )

    async def check_status(self, transaction_id: str):
        if not transaction_id:
            raise ValueError("transaction_id wajib diisi")

        return await self._request(
            "GET",
            "/check-payment.php",
            params={"transaction_id": transaction_id},
        )

    @staticmethod
    def is_pending(status: str) -> bool:
        return status == QRPWStatus.PENDING

    @staticmethod
    def is_paid(status: str) -> bool:
        return status == QRPWStatus.PAID

    @staticmethod
    def is_expired(status: str) -> bool:
        return status == QRPWStatus.EXPIRED

    @staticmethod
    def is_failed(status: str) -> bool:
        return status == QRPWStatus.FAILED

    @staticmethod
    def is_cancelled(status: str) -> bool:
        return status == QRPWStatus.CANCELLED
