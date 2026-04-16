from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
from typing import Any, Dict, Optional

import httpx
from ..data.ymlreder import YamlHandler


class QRPWError(Exception):
    pass


class QRPWHTTPError(QRPWError):
    pass


class QRPWConnectionError(QRPWError):
    pass


class PaymentQRPW:
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        *,
        webhook_secret: Optional[str] = None,
        timeout: float = 30.0,
        max_retries: int = 3,
    ):
        if not api_key:
            raise ValueError("api_key wajib diisi")
        if not api_secret:
            raise ValueError("api_secret wajib diisi")

        self.api_key = api_key
        self.api_secret = api_secret
        self.webhook_secret = webhook_secret

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
            "Content-Type": "application/json",
            "X-API-Key": self.api_key,
            "X-API-Secret": self.api_secret,
        }

    async def _request(
        self,
        method: str,
        endpoint: str,
        *,
        json_data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        raw: bool = False,
    ):
        url = endpoint if endpoint.startswith("http") else f"{self.base_url}{endpoint}"

        for attempt in range(self.max_retries):
            try:
                client = self._client or httpx.AsyncClient(timeout=self.timeout)

                response = await client.request(
                    method=method,
                    url=url,
                    headers=self._headers(),
                    json=json_data,
                    params=params,
                )
                response.raise_for_status()

                if raw:
                    return response.content

                data = response.json()
                return self.convert._convertToNamespace(data)

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
        order_id: str,
        customer_name: str,
        callback_url: str,
    ):
        if amount <= 0:
            raise ValueError("amount harus lebih dari 0")
        if not order_id:
            raise ValueError("order_id wajib diisi")
        if not customer_name:
            raise ValueError("customer_name wajib diisi")
        if not callback_url:
            raise ValueError("callback_url wajib diisi")

        payload = {
            "amount": amount,
            "order_id": order_id,
            "customer_name": customer_name,
            "callback_url": callback_url,
        }

        return await self._request(
            "POST",
            "/create-payment.php",
            json_data=payload,
        )

    async def check_status(self, transaction_id: str):
        if not transaction_id:
            raise ValueError("transaction_id wajib diisi")

        return await self._request(
            "GET",
            "/check-payment.php",
            params={"transaction_id": transaction_id},
        )

    def verify_webhook_signature(
        self,
        webhook_data: Dict[str, Any],
        webhook_secret: Optional[str] = None,
    ) -> bool:
        secret = webhook_secret or self.webhook_secret
        if not secret:
            raise ValueError("webhook_secret wajib diisi")

        if "signature" not in webhook_data:
            return False

        payload_to_sign = {k: v for k, v in webhook_data.items() if k != "signature"}
        payload = json.dumps(payload_to_sign, separators=(",", ":"), ensure_ascii=False)

        expected_signature = hmac.new(
            secret.encode(),
            payload.encode(),
            hashlib.sha256,
        ).hexdigest()

        return hmac.compare_digest(expected_signature, str(webhook_data["signature"]))
