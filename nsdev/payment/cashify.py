from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

import httpx
from ..data.ymlreder import YamlHandler


class CashifyError(Exception):
    pass


class CashifyHTTPError(CashifyError):
    pass


class CashifyConnectionError(CashifyError):
    pass


class PaymentCashify:
    def __init__(
        self,
        license_key: str,
        *,
        timeout: float = 30.0,
        max_retries: int = 3,
    ):
        if not license_key:
            raise ValueError("license_key wajib diisi")

        self.license_key = license_key
        self.base_url = "https://cashify.my.id/api"
        self.qr_generator_url = "https://larabert-qrgen.hf.space/v1/create-qr-code"

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
            "x-license-key": self.license_key,
            "content-type": "application/json",
        }

    async def _request(
        self,
        method: str,
        endpoint: str,
        *,
        json: Optional[Dict[str, Any]] = None,
        raw: bool = False,
    ):
        url = endpoint if endpoint.startswith("http") else f"{self.base_url}{endpoint}"

        for attempt in range(self.max_retries):
            try:
                client = self._client or httpx.AsyncClient(timeout=self.timeout)

                if method == "POST":
                    res = await client.post(url, headers=self._headers(), json=json)
                else:
                    res = await client.get(url, headers=self._headers())

                res.raise_for_status()
                return res.content if raw else self.convert._convertToNamespace(res.json())

            except httpx.HTTPStatusError as e:
                raise CashifyHTTPError(
                    f"{e.response.status_code} - {e.response.text}"
                ) from e

            except httpx.RequestError as e:
                if attempt == self.max_retries - 1:
                    raise CashifyConnectionError(str(e)) from e
                await asyncio.sleep(2**attempt)

            finally:
                if not self._client:
                    await client.aclose()

    async def generate_qris(
        self,
        qris_id: str,
        amount: int,
        use_unique_code: bool = True,
        package_ids: Optional[List[str]] = None,
        expired_in_minutes: int = 15,
    ):
        if amount <= 0:
            raise ValueError("amount harus lebih dari 0")

        package_ids = package_ids or ["id.dana"]
        expired_in_minutes = max(15, min(expired_in_minutes, 1440))

        payload = {
            "id": qris_id,
            "amount": amount,
            "useUniqueCode": use_unique_code,
            "packageIds": package_ids,
            "expiredInMinutes": expired_in_minutes,
        }

        return await self._request("POST", "/generate/qris", json=payload)

    async def check_status(self, payment_id: str):
        payload = {"transactionId": payment_id}
        return await self._request("POST", "/generate/check-status", json=payload)

    async def cancel_payment(self, transaction_id: str):
        payload = {"transactionId": transaction_id}
        return await self._request("POST", "/generate/cancel-status", json=payload)

    def generate_stylish_qr(
        self,
        data: str,
        size: str = "500x500",
        style: Optional[int] = None,
        color: Optional[str] = None,
    ):
        style = style or 1
        color = color or "000000"
        return f"{self.qr_generator_url}?size={size}&style={style}&color={color}&data={data}"

    async def download_qr_image(
        self,
        data: str,
        size: str = "500x500",
        style: Optional[int] = None,
        color: Optional[str] = None,
    ):
        qr_url = self.generate_stylish_qr(data, size, style, color)
        return await self._request("GET", qr_url, raw=True)
