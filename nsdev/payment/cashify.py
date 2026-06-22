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
    RETRYABLE_STATUSES = {429, 502, 503, 504}

    def __init__(
        self,
        license_key: str,
        *,
        timeout: float = 30.0,
        max_retries: int = 3,
        base_url: str = "https://api.casaku.id/api",
        qr_generator_url: str = "https://larabert-qrgen.hf.space/v1/create-qr-code",
        package_ids: Optional[List[str]] = None,
    ):
        if not license_key:
            raise ValueError("license_key wajib diisi")

        self.license_key = license_key
        self.base_url = base_url.rstrip("/")
        self.qr_generator_url = qr_generator_url
        self.package_ids = package_ids or []

        self.timeout = httpx.Timeout(timeout, connect=10.0, read=30.0, write=10.0)
        self.max_retries = max(1, int(max_retries))
        self.convert = YamlHandler()

        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self):
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self.timeout)
        return self

    async def __aexit__(self, *args):
        await self.aclose()

    async def aclose(self):
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def _headers(self, *, expect_json: bool = True) -> Dict[str, str]:
        headers = {
            "x-license-key": self.license_key,
        }

        if expect_json:
            headers["content-type"] = "application/json"
            headers["accept"] = "application/json"

        return headers

    @staticmethod
    def _preview_text(text: str, limit: int = 300) -> str:
        return (text or "").replace("\n", " ").replace("\r", " ")[:limit]

    async def _request(
        self,
        method: str,
        endpoint: str,
        *,
        json: Optional[Dict[str, Any]] = None,
        raw: bool = False,
    ):
        url = endpoint if endpoint.startswith("http") else f"{self.base_url}{endpoint}"

        own_client = self._client is None
        client = self._client or httpx.AsyncClient(timeout=self.timeout)

        try:
            for attempt in range(self.max_retries):
                try:
                    response = await client.request(
                        method=method.upper(),
                        url=url,
                        headers=self._headers(expect_json=not raw),
                        json=json if method.upper() != "GET" else None,
                    )

                    if (
                        response.status_code in self.RETRYABLE_STATUSES
                        and attempt < self.max_retries - 1
                    ):
                        await asyncio.sleep(2**attempt)
                        continue

                    response.raise_for_status()

                    if raw:
                        return response.content

                    content_type = response.headers.get("content-type", "").lower()
                    if "application/json" not in content_type:
                        preview = self._preview_text(response.text)
                        raise CashifyHTTPError(
                            f"Respons non-JSON dari server: {response.status_code} - {preview}"
                        )

                    return self.convert._convertToNamespace(response.json())

                except httpx.HTTPStatusError as e:
                    status_code = e.response.status_code
                    preview = self._preview_text(e.response.text)

                    if (
                        status_code in self.RETRYABLE_STATUSES
                        and attempt < self.max_retries - 1
                    ):
                        await asyncio.sleep(2**attempt)
                        continue

                    raise CashifyHTTPError(f"{status_code} - {preview}") from e

                except httpx.RequestError as e:
                    if attempt >= self.max_retries - 1:
                        raise CashifyConnectionError(str(e)) from e
                    await asyncio.sleep(2**attempt)

        finally:
            if own_client:
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

        package_ids = package_ids or self.package_ids or ["id.dana"]
        expired_in_minutes = max(15, min(int(expired_in_minutes), 1440))

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
    ) -> str:
        style = style or 1
        color = color or "000000"
        return (
            f"{self.qr_generator_url}"
            f"?size={size}&style={style}&color={color}&data={data}"
        )

    async def download_qr_image(
        self,
        data: str,
        size: str = "500x500",
        style: Optional[int] = None,
        color: Optional[str] = None,
    ):
        qr_url = self.generate_stylish_qr(data, size, style, color)
        return await self._request("GET", qr_url, raw=True)
