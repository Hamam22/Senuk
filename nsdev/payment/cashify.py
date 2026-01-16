import httpx
from ..data.ymlreder import YamlHandler


class PaymentCashify:
    def __init__(self, license_key: str):
        if not license_key:
            raise ValueError("license_key wajib diisi")

        self.license_key = license_key
        self.base_url = "https://cashify.my.id/api"
        self.convert = YamlHandler()
        self.qr_generator_url = "https://larabert-qrgen.hf.space/v1/create-qr-code"
        self.timeout = httpx.Timeout(30.0)

    def _get_headers(self):
        return {
            "x-license-key": self.license_key,
            "content-type": "application/json",
        }

    async def generate_qris(
        self,
        qris_id: str,
        amount: int,
        use_unique_code: bool = True,
        package_ids: list = None,
        expired_in_minutes: int = 15,
    ):
        url = f"{self.base_url}/generate/qris"

        if package_ids is None:
            package_ids = ["id.dana"]

        if expired_in_minutes < 15:
            expired_in_minutes = 15
        elif expired_in_minutes > 1440:
            expired_in_minutes = 1440

        payload = {
            "id": qris_id,
            "amount": amount,
            "useUniqueCode": use_unique_code,
            "packageIds": package_ids,
            "expiredInMinutes": expired_in_minutes,
        }

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                res = await client.post(url, headers=self._get_headers(), json=payload)
                res.raise_for_status()
                return self.convert._convertToNamespace(res.json())
        except httpx.HTTPStatusError as e:
            raise Exception(f"HTTP error from Cashify API: {e.response.status_code} - {e.response.text}")
        except httpx.RequestError as e:
            raise Exception(f"Error communicating with Cashify API: {e}")
        except Exception as e:
            raise Exception(f"Error generating Cashify QRIS: {e}")

    async def check_status(self, payment_id: str):
        url = f"{self.base_url}/generate/check-status"
        payload = {"transactionId": payment_id}

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                res = await client.post(url, headers=self._get_headers(), json=payload)
                res.raise_for_status()
                return self.convert._convertToNamespace(res.json())
        except httpx.HTTPStatusError as e:
            raise Exception(f"HTTP error from Cashify API: {e.response.status_code} - {e.response.text}")
        except httpx.RequestError as e:
            raise Exception(f"Error communicating with Cashify API: {e}")
        except Exception as e:
            raise Exception(f"Error checking Cashify payment status: {e}")

    async def cancel_payment(self, transaction_id: str):
        url = f"{self.base_url}/generate/cancel-status"
        payload = {"transactionId": transaction_id}

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                res = await client.post(url, headers=self._get_headers(), json=payload)
                res.raise_for_status()
                return self.convert._convertToNamespace(res.json())
        except httpx.HTTPStatusError as e:
            raise Exception(f"HTTP error from Cashify API: {e.response.status_code} - {e.response.text}")
        except httpx.RequestError as e:
            raise Exception(f"Error communicating with Cashify API: {e}")
        except Exception as e:
            raise Exception(f"Error cancel Cashify payment: {e}")

    def generate_stylish_qr(
        self,
        data: str,
        size: str = "500x500",
        style: int = None,
        color: str = None,
    ):
        style = style if style else 1
        color = "000000"
        return f"{self.qr_generator_url}?size={size}&style={style}&color={color}&data={data}"

    async def download_qr_image(
        self,
        data: str,
        size: str = "500x500",
        style: int = None,
        color: str = None,
    ):
        qr_url = self.generate_stylish_qr(data, size, style, color)

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                res = await client.get(qr_url)
                res.raise_for_status()
                return res.content
        except httpx.HTTPStatusError as e:
            raise Exception(f"HTTP error downloading QR image: {e.response.status_code}")
        except httpx.RequestError as e:
            raise Exception(f"Error downloading QR image: {e}")
        except Exception as e:
            raise Exception(f"Error generating QR image: {e}")
