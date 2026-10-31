import httpx
from datetime import timedelta
from aiobreaker import CircuitBreaker

class PaymentDeclined(Exception):
    pass

class PaymentUnavailable(Exception):
    pass

payment_breaker = CircuitBreaker(
    fail_max=5,
    timeout_duration=timedelta(seconds=30),
    exclude=[PaymentDeclined]
)

class PaymentClient:
    def __init__(self, base_url, client) -> None:
        self.base_url, self.client = base_url, client

    @payment_breaker
    async def authorize(self, order_id, amount):
        try:
            response = await self.client.post(
                f"{self.base_url}/authorizations",
                json = {"order_id": order_id, "amount": amount},
                headers = {"Idempotency-Key": f"payment:{order_id}"},
                timeout = httpx.Timeout(connect=0.2, read=0.8, write=0.2, pool=0.2)
            )
            if 400 <= response.status_code < 500:
                raise PaymentDeclined("Payment was declined or request was invalid")
            if response.status_code >= 500:
                raise PaymentUnavailable(f"Payment returned {response.status_code}")
            return response.json()["authorization_id"]
        except httpx.TimeoutException as exc:
            raise PaymentUnavailable("Payment timed out") from exc
        except httpx.TransportError as exc:
            raise PaymentUnavailable("Payment transport failed") from exc