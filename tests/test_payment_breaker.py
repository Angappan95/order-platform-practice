import httpx
import pytest
from aiobreaker import CircuitBreakerError

from services.order_service.app.infrastructure.payment_client import (
    PaymentClient,
    PaymentDeclined,
    PaymentUnavailable,
    payment_breaker
)

class FixedStatusTransport:
    def __init__(self, status_code):
        self.status_code = status_code
        self.calls = 0

    async def post(self, url, **kwargs):
        self.calls += 1
        return httpx.Response(self.status_code, request=httpx.Request("POST", url))

@pytest.fixture(autouse=True)
def reset_breaker():
    payment_breaker.close()
    yield
    payment_breaker.close()

@pytest.mark.asyncio
async def test_breaker_opens_after_five_unavailable_responses():
    transport = FixedStatusTransport(500)
    client = PaymentClient("http://payment.com", transport)

    for _ in range(4):
        with pytest.raises(PaymentUnavailable):
            await client.authorize("order-1", 10)

    with pytest.raises(CircuitBreakerError):  # 5th failure opens the circuit
        await client.authorize("order-1", 10)

    calls_before = transport.calls
    with pytest.raises(CircuitBreakerError):  # open circuit: no request sent
        await client.authorize("order-1", 10)

    assert transport.calls == calls_before

@pytest.mark.asyncio
async def test_decline_never_open_the_breaker():
    transport = FixedStatusTransport(402)
    client = PaymentClient("http://payment.com", transport)

    for _ in range(10):
        with pytest.raises(PaymentDeclined):
            await client.authorize("order-1", 10)

        