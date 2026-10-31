from hashlib import sha256
from json import dumps
from uuid import uuid4
from decimal import Decimal
from sqlalchemy import text
from ..api.errors import DomainError

async def create_order(session, command, idempotency_key):
    
    payload = command
    request_hash = sha256(dumps(payload, sort_keys=True).encode()).hexdigest()
    order_id = uuid4()

    async with session.begin():
        result = await session.execute(text(
            """
            INSERT INTO idempotency_keys(key, request_hash, order_id)
            VALUES (:key, :hash, :order_id)
            ON CONFLICT (key) DO NOTHING
            RETURNING order_id
            """
        ))

        inserted = result.scalar_one_or_none()

        if inserted is None:
            existing = await session.execute(
                text("SELECT request_hash, order_id FROM idempotency_keys WHERE key = :key"),
                {"key": idempotency_key})
            old_hash, old_order_id = existing.one()

            # Raise if Duplicate Order id for different purchase
            if old_hash != request_hash:
                raise DomainError("IDEMPOTENCY_KEY_REFUSED", "Key belong to another request", 422)

            return old_order_id, True

        total = sum(
            Decimal(str(line["unit_price"])) * int(line["quantity"]) for line in command["lines"]
        )

        await session.execute(
            text("""
            INSERT INTO orders(id, customer_id, status, total)
            VALUES (:id, :customer_id, 'PENDING', :total)
            """),
            {"id": order_id, "customer_id": command["customer_id"], "total": total}
        )
        return order_id, False
            