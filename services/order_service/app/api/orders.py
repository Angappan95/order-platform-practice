from uuid import UUID
from decimal import Decimal
from fastapi import APIRouter, Body, Header, HTTPException, status
from pydantic import BaseModel, Field

router = APIRouter(prefix="/v1/orders", tags=["orders"])

class OrderLine(BaseModel):
    sku: str = Field(min_length=1)
    quantity: int = Field(gt=0)
    unit_price: Decimal = Field(gt=0)

class CreateOrderRequest(BaseModel):
    customer_id: UUID
    lines: list[OrderLine] = Field(min_length=1)

@router.post("", status_code=status.HTTP_202_ACCEPTED)
async def create_order(body: CreateOrderRequest, idempotency_key: str=Header(min_length=10, max_length=128)):
    return {
        "order_id": "123456-78910", 
        "status": "PENDING"
        }

    