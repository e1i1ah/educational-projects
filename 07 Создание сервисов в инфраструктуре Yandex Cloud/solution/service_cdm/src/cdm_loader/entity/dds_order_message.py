from pydantic import BaseModel
from typing import List
from uuid import UUID


class Payload(BaseModel):
    order_id: UUID
    user_id: UUID
    category_id: UUID
    category_name: str
    product_id: UUID
    product_name: str
    quantity: int


class DdsOrderMessage(BaseModel):
    object_id: int
    object_type: str
    payload: List[Payload]
