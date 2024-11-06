from pydantic import BaseModel
from typing import List
from uuid import UUID


class PayloadOut(BaseModel):
    order_id: UUID
    user_id: UUID
    category_id: UUID
    category_name: str
    product_id: UUID
    product_name: str
    quantity: int


class Restaurant(BaseModel):
    id: str
    name: str


class User(BaseModel):
    id: str
    name: str
    login: str


class Product(BaseModel):
    id: str
    price: float
    quantity: int
    name: str
    category: str


class Payload(BaseModel):
    id: int
    date: str
    cost: float
    payment: float
    status: str
    restaurant: Restaurant
    user: User
    products: List[Product]


class StgOrderMessage(BaseModel):
    object_id: int
    object_type: str
    payload: Payload
