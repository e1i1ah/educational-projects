from pydantic import BaseModel
from datetime import datetime


class ProductDds(BaseModel):
    id: int
    restaurant_id: int
    product_id: str
    product_name: str
    product_price: float
    active_from: datetime
    active_to: datetime
