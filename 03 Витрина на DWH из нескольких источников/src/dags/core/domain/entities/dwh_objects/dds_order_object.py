from pydantic import BaseModel


class OrderDds(BaseModel):
    id: int
    user_id: int
    restaurant_id: int
    timestamp_id: int
    order_key: str
    order_status: str
