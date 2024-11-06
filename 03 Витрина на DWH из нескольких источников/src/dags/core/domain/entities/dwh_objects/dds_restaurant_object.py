from pydantic import BaseModel
from datetime import datetime


class RestaurantDds(BaseModel):
    id: int
    restaurant_id: str
    restaurant_name: str
    active_from: datetime
    active_to: datetime
