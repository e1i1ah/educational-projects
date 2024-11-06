from pydantic import BaseModel


class OriginUserObj(BaseModel):
    id: int
    order_user_id: str
