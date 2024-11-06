from pydantic import BaseModel


class OriginRankObj(BaseModel):
    id: int
    name: str
    bonus_percent: float
    min_payment_threshold: float
