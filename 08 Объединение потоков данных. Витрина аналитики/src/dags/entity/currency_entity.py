from pydantic import BaseModel, validator
from uuid import UUID
from datetime import datetime
import json


class CurrencyPayload(BaseModel):
    date_update: datetime
    currency_code: int
    currency_code_with: int
    currency_with_div: float


class Currency(BaseModel):
    object_id: UUID
    object_type: str
    sent_dttm: datetime
    payload: CurrencyPayload

    @validator("payload", pre=True)
    def parse_payload(cls, v):
        if isinstance(v, str):
            return json.loads(v)
        return v
