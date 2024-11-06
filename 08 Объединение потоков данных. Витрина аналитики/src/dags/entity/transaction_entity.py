from pydantic import BaseModel, validator
from uuid import UUID
from datetime import datetime
import json


class TransactionPayload(BaseModel):
    operation_id: UUID
    account_number_from: int
    account_number_to: int
    currency_code: int
    country: str
    status: str
    transaction_type: str
    amount: float
    transaction_dt: datetime


class Transaction(BaseModel):
    object_id: UUID
    object_type: str
    sent_dttm: datetime
    payload: TransactionPayload

    @validator("payload", pre=True)
    def parse_payload(cls, v):
        if isinstance(v, str):
            return json.loads(v)
        return v
