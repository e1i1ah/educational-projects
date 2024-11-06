from pydantic import BaseModel
from datetime import datetime


class OriginEventObj(BaseModel):
    id: int
    event_ts: datetime
    event_type: str
    event_value: str
