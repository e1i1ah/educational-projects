from pydantic import BaseModel
from datetime import datetime, date, time


class TimestampDds(BaseModel):
    id: int
    ts: datetime
    year: int
    month: int
    day: int
    time: time
    date: date
