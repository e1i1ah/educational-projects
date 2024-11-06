from pydantic import BaseModel
from datetime import datetime


class OriginApiObj(BaseModel):
    object_id: str
    object_value: str
    update_ts: datetime
