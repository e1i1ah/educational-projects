from pydantic import BaseModel
from datetime import datetime


class OriginMongoObj(BaseModel):
    object_id: str
    object_value: str
    update_ts: datetime
