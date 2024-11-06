from pydantic import BaseModel


class CouriertDds(BaseModel):
    id: int
    courier_id: str
    courier_name: str
