from pydantic import BaseModel


class UserDds(BaseModel):
    id: int
    user_id: str
    user_name: str
    user_login: str
