from pydantic import BaseModel

class UserCreateRequest(BaseModel):
    phone_number: str