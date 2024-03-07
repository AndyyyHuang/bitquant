from pydantic import BaseModel

class Pair(BaseModel):
    base: str
    quote: str

# TODO should define all pairs acceptable by miners and validators