from pydantic import BaseModel

# TODO maybe pair should be something else idk
class Pair(BaseModel):
    base: str
    quote: str

# TODO should define all pairs acceptable by miners and validators