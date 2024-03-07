from typing import List, Dict, Tuple, AsyncIterator
from pydantic import BaseModel, validator, Field, ValidationError
import bittensor as bt
from starlette.responses import StreamingResponse

from bitquant.base.pair import Pair

class Portfolio(BaseModel):
    holdings: Dict[Pair, float]

class Trade(BaseModel):
    pair: Pair
    time: int # TODO idk about this

# TODO not sure if possible to pass pydantic objects through. Otherwise it'll have to be a string and parsed in process_streaming_resp
class StreamingTradeHistory(bt.StreamingSynapse):

    portfolio_start: Portfolio = Field(...)

    trade_history: List[Trade] = Field(default_factory=list())


    def deserialize(self) -> Tuple[Portfolio, List[Trade]]:
        return self.portfolio_start, self.trade_history

    # TODO unsure about this
    async def process_streaming_response(self, response: StreamingResponse) -> AsyncIterator[Trade]:
        if self.trade_history is None:
            self.trade_history = []

        try:
            async for chunk in response.content.iter_any():
                # bytes -> string
                chunk - chunk.decode("utf-8")

                ...


        except ValidationError:
            ...


    # TODO unsure about this
    def extract_response_json(self, response: StreamingResponse) -> dict:
        headers = {
            k.decode("utf-8"): v.decode("utf-8")
            for k, v in response.__dict__["_raw_headers"]
        }

        def extract_info(prefix):
            return {
                key.split("_")[-1]: value
                for key, value in headers.items()
                if key.startswith(prefix)
            }

        return {
            "name": headers.get("name", ""),
            "timeout": float(headers.get("timeout", 0)),
            "total_size": int(headers.get("total_size", 0)),
            "header_size": int(headers.get("header_size", 0)),
            "dendrite": extract_info("bt_header_dendrite"),
            "axon": extract_info("bt_header_axon"),
            "portfolio_start": self.portfolio_start,
            "trade_history": self.trade_history,
        }
