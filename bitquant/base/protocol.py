from typing import List, Dict, Tuple, AsyncIterator
from pydantic import BaseModel, validator, Field, ValidationError
import bittensor as bt
from starlette.responses import StreamingResponse

from bitquant.base.pair import Portfolio

class MinerEvaluationWindow(BaseModel):
    start: int
    end: int

    @validator('end')
    def check_start_less_than_end(cls, v, values, **kwargs):
        if 'start' in values and v <= values['start']:
            raise ValueError('end must be greater than start')
        return v

# need BaseModel to type check
class PortfolioModel(BaseModel):
    portfolio: Portfolio



class StreamingTradeHistory(bt.StreamingSynapse):

    miner_window: MinerEvaluationWindow = Field(...)
    new_portfolio: PortfolioModel = Field(...)
    # portfolio_history: List[Portfolio] = Field(default_factory=list())


    def deserialize(self) -> List[Portfolio]:
        return self.new_portfolio.portfolio

    # TODO unsure about this
    async def process_streaming_response(self, response: StreamingResponse) -> AsyncIterator[PortfolioModel]:
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


if __name__ == "__main__":
    p = Portfolio({"BTCUSDT":1})
    p = p.update_portfolio({"BTCUSDT":2})
    print(p)
    # PortfolioModel(portfolio=p)
