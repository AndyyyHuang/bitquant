from typing import List, Dict, Tuple, AsyncIterator
from pydantic import BaseModel, validator, Field, ValidationError
import bittensor as bt
from starlette.responses import StreamingResponse

from bitquant.base.pair import Portfolio

class MinerEvaluationWindow(BaseModel):
    start_ms: int
    end_ms: int

    @validator('end')
    def check_start_less_than_end(cls, v, values, **kwargs):
        if 'start' in values and v <= values['start']:
            raise ValueError('end must be greater than start')
        return v

# need BaseModel to type check
class PortfolioModel(BaseModel):
    portfolio: Portfolio
    timestamp_ms: int

    @validator('timestamp_ms')
    def check_timestamp_length(cls, v):
        if len(str(v)) != 13:
            raise ValueError("timestamp_ms should be 13 digits long")
        return v

    # HACK figure out why type checking is not happening automatically
    @validator('portfolio', pre=True)
    def check_portfolio_values(cls, value):
        if not all(isinstance(v, (int, float)) for v in value.values()):
            raise ValueError("All values in the portfolio must be integers or floats")
        if any(v == float('inf') or v == float('-inf') or v == float('nan') for v in value.values()):
            raise ValueError("There can't be inf, -inf, or nan in the portfolio")
        return value



class StreamingTradeHistory(bt.StreamingSynapse):

    miner_window: MinerEvaluationWindow = Field(..., allow_mutation=False)
    portfolio_history: List[PortfolioModel] = Field(default_factory=list(), allow_mutation=True)

    # @validator('portfolio_history', each_item=True)
    # def check_timestamp_in_range(cls, v, values, **kwargs):
    #     if 'miner_window' in values:
    #         miner_window = values['miner_window']
    #         if not (miner_window.start <= v.timestamp_ms <= miner_window.end):
    #             raise ValueError(f"timestamp_ms {v.timestamp_ms} is not between {miner_window.start} and {miner_window.end}")
    #     return v
    @validator('portfolio_history')
    def check_timestamp_in_range(cls, portfolio_history, values, **kwargs):
        if 'miner_window' in values:
            miner_window = values['miner_window']
            lastest_portfolio = portfolio_history[-1]
            if not (miner_window.start <= lastest_portfolio.timestamp_ms <= miner_window.end):
                raise ValueError(f"timestamp_ms {lastest_portfolio.timestamp_ms} is not between {miner_window.start} and {miner_window.end}")
        return portfolio_history


    def deserialize(self) -> List[PortfolioModel]:
        return self.portfolio_history

    # TODO unsure about this
    async def process_streaming_response(self, response: StreamingResponse) -> AsyncIterator[PortfolioModel]:
        # if self.trade_history is None:
            # self.trade_history = []

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
            "portfolio_history": self.portfolio_history,
        }


if __name__ == "__main__":
    p = Portfolio({"BTCUSDT":1})
    m = PortfolioModel(portfolio=p, timestamp_ms=1600203090)
    # p = p.update_portfolio({"BTCUSDT":2})
    a = p.update_portfolio({"BTCUSDT":'2'})
    print(a)
    x = PortfolioModel(portfolio=a)
    print(x)
    # PortfolioModel(portfolio=p)
