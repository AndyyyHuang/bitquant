from typing import List, Dict, Tuple, AsyncIterator, Union, Any
from functools import reduce
from pydantic import BaseModel, validator, Field, ValidationError, PrivateAttr
import bittensor as bt
from starlette.responses import StreamingResponse

from bitquant.base.pair import TRADABLE_PAIRS, Values
from bitquant.utils.timeutils import TimeUtils

class SymbolValueDict(dict):
    """
    Stores the portfolio of current pair holdings
    SymbolValueDict is meant to be immutable
    The only method available is update_portfolio() which creates a new SymbolValueDict instance

    Initialization:
        - SymbolValueDict(values=[-1, 0.5, 0.1, ...])
            -- values here has to be in the same order as TRADABLE_PAIRS
        - SymbolValueDict(values=tuple([-1, 0.5, 0.1, ...]))
            -- values here has to be in the same order as TRADABLE_PAIRS
        - SymbolValueDict(values={"BTCUSDT": -1, "ETHUSDT": 0.5, ...})
            -- keys here doesn't have to be in order
        - SymbolValueDict({})  # creates a SymbolValueDict with
    """
    pairs = TRADABLE_PAIRS

    def __init__(self, values: Union[Dict[str, Values], List[Values], Tuple[Values, ...]]):
        if isinstance(values, dict):
            if not all((failed_key := k) in self.pairs for k in values.keys()):
                raise KeyError(f"Key={failed_key} not in TRADABLE_PAIRS")
            super().__init__({p:values.get(p, 0) for p in self.pairs})
        elif isinstance(values, (list, tuple)):
            if len(values) != len(self.pairs):
                raise IndexError(f"values length has to be the same as length of TRADABLE_PAIRS")
            super().__init__({p:values[i] for i,p in enumerate(self.pairs)})
        else:
            raise TypeError(f"unexpected type {type(values)}")

    def update_portfolio(self, changes:Union[List[Dict], Dict[str, Union[int, float]]]) -> 'SymbolValueDict':
        if isinstance(changes, dict):
            return SymbolValueDict({**self, **changes})
        elif isinstance(changes, list) and all(isinstance(d, dict) for d in changes):
            return reduce(lambda a,b: a.update(b), [self] + changes)
        else:
            raise TypeError("cannot update portfolio with changes")

    def __setitem__(self, __key: Any, __value: Any) -> None:
        raise AttributeError("SymbolValueDict class is immutable")
    def __delitem__(self, __key: Any) -> None:
        raise AttributeError("SymbolValueDict class is immutable")


class PortfolioRecord(BaseModel):
    portfolio: SymbolValueDict
    timestamp_ms: int = TimeUtils.now_in_ms()

    def to_dict(self) -> Dict[str, Union[SymbolValueDict, int]]:
        return {
            'portfolio': self.portfolio,
            'timestamp_ms': self.timestamp_ms
        }

    @classmethod
    def from_dict(cls, json_data: Dict) -> "PortfolioRecord":
        return cls(
            portfolio=json_data['portfolio'],
            timestamp_ms=json_data['timestamp_ms']
        )

    # HACK figure out why type checking is not happening automatically
    @validator('portfolio', pre=True)
    def check_portfolio_values(cls, value):
        if not all(isinstance(v, (int, float)) for v in value.values()):
            raise ValueError("All values in the portfolio must be integers or floats")
        if any(v == float('inf') or v == float('-inf') or v == float('nan') for v in value.values()):
            raise ValueError("There can't be inf, -inf, or nan in the portfolio")
        return value


class MinerEvaluationWindow(BaseModel):
    start_ms: int
    end_ms: int

    @validator('end_ms')
    def check_start_less_than_end(cls, v, values, **kwargs):
        if 'start' in values and v <= values['start']:
            raise ValueError('end must be greater than start')
        return v


class StreamingPortfolioHistory(bt.StreamingSynapse):

    miner_window: MinerEvaluationWindow = Field(..., allow_mutation=False)
    portfolio_history: List[PortfolioRecord] = Field(default_factory=list, allow_mutation=True)

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


    def deserialize(self) -> List[PortfolioRecord]:
        return self.portfolio_history

    # TODO unsure about this
    async def process_streaming_response(self, response: StreamingResponse) -> AsyncIterator[PortfolioRecord]:
        # NOTE: this should be the same as default factory
        # if self.trade_history is None:
            # self.trade_history = []

        try:
           async for chunk in response.content.iter_any():
                bt.logging.debug(f"Processing chunk: {chunk}")
                # bytes -> string -> dict -> PortfolioRecord
                chunk = chunk.decode("utf-8")
                record = PortfolioRecord.from_dict(json.loads(chunk))

                # add it to history
                self.portfolio_history.append(record)

                # TODO where is this yielded to?
                yield record

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
    import json
    svdict = SymbolValueDict({"BTCUSDT":1})
    PortfolioRecord(svdict=svdict)