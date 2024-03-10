from abc import abstractmethod, ABC
from typing import List, Callable, Dict, Any, Union

import pandas as pd
import numpy as np
import requests
import asyncio
import aiohttp
import traceback
from bitquant.data.utils import TimeUtils


# https://api.mexc.com/api/v3/klines?symbol=FORTHUSDT&interval=60m&limit=3
# https://api.bybit.com/v5/market/kline?symbol=WBTCBTC&interval=60&category=spot&limit=3
# https://api-aws.huobi.pro/market/history/kline?period=4hour&size=3&symbol=btcusdt
# https://api.kucoin.com/api/v1/market/candles?symbol=BTC-USDT&type=4hour
# https://api.bitget.com/api/v2/spot/market/candles?symbol=BTCUSDT&granularity=4h&limit=3
# https://api.lbkex.com/v2/kline.do?size=3&type=hour4&time=1705015573&symbol=bch_usdt
# logic: descending order an discard the first data


class BaseAPIHandler(ABC):
    base_url: str
    limit_per_second: int

    @classmethod
    def get_json(cls, endpoint: str, params:Dict[str, Any] = {}):
        url = cls.base_url + endpoint
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            return None
        except requests.exceptions.RequestException as e:
            print(f"error {e=} for {url=} for {params=} for {cls.__name__}")
            return None
        except Exception as e:
            print(f"error {e=} for {url=} for {params=} for {cls.__name__}")
            return None
        # should maybe also check for code inside response here
        # if (ret := response.json())['status_code']
            # return None

        return response.json()

    @classmethod
    async def async_get_json(cls, session:aiohttp.ClientSession, url, param):
        async with session.get(url, params=param, timeout=10) as response:
            response.raise_for_status()
            data = await response.json()
            return data

    @classmethod
    async def batch_get_json(cls, endpoint:str, params:List[Dict[str, Any]] = []):
        url = cls.base_url + endpoint
        # timeout = aiohttp.ClientTimeout(total=15) # use limit_per_second??
        async with aiohttp.ClientSession() as session:
            # tasks = [asyncio.ensure_future(cls.async_get_json(session=session, url=url, param=param) for param in params)]
            tasks = [cls.async_get_json(session=session, url=url, param=param) for param in params]
            all_responses = await asyncio.gather(*tasks)
        return all_responses

    @classmethod
    @abstractmethod
    def get_exchange_info(cls, filters: list = None):
        pass

    @classmethod
    @abstractmethod
    async def get_klines(cls, symbol, interval, st, et):
        pass


class BinanceExchange(BaseAPIHandler):
    base_url="https://fapi.binance.com"
    limit_per_second=int(20000 / 60 / 5)

    @classmethod
    def get_exchange_info(cls) -> Dict:
        return cls.get_json(endpoint="/fapi/v1/exchangeInfo")

    @classmethod
    def get_symbol_info(cls):
        data = [
            {
                "symbol": each["symbol"],
                "contractType": each["contractType"],
                "status": each["status"],
                "tickSize": each["filters"][0]["tickSize"],
                "notional": each["filters"][5]["notional"]
            } for each in cls.get_exchange_info()["symbols"]
        ]
        return data

    @classmethod
    def get_klines(cls, params:List[Dict[str,Any]]=None, asynchonous_batch=True):
        '''
        Return:
            List[all_symbols_list[kline_data_list]]
            kline_data_list is ascending
        '''
        endpoint = "/fapi/v1/continuousKlines"
        if asynchonous_batch:
            return asyncio.run(cls.batch_get_json(endpoint=endpoint, params=params))
        else:
            return [cls.get_json(endpoint, param) for param in params]

    @classmethod
    def get_klines_by_symbol(cls, symbols: List[str], interval: str, st: str, et: str):
        # build params
        limit = 1500
        st_in_ms = TimeUtils.dt_str_to_ms(st, format="%Y-%m-%d %H:%M:%S")
        et_in_ms = TimeUtils.dt_str_to_ms(et, format="%Y-%m-%d %H:%M:%S")
        interval_in_ms = TimeUtils.interval_str_to_ms(interval)

        # TODO need tests
        # TODO check if it's including or excluding for et_
        # create params for each symbol and for every request interval
        params = [{
            "pair": symbol,
            "contractType": "PERPETUAL",
            "interval": interval,
            "startTime": st_,
            "endTime": et_in_ms,
            "limit": 1000
        } for symbol in symbols for st_ in range(st_in_ms, et_in_ms, interval_in_ms*limit)]

        # create dictionary using pair name and klines
        ret = {p['pair']:k for p,k in zip(params, cls.get_klines(params))}
        return ret


if __name__ == '__main__':
    ...
    # st = TimeUtils.now_in_ms()
    # print(len(BinanceExchange.get_klines_by_symbol(["BTCUSDT", "ETHUSDT"], '1m', str(st-1000000), str(st))[0]))