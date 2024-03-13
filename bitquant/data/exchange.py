from abc import abstractmethod, ABC
from typing import List, Callable, Dict, Any, Union

import pandas as pd
import numpy as np
import requests
import asyncio
import aiohttp
import traceback
from bitquant.utils.timeutils import TimeUtils


# https://api.mexc.com/api/v3/klines?symbol=FORTHUSDT&interval=60m&limit=3
# https://api.bybit.com/v5/market/kline?symbol=WBTCBTC&interval=60&category=spot&limit=3
# https://api-aws.huobi.pro/market/history/kline?period=4hour&size=3&symbol=btcusdt
# https://api.kucoin.com/api/v1/market/candles?symbol=BTC-USDT&type=4hour
# https://api.bitget.com/api/v2/spot/market/candles?symbol=BTCUSDT&granularity=4h&limit=3
# https://api.lbkex.com/v2/kline.do?size=3&type=hour4&time=1705015573&symbol=bch_usdt
# logic: descending order an discard the first data

ResponseType = Union[str, float, int]

class BaseAPIHandler(ABC):
    base_url: str
    limit_per_second: int

    @classmethod
    def get_json(cls, endpoint: str, params: Dict[str, ResponseType] = {}):
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
    async def async_get_json(cls, session: aiohttp.ClientSession, url, param: Dict[str, ResponseType]):
        async with session.get(url, params=param, timeout=10) as response:
            response.raise_for_status()
            data = await response.json()
            return data

    @classmethod
    async def batch_get_json(cls, endpoint: str, params: List[Dict[str, ResponseType]] = []):
        url = cls.base_url + endpoint
        # timeout = aiohttp.ClientTimeout(total=15) # use limit_per_second??
        async with aiohttp.ClientSession() as session:
            # tasks = [asyncio.ensure_future(cls.async_get_json(session=session, url=url, param=param) for param in params)]
            tasks = [cls.async_get_json(session=session, url=url, param=param) for param in params]
            all_responses = await asyncio.gather(*tasks)
            # concatenate responses
            all_responses = [element for innerList in all_responses for element in innerList]
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
        def parse_volume_precision(stepSize):
            if '.' in stepSize and stepSize.split('.')[0] == '0':
                volume_precision = len(stepSize.split('.')[1])
            elif '.' in stepSize and stepSize.split('.')[0] != '0':
                volume_precision = 0
            else:
                volume_precision = 0
            return volume_precision

        data = [
            {
                "symbol": each["symbol"],
                "contractType": each["contractType"],
                "status": each["status"],
                "volume_precision": parse_volume_precision(each["filters"][1]["stepSize"]),
                "notional": float(each["filters"][5]["notional"])
            } for each in cls.get_exchange_info()["symbols"]
        ]

        symbol_info = pd.DataFrame(data)
        symbol_info = symbol_info.loc[(symbol_info["contractType"] == "PERPETUAL") & (symbol_info["status"] == "TRADING")]
        return symbol_info

    @classmethod
    def get_klines(cls, params: List[Dict[str, ResponseType]]=None, asynchonous_batch=True):
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
        limit = 1000
        st_in_ms = TimeUtils.dt_str_to_ms(st, format="%Y-%m-%d %H:%M:%S")
        et_in_ms = TimeUtils.dt_str_to_ms(et, format="%Y-%m-%d %H:%M:%S")
        interval_in_ms = TimeUtils.interval_str_to_ms(interval)

        # TODO need tests
        # TODO check if it's including or excluding for et_
        # create params for each symbol and for every request interval
        # Response will include et
        ret = {}
        for symbol in symbols:
            params = [{
                "pair": symbol,
                "contractType": "PERPETUAL",
                "interval": interval,
                "startTime": st_,
                "endTime": et_in_ms,
                "limit": limit
            } for st_ in range(st_in_ms, et_in_ms, interval_in_ms*limit)]

            # create dictionary using pair name and klines
            ret[symbol] = cls.get_klines(params)
        return ret

    @classmethod
    def get_aggregated_symbols_kline(cls, symbols: List[str], interval: str, st: str, et: str) -> pd.DataFrame:
        klines = cls.get_klines_by_symbol(symbols, interval, st, et)
        def parse(kline: List[List[Any]]):
            kl_df = pd.DataFrame(kline, columns=["ots", "open", "high", "low", "close", "volume", "ts",
                                            "usd_v", "n_trades", "taker_buy_v", "taker_buy_usd", "ig"]).drop(["ots", "ig"], axis=1)
            kl_df['ts'] = kl_df['ts'] + 1
            kl_df['ts'] = kl_df['ts'].apply(TimeUtils.ms_to_timestamp)
            kl_df.set_index("ts", inplace=True)
            return kl_df
        # turn list in dict into df
        klines = {pair: parse(kline) for pair, kline in klines.items()}

        btc = klines["BTCUSDT"]
        st = btc.index[0]
        et = btc.index[-1]

        ts_lis = btc.index.tolist()
        col = btc.columns.tolist() + ['vwap']

        # symbols = list(klines.keys())
        multi_idx = pd.MultiIndex.from_product([ts_lis, symbols], names=["ts", "symbol"])
        data = np.full(shape=(len(ts_lis), len(symbols), len(col)), fill_value=np.nan)

        # wrong_symbol = []
        for symbol, kline in klines.items():
            indice = symbols.index(symbol)
            try:
                kline = kline.loc[(kline.index >= st) & (kline.index <= et)]
                kline = kline.apply(pd.to_numeric)
                kline['vwap'] = (kline['usd_v'] / kline['volume']).ffill().bfill()  # volume可能为0而导致nan，填充即可
                st_idx = ts_lis.index(kline.index[0])
                ed_idx = ts_lis.index(kline.index[-1])
                data[st_idx:ed_idx + 1, indice, :] = kline.values
            except:
                # wrong_symbol.append(symbol)
                traceback.print_exc()

        aggregated_klines = pd.DataFrame(data.reshape(-1, data.shape[-1]), index=multi_idx, columns=col)
        # symbols = list(set(symbols).difference(set(wrong_symbol)))
        # df = df.loc[(slice(None), symbols), :]
        aggregated_klines['return_1'] = (aggregated_klines['close'].unstack().diff(1).shift(-1) / aggregated_klines[
            'close'].unstack()).stack()
        # symbols_with_complete_data = aggregated_klines["close"].unstack().isna().sum(axis=0)[
        #    aggregated_klines["close"].unstack().isna().sum(axis=0) == 0].index.tolist()
        # aggregated_klines = aggregated_klines.loc[(slice(None), symbols_with_complete_data), :]


        return aggregated_klines

if __name__ == '__main__':
    symbols = ["BTCUSDT", "ETHUSDT"]
    st = "2023-01-01 00:00:00"
    et = "2024-01-01 00:00:00"
    interval = "1h"
    aggregated_kline = BinanceExchange.get_aggregated_symbols_kline(symbols, interval, st, et)

    print((pd.Series(aggregated_kline.index.get_level_values(0).unique()).diff().dropna() != TimeUtils.str_to_timedelta(interval)).sum())
    ...
    # st = TimeUtils.now_in_ms()
    # print(len(BinanceExchange.get_klines_by_symbol(["BTCUSDT", "ETHUSDT"], '1m', str(st-1000000), str(st))[0]))