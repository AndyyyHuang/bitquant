from abc import abstractmethod

import pandas as pd
import numpy as np
import requests
import asyncio
import aiohttp
import traceback
from bitquant.data.utils import TimeManager


# https://api.mexc.com/api/v3/klines?symbol=FORTHUSDT&interval=60m&limit=3
# https://api.bybit.com/v5/market/kline?symbol=WBTCBTC&interval=60&category=spot&limit=3
# https://api-aws.huobi.pro/market/history/kline?period=4hour&size=3&symbol=btcusdt
# https://api.kucoin.com/api/v1/market/candles?symbol=BTC-USDT&type=4hour
# https://api.bitget.com/api/v2/spot/market/candles?symbol=BTCUSDT&granularity=4h&limit=3
# https://api.lbkex.com/v2/kline.do?size=3&type=hour4&time=1705015573&symbol=bch_usdt
# logic: descending order an discard the first data


class BaseExchange:
    def __init__(self, name, base_url, info_endpoint, klines_endpoint, limit_per_second):
        self.base_url = base_url
        self.name = name
        self.info_endpoint = info_endpoint
        self.klines_endpoint = klines_endpoint
        self.max_request_per_second = limit_per_second

    @abstractmethod
    def get_exchange_info(self, filters: list = None):
        pass

    @abstractmethod
    async def fetch_klines_by_symbol(self, symbol, interval, st, et):
        pass


class BinanceExchange(BaseExchange):
    def __init__(self):
        super().__init__(
            name="BINANCE",
            base_url="https://fapi.binance.com",
            info_endpoint="/fapi/v1/exchangeInfo",
            klines_endpoint="/fapi/v1/continuousKlines",  # ascending
            limit_per_second=int(20000 / 60 / 5),
        )
        self.info_url = self.base_url + self.info_endpoint
        self.klines_url = self.base_url + self.klines_endpoint


    def get_exchange_info(self, filters: list = None):
        response = requests.get(self.info_url)
        data = response.json()
        """
        data = [
            {
                "symbol": each["symbol"],
                "contractType": each["contractType"],
                "status": each["status"],
                "tickSize": each["filters"][0]["tickSize"],
                "notional": each["filters"][5]["notional"]
            } for each in response.json()["symbols"]
        ]
        """
        return data

    async def fetch_klines_by_symbol(self, symbol, interval, st, et):

        st_in_ms = TimeManager.dt_str_to_ms(st, format="%Y-%m-%d %H:%M:%S")
        et_in_ms = TimeManager.dt_str_to_ms(et, format="%Y-%m-%d %H:%M:%S")
        interval_in_ms = TimeManager.interval_str_to_ms(interval)
        data_lis = []
        async with aiohttp.ClientSession() as session:
            while True:
                params = {
                    "pair": symbol,
                    "contractType": "PERPETUAL",
                    "interval": interval,
                    "startTime": st_in_ms,
                    "endTime": et_in_ms,
                    "limit": 1000,
                }

                try:
                    async with session.get(self.klines_url, params=params) as response:
                        data = await response.json()
                        # Check if the response is rate limited
                        if response.status != 200:
                            wait_time = 2
                            print(
                                f"Error Status: {response.status}, Error Msg: {data['msg']}."
                            )
                            await asyncio.sleep(wait_time)

                        if len(data) > 0:
                            st_in_ms = data[-1][0] + interval_in_ms
                            data_lis.extend(data)
                        else:
                            break

                        if st_in_ms >= et_in_ms:
                            break

                except Exception as e:
                    print(f"Error processing binance data {e}")
                    traceback.print_exc()

        return data_lis

    async def get_klines(self, symbol_lis: list, interval: str, st: str, et: str) -> dict:
        tasks = [
            self.fetch_klines_by_symbol(symbol=symbol, interval=interval, st=st, et=et)
            for symbol in symbol_lis
        ]
        results = await asyncio.gather(*tasks)

        klines = {}
        for i, res in enumerate(results):
            kl_df = pd.DataFrame(res, columns=["ots", "open", "high", "low", "close", "volume", "ts",
                                                     "usd_v", "n_trades", "taker_buy_v", "taker_buy_usd", "ig"]).drop(["ots", "ig"], axis=1)
            kl_df['ts'] = kl_df['ts'] + 1
            kl_df['ts'] = kl_df['ts'].apply(TimeManager.ms_to_timestamp)
            kl_df.set_index("ts", inplace=True)
            klines[symbol_lis[i]] = kl_df

        return klines

    def get_aggregated_symbols_kline(self, symbol_lis: list, interval: str, st: str, et: str) -> pd.DataFrame:

        klines = asyncio.run(self.get_klines(symbol_lis, interval, st, et))
        btc = klines["BTCUSDT"]
        st = btc.index[0]
        et = btc.index[-1]

        ts_lis = btc.index.tolist()
        symbol_lis = list(klines.keys())
        col = btc.columns.tolist() + ['vwap']

        multi_idx = pd.MultiIndex.from_product([ts_lis, symbol_lis], names=["ts", "symbol"])
        data = np.full(shape=(len(ts_lis), len(symbol_lis), len(col)), fill_value=np.nan)

        wrong_symbol = []
        for symbol, kline in klines.items():
            indice = symbol_lis.index(symbol)
            try:
                kline = kline.loc[(kline.index >= st) & (kline.index <= et)]
                kline = kline.apply(pd.to_numeric)
                kline['vwap'] = (kline['usd_v'] / kline['volume']).ffill().bfill()  # volume可能为0而导致nan，填充即可
                st_idx = ts_lis.index(kline.index[0])
                ed_idx = ts_lis.index(kline.index[-1])
                data[st_idx:ed_idx + 1, indice, :] = kline.values
            except:
                wrong_symbol.append(symbol)

        df = pd.DataFrame(data.reshape(-1, data.shape[-1]), index=multi_idx, columns=col)
        symbol_lis = list(set(symbol_lis).difference(set(wrong_symbol)))
        df = df.loc[(slice(None), symbol_lis), :]
        return df

    def get_symbol_info(self) -> pd.DataFrame:

        info = self.get_exchange_info()
        data = [
            {
                "symbol": each["symbol"],
                "contractType": each["contractType"],
                "status": each["status"],
                "tickSize": each["filters"][0]["tickSize"],
                "notional": each["filters"][5]["notional"]
            } for each in info["symbols"]
        ]
        return pd.DataFrame(data)

