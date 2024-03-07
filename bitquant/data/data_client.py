from abc import ABC, abstractmethod
import requests
import asyncio
import aiohttp
import time
import traceback
import pandas as pd

MAX_RETRIES = 5

# https://api.mexc.com/api/v3/klines?symbol=FORTHUSDT&interval=60m&limit=3
# https://api.bybit.com/v5/market/kline?symbol=WBTCBTC&interval=60&category=spot&limit=3
# https://api-aws.huobi.pro/market/history/kline?period=4hour&size=3&symbol=btcusdt
# https://api.kucoin.com/api/v1/market/candles?symbol=BTC-USDT&type=4hour
# https://api.bitget.com/api/v2/spot/market/candles?symbol=BTCUSDT&granularity=4h&limit=3
# https://api.lbkex.com/v2/kline.do?size=3&type=hour4&time=1705015573&symbol=bch_usdt
# logic: descending order an discard the first data


class BaseExchange:
    def __init__(self, name, base_url, symbols_endpoint, klines_endpoint, interval, limit_per_second):
        self.base_url = base_url
        self.name = name
        self.symbols_endpoint = symbols_endpoint
        self.klines_endpoint = klines_endpoint
        self.interval = interval
        self.max_request_per_second = limit_per_second

    @abstractmethod
    def get_all_symbols(self, filters: list = None):
        pass

    @abstractmethod
    async def fetch_klines_by_symbol(self, symbol, session):
        pass


class BinanceExchange(BaseExchange):
    def __init__(self):
        super().__init__(
            name="BINANCE",
            base_url="https://fapi.binance.com",
            symbols_endpoint="/fapi/v1/exchangeInfo",
            klines_endpoint="/fapi/v1/continuousKlines",  # ascending
            interval="60m",
            limit_per_second=int(20000 / 60 / 5),
        )
        self.symbols_url = self.base_url + self.symbols_endpoint
        self.klines_url = self.base_url + self.klines_endpoint

    def get_all_symbols(self, filters: list = None):
        response = requests.get(self.symbols_url)
        data = response.json()
        if not filters:
            res = [i["symbol"] for i in data["symbols"] if i["isSpotTradingAllowed"]]
        else:
            res = [
                i["symbol"]
                for i in data["symbols"]
                if i["isSpotTradingAllowed"] and i["baseAsset"] in filters
            ]
        return res

    async def fetch_klines_by_symbol(self, symbol, session):
        params = {
            "symbol": symbol,
            "interval": self.interval,
            "limit": 9,
        }
        for attempt in range(MAX_RETRIES):
            try:
                async with session.get(self.klines_url, params=params) as response:
                    # Check if the response is rate limited
                    if response.status == 429:
                        wait_time = 2**attempt
                        print(
                            f"MEXC Rate limited. Retrying {symbol} after {wait_time} seconds."
                        )
                        await asyncio.sleep(wait_time)
                        continue

                    data = await response.json()
                    # print("mexc first", data[0][0], "last", data[-1][0])
                    converted_data = []
                    for sublist in data:
                        converted_sublist = [int(sublist[0]) // 1000] + [
                            float(sublist[i]) for i in [1, 2, 3, 4, 7]
                        ]
                        converted_data.append(converted_sublist)

                    if converted_data == []:
                        print("empty mexc data", self.klines_url, params)
                    return converted_data

            except Exception as e:
                print("error processing mexc data")
                print(traceback.format_exc())
                continue
        return None

class MexcExchange(BaseExchange):
    def __init__(self):
        super().__init__(
            name="MEXC",
            symbols_url="https://api.mexc.com/api/v3/exchangeInfo",
            klines_url="https://api.mexc.com/api/v3/klines",  # ascending
            interval="60m",
            limit_per_second=int(20000 / 60 / 5),
        )

    def get_all_symbols(self, filters: list = None):
        response = requests.get(self.symbols_url)
        data = response.json()
        if not filters:
            res = [i["symbol"] for i in data["symbols"] if i["isSpotTradingAllowed"]]
        else:
            res = [
                i["symbol"]
                for i in data["symbols"]
                if i["isSpotTradingAllowed"] and i["baseAsset"] in filters
            ]
        return res

    async def fetch_klines_by_symbol(self, symbol, session):
        params = {
            "symbol": symbol,
            "interval": self.interval,
            "limit": 9,
        }
        for attempt in range(MAX_RETRIES):
            try:
                async with session.get(self.klines_url, params=params) as response:
                    # Check if the response is rate limited
                    if response.status == 429:
                        wait_time = 2**attempt
                        print(
                            f"MEXC Rate limited. Retrying {symbol} after {wait_time} seconds."
                        )
                        await asyncio.sleep(wait_time)
                        continue

                    data = await response.json()
                    # print("mexc first", data[0][0], "last", data[-1][0])
                    converted_data = []
                    for sublist in data:
                        converted_sublist = [int(sublist[0]) // 1000] + [
                            float(sublist[i]) for i in [1, 2, 3, 4, 7]
                        ]
                        converted_data.append(converted_sublist)

                    if converted_data == []:
                        print("empty mexc data", self.klines_url, params)
                    return converted_data

            except Exception as e:
                print("error processing mexc data")
                print(traceback.format_exc())
                continue
        return None


class BybitExchange(BaseExchange):
    def __init__(self):
        super().__init__(
            name="BYBIT",
            symbols_url="https://api.bybit.com/v5/market/instruments-info?category=spot",
            klines_url="https://api.bybit.com/v5/market/kline",  # descending
            interval="60",
            limit_per_second=int(120 / 5) - 8,
        )
        self.wait_time = 60 * 30

    def get_all_symbols(self, filters: list = None):
        response = requests.get(self.symbols_url)
        data = response.json()
        if filters:
            res = [
                i["symbol"] for i in data["result"]["list"] if i["baseCoin"] in filters
            ]
        else:
            res = [i["symbol"] for i in data["result"]["list"]]

        return res

    async def fetch_klines_by_symbol(self, symbol, session):
        params = {
            "symbol": symbol,
            "interval": self.interval,
            "limit": 9,
            "category": "spot",
        }
        for attempt in range(MAX_RETRIES):
            try:
                async with session.get(self.klines_url, params=params) as response:
                    if response.status == 403:
                        wait_time = 2**attempt
                        print(f"Bybit Rate limited. sleep 30m")
                        await asyncio.sleep(self.wait_time)
                        continue
                    res = await response.json()
                    if "Too many visits" in str(res):
                        print(f"Bybit Rate limited. sleep 5sec")
                        await asyncio.sleep(5)
                        continue
                    data = res["result"]["list"]
                    # print("bybit first", data[0][0], "last", data[-1][0])
                    converted_data = []
                    for sublist in data:
                        converted_sublist = [int(sublist[0]) // 1000] + [
                            float(sublist[i]) for i in [1, 2, 3, 4, -1]
                        ]
                        converted_data.append(converted_sublist)
                    return converted_data
            except Exception as e:
                print("error processing bybit data")
                print(traceback.format_exc())
                break
        return None


class HtxExchange(BaseExchange):
    def __init__(self):
        super().__init__(
            name="HTX",
            symbols_url="https://api-aws.huobi.pro/v2/settings/common/symbols",
            klines_url="https://api-aws.huobi.pro/market/history/kline",  # descending o c l h
            interval="60min",
            limit_per_second=int(100 / 10) - 3,
        )

    def get_all_symbols(self, filters: list = None):
        response = requests.get(self.symbols_url)
        data = response.json()
        res = []
        if filters:
            res = [
                i["sc"]
                for i in data["data"]
                if i["state"] == "online" and i["bcdn"] in filters
            ]
        else:
            res = [i["sc"] for i in data["data"] if i["state"] == "online"]

        return res

    async def fetch_klines_by_symbol(self, symbol, session):
        params = {
            "symbol": symbol,
            "period": self.interval,
            "size": 9,
        }
        for attempt in range(MAX_RETRIES):
            try:
                async with session.get(self.klines_url, params=params) as response:
                    res = await response.json()
                    # if "Too many visits" in str(res):
                    #     print(f"Bybit Rate limited. sleep 5sec")
                    #     await asyncio.sleep(5)
                    #     continue
                    required_keys = ["id", "open", "close", "low", "high", "vol"]
                    data = [
                        [item[key] for key in required_keys if key in item]
                        for item in res["data"]
                        if all(key in item for key in required_keys)
                    ]
                    if not data:
                        continue

                    return data
            except Exception as e:
                print("error processing htx data")
                print(traceback.format_exc())
                break
        return None


class KucoinExchange(BaseExchange):
    def __init__(self):
        super().__init__(
            name="KUCOIN",
            symbols_url="https://api.kucoin.com/api/v2/symbols",
            klines_url="https://api.kucoin.com/api/v1/market/candles",  # descending ochl
            interval="1hour",
            limit_per_second=int(2000 / 30) - 10,
        )

    def get_all_symbols(self, filters: list = None):
        response = requests.get(self.symbols_url)
        data = response.json()
        if filters:
            res = [i["symbol"] for i in data["data"] if i["baseCurrency"] in filters]
        else:

            res = [i["symbol"] for i in data["data"]]

        return res

    async def fetch_klines_by_symbol(self, symbol, session):
        params = {
            "symbol": symbol,
            "type": self.interval,
        }
        for attempt in range(MAX_RETRIES):
            try:
                async with session.get(self.klines_url, params=params) as response:
                    res = await response.json()
                    if "too many request" in str(res):
                        print(f"Kucoin Rate limited. sleep 5sec")
                        await asyncio.sleep(5)
                        continue
                    data = res["data"][:10]
                    # print(data)
                    # print("Kucoin first", data[0][0], "last", data[-1][0])
                    converted_data = []
                    for sublist in data:
                        converted_sublist = [int(sublist[0])] + [
                            float(item) for item in sublist[1:]
                        ]
                        converted_data.append(converted_sublist)
                    return converted_data
            except Exception as e:
                print("error processing kucoin data")
                print(traceback.format_exc())
                break
        return None


class IbankExchange(BaseExchange):
    def __init__(self):
        super().__init__(
            name="IBANK",
            symbols_url="https://api.lbkex.com/v2/accuracy.do",
            klines_url="https://api.lbkex.com/v2/kline.do",  # acending ochl
            interval="hour1",
            limit_per_second=200 / 10 - 5,
        )

    def get_all_symbols(self, filters: list = None):
        response = requests.get(self.symbols_url)
        data = response.json()
        if filters:
            res = [
                i["symbol"]
                for i in data["data"]
                if i["symbol"].split("_")[0].upper() in filters
            ]
        else:
            res = [i["symbol"] for i in data["data"]]
        return res

    async def fetch_klines_by_symbol(self, symbol, session):
        params = {
            "symbol": symbol,
            "size": 15,
            "type": self.interval,
            "time": int(time.time() - 3600 * 15),
        }
        for attempt in range(MAX_RETRIES):
            try:
                async with session.get(self.klines_url, params=params) as response:
                    res = await response.json()
                    data = res["data"]
                    # if "Too many visits" in str(res):
                    #     print(f"Bybit Rate limited. sleep 5sec")
                    #     await asyncio.sleep(5)
                    #     continue
                    # print("IBANK first", data[0][0], "last", data[-1][0])
                    return data
            except Exception as e:
                print("error processing ibank data")
                print(traceback.format_exc())
                break


class BitgetExchange(BaseExchange):
    def __init__(self):
        super().__init__(
            name="BITGET",
            symbols_url="https://api.bitget.com/api/v2/spot/public/symbols",
            klines_url="https://api.bitget.com/api/v2/spot/market/candles",  # ascending o h l c
            interval="1h",
            limit_per_second=20 - 5,
        )

    def get_all_symbols(self, filters: list = None):
        response = requests.get(self.symbols_url)
        data = response.json()
        if filters:
            res = [i["symbol"] for i in data["data"] if i["baseCoin"] in filters]
        else:
            res = [i["symbol"] for i in data["data"]]
        return res

    async def fetch_klines_by_symbol(self, symbol, session):
        params = {
            "symbol": symbol,
            "granularity": self.interval,
            "limit": 10,
        }
        for attempt in range(MAX_RETRIES):
            try:
                async with session.get(self.klines_url, params=params) as response:
                    res = await response.json()
                    if "API exceeds the maximum limit added" in str(res):
                        print(f"Bitget Rate limited. sleep 5sec")
                        await asyncio.sleep(5)
                        continue
                    data = res["data"]
                    # print("BITGET first", data[0][0], "last", data[-1][0])
                    converted_data = []
                    for sublist in data:
                        converted_sublist = [int(sublist[0]) // 1000] + [
                            float(sublist[i]) for i in [1, 2, 3, 4, 7]
                        ]
                        converted_data.append(converted_sublist)
                    return converted_data
            except Exception as e:
                print(traceback.format_exc())
                break


class OkexExchange(BaseExchange):
    def __init__(self):
        super().__init__(
            name="OKEX",
            symbols_url="https://aws.okx.com/api/v5/public/instruments?instType=SPOT",
            klines_url="https://aws.okx.com/api/v5/market/candles",
            interval="1H",
            limit_per_second=40 / 2 - 4,
        )

    def get_all_symbols(self, filters: list = None):
        response = requests.get(self.symbols_url)
        data = response.json()
        if filters:
            res = [i["instId"] for i in data["data"] if i["baseCcy"] in filters]
        else:
            res = [i["instId"] for i in data["data"]]
        return res

    async def fetch_klines_by_symbol(self, symbol, session):
        params = {
            "instId": symbol,
            "bar": self.interval,
            "limit": 10,
        }
        for attempt in range(MAX_RETRIES):
            try:
                async with session.get(self.klines_url, params=params) as response:
                    res = await response.json()
                    if "Requests too frequent" in str(res):
                        print(f"Okex Rate limited. sleep 5sec")
                        await asyncio.sleep(5)
                        continue
                    data = res["data"]
                    # print("BITGET first", data[0][0], "last", data[-1][0])
                    converted_data = []
                    for sublist in data:
                        converted_sublist = [int(sublist[0]) // 1000] + [
                            float(sublist[i]) for i in [1, 2, 3, 4, 5]
                        ]
                        converted_data.append(converted_sublist)
                    return converted_data
            except Exception as e:
                print(traceback.format_exc())
                break
