import pandas as pd
import numpy as np
import asyncio
from utils import TimeManager
from exchange import BinanceExchange

async def get_binance_kline(symbol_lis: list, st: str, et: str) -> pd.DataFrame:
    binance = BinanceExchange()
    tasks = [
        binance.fetch_klines_by_symbol(symbol=symbol, st=st, et=et)
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

def aggregate_multi_symbols_kline(klines: dict) -> pd.DataFrame:

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

def get_binance_symbol_info() -> pd.DataFrame:
    binance = BinanceExchange()
    info = binance.get_exchange_info()
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

if __name__ == "__main__":

    info = get_binance_symbol_info()
    klines = asyncio.run(get_binance_kline(["BTCUSDT", "ETHUSDT"], st="2023-01-01 00:00:00", et="2024-01-01 00:00:00"))
    aggregated_kline = aggregate_multi_symbols_kline(klines)

    print(aggregated_kline, info)