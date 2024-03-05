import pandas as pd
import numpy as np
from datetime import datetime
from utils import my_logger
from glob import glob
from warnings import filterwarnings
filterwarnings('ignore')


def aggregate_multi_symbols_kline(exchange_name, freq):
    """

    :param exchange_name:
    :type exchange_name:
    :param freq:
    :type freq:
    :return:
    :rtype:
    """
    logger_dt = datetime.utcnow().strftime("%Y-%m-%d-%H-%M")
    mylogger = my_logger(log_type="logger",
                         filename=f"./logs/{exchange_name}_kline_{freq}_aggregation_{logger_dt}")
    btc = pd.read_parquet(f"./{exchange_name}_kline_{freq}/{exchange_name}_BTCUSDT_kline_{freq}.parquet")
    btc.index = pd.Series(btc.index).apply(lambda x: x.round('1s'))
    st = btc.index[0]
    et = btc.index[-1]
    if freq.endswith('m'):
        resample_freq = freq + 'in'
    else:
        resample_freq = freq
    ts_lis = pd.date_range(st, et, freq=resample_freq).tolist()
    col = btc.columns.tolist() + ['vwap']
    symbol_lis = list(map(lambda x: x.split("/")[-1].split(f"{exchange_name}_")[-1].split(f"_kline_{freq}.parquet")[0],
                          glob(f"./{exchange_name}_kline_{freq}/{exchange_name}_*USDT_kline_{freq}.parquet")))
    symbol_lis = list(set(symbol_lis).difference(set(['USDC'])))
    multi_idx = pd.MultiIndex.from_product([ts_lis, symbol_lis], names=["ts", "symbol"])
    data = np.full(shape=(len(ts_lis), len(symbol_lis), len(col)), fill_value=np.nan)

    wrong_symbol = []
    for i in range(len(symbol_lis)):
        symbol = symbol_lis[i]
        try:
            tmp = pd.read_parquet(f"./{exchange_name}_kline_{freq}/{exchange_name}_{symbol}_kline_{freq}.parquet")
            tmp.index = pd.Series(tmp.index).apply(lambda x: x.round('1s'))
            tmp = tmp.loc[(tmp.index >= st) & (tmp.index <= et)]
            tmp = tmp.apply(pd.to_numeric)
            tmp['vwap'] = (tmp['usd_v'] / tmp['volume']).ffill().bfill()  # volume可能为0而导致nan，填充即可
            st_idx = ts_lis.index(tmp.index[0])
            ed_idx = ts_lis.index(tmp.index[-1])
            data[st_idx:ed_idx + 1, i, :] = tmp.values

        except Exception as e:
            wrong_symbol.append(symbol)
            mylogger.error(f"{symbol} error: {e}")
    df = pd.DataFrame(data.reshape(-1, data.shape[-1]), index=multi_idx, columns=col)
    symbol_lis = list(set(symbol_lis).difference(set(wrong_symbol)))
    df = df.loc[(slice(None), symbol_lis), :]
    df.to_parquet(f"./local_data/{exchange_name}_{freq}_aggregated_kline.parquet")

if __name__ == "__main__":
    aggregate_multi_symbols_kline(exchange_name="binanceusdm", freq="4h")