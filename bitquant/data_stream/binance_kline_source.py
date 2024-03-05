from binance import enums, Client
import time
import pandas as pd
from datetime import datetime
import os
from utils import get_delta_time, my_logger
from warnings import filterwarnings
filterwarnings('ignore')


def retrieve_binanceusdm_data(freq=enums.KLINE_INTERVAL_4HOUR):
    """

    :param freq: [enums.KLINE_INTERVAL_1MINUTE, enums.KLINE_INTERVAL_1HOUR, enums.KLINE_INTERVAL_4HOUR, enums.KLINE_INTERVAL_12HOUR]:
    :type freq:
    :return:
    :rtype:
    """
    client = Client()
    et = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    bucket = "kline"
    exchange_name = "binanceusdm"

    info = client.futures_exchange_info()
    contract = [pair['pair'] for pair in info['symbols'] if
                pair['contractType'] == 'PERPETUAL' and pair['status'] == 'TRADING' and pair['pair'].endswith("USDT")]

    logger_dt = datetime.utcnow().strftime("%Y-%m-%d-%H-%M")
    mylogger = my_logger(log_type="logger",filename=f"./logs/{exchange_name}_{bucket}_{freq}_{logger_dt}")
    path = f"./{exchange_name}_kline_{freq}"
    if not os.path.exists(path):
        os.makedirs(path)
    for symbol in contract:
        time.sleep(1)
        try:
            filepath = f"{path}/binance_swap_{symbol}_kline_{freq}.parquet"
            if os.path.exists(filepath):
                old_df = pd.read_parquet(filepath)
                try:
                    delta = get_delta_time(freq)
                    breakpoint = old_df.loc[list(pd.Series(old_df.index).diff().fillna(delta) != delta)].index[0]
                    st = datetime.utcfromtimestamp(
                        old_df.index[old_df.index.tolist().index(breakpoint) - 2].timestamp())
                    st = st.strftime("%Y-%m-%d %H:%M:%S")
                    mylogger.info(f"Exits breakpoint {breakpoint}")
                except:
                    st = datetime.utcfromtimestamp(old_df.index[-3].timestamp())
                    st = st.strftime("%Y-%m-%d %H:%M:%S")
                res = client.get_historical_klines(symbol, freq, klines_type=enums.HistoricalKlinesType.FUTURES,
                                                start_str=st, end_str=et)

                kl_df = pd.DataFrame(res, columns=["ots", "open", "high", "low", "close", "volume", "ts", "usd_v",
                                                   "n_trades", "taker_buy_v", "taker_buy_usd", "ig"]).drop(['ots', 'ig'], axis=1)
                kl_df.index = pd.to_datetime(kl_df['ts'], unit='ms', utc=True)
                kl_df = kl_df.drop('ts', axis=1)
                new_st = kl_df.index[0]
                old_df = old_df.loc[(old_df.index < new_st)]
                new_df = pd.concat([old_df, kl_df], axis=0)
                new_df.loc[~new_df.index.duplicated(keep='last')].to_parquet(
                    f"{path}/{exchange_name}_{symbol}_kline_{freq}.parquet")
                mylogger.info(f"symbol :{symbol}, {freq} update complete！")
            else:
                st = "1 Jan,2022"
                res = client.get_historical_klines(symbol, freq, klines_type=enums.HistoricalKlinesType.FUTURES,
                                                start_str=st, end_str=et)
                kl_df = pd.DataFrame(res, columns=["ots", "open", "high", "low", "close", "volume", "ts", "usd_v",
                                                   "n_trades", "taker_buy_v", "taker_buy_usd", "ig"]).drop(['ots', 'ig'], axis=1)
                kl_df.index = pd.to_datetime(kl_df['ts'], unit='ms', utc=True)
                kl_df = kl_df.drop('ts', axis=1)
                kl_df.to_parquet(f"{path}/{exchange_name}_{symbol}_kline_{freq}.parquet")
                mylogger.info(f"symbol :{symbol}, {freq} download complete！")
        except Exception as e:
            mylogger.error(f"symbol: :{symbol} update error:{e}")



if __name__ == "__main__":
    retrieve_binanceusdm_data(enums.KLINE_INTERVAL_4HOUR)