import pandas as pd
from datetime import datetime, timedelta
import logging
from loguru import logger as logu

def preprocess_data(bar, st="2023-06-01", et="2025-01-01"):
    bar = bar.loc[(bar.index.get_level_values(0) > pd.to_datetime(st, utc=True)) & (bar.index.get_level_values(0) <= pd.to_datetime(et, utc=True))]
    bar['return_1'] = (bar['close'].unstack().diff(1).shift(-1) / bar['close'].unstack()).stack()
    bar = bar.unstack().iloc[:-1].stack()
    symbol_lis = bar.return_1.unstack().isna().sum(axis=0)[bar.return_1.unstack().isna().sum(axis=0) == 0].index.tolist()
    missing_symbol = list(set(bar.index.get_level_values(1).unique()).difference(set(symbol_lis)))
    if len(missing_symbol) == 0:
        pass
    else:
        print('There are uncompleted data in these symbols: ', list(set(bar.index.get_level_values(1).unique()).difference(set(symbol_lis))))

    bar = bar.loc[(slice(None), symbol_lis), :]
    return bar


def get_delta_time(s: str) -> datetime:
    # Extract the number and the time unit from the string
    num = int(s[:-1])
    unit = s[-1]

    # Convert the string into a timedelta object
    if unit == 'h':  # hours
        delta = timedelta(hours=num)
    elif unit == 'm':  # minutes
        delta = timedelta(minutes=num)
    elif unit == 's':  # seconds
        delta = timedelta(seconds=num)
    elif unit == 'd':  # days
        delta = timedelta(days=num)
    else:
        raise ValueError(f"Unknown time unit: {unit}")

    # Add the timedelta to the current timestamp
    return delta


def my_logger(log_type,filename=None):
    if log_type=="console":
        logger = logging.getLogger('influxdb_save')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        # 创建一个handler，用于输出控制台，并且设定严重级别
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)
        logger.addHandler(ch)
    else:
        logu.add(filename, retention='30 days')
        logger=logu
    return logger