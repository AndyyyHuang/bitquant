from typing import List, Any, Dict
import pandas as pd
import numpy as np
from bitquant.data.exchange import BinanceExchange, ResponseType
from bitquant.data.utils import TimeUtils


# TODO: maybe support multiple exchanges
# TODO: maybe support priority get in case an exchange goes offline
class DataClient:
    def __init__(self, exchange: BinanceExchange):
        self.exchange = exchange

    def get_symbol_info(self) -> List[Dict[str, ResponseType]]:
        symbol_info = self.exchange.get_symbol_info()
        return symbol_info

    def get_aggregated_symbols_kline(self, symbols: List[str], interval, st, et):
        klines = self.exchange.get_klines_by_symbol(symbols, interval, st, et)
        klines = self.klines_to_df(klines)
        klines = self.process_aggregated_symbols_kline(klines)
        return klines


    @staticmethod
    def klines_to_df(klines:Dict[str, List[List[ResponseType]]]) -> pd.DataFrame:
        def parse(kline: List[List[Any]]):
            kl_df = pd.DataFrame(kline, columns=["ots", "open", "high", "low", "close", "volume", "ts",
                                            "usd_v", "n_trades", "taker_buy_v", "taker_buy_usd", "ig"]).drop(["ots", "ig"], axis=1)
            kl_df['ts'] = kl_df['ts'] + 1
            kl_df['ts'] = kl_df['ts'].apply(TimeUtils.ms_to_timestamp)
            kl_df.set_index("ts", inplace=True)
            return kl_df
        # turn list in dict into df
        klines = {pair:parse(kline) for pair, kline in klines.items()}

        btc = klines["BTCUSDT"]
        st = btc.index[0]
        et = btc.index[-1]

        ts_lis = btc.index.tolist()
        col = btc.columns.tolist() + ['vwap']

        symbols = list(klines.keys())
        multi_idx = pd.MultiIndex.from_product([ts_lis, symbols], names=["ts", "symbol"])
        data = np.full(shape=(len(ts_lis), len(symbols), len(col)), fill_value=np.nan)

        wrong_symbol = []
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
                wrong_symbol.append(symbol)

        df = pd.DataFrame(data.reshape(-1, data.shape[-1]), index=multi_idx, columns=col)
        symbols = list(set(symbols).difference(set(wrong_symbol)))
        df = df.loc[(slice(None), symbols), :]
        return df

    @staticmethod
    def process_aggregated_symbols_kline(aggregated_klines: pd.DataFrame):
        """
        Calculate target and filter out the symbols with nan value
        """
        aggregated_klines['return_1'] = (aggregated_klines['close'].unstack().diff(1).shift(-1) / aggregated_klines['close'].unstack()).stack()
        symbols_with_complete_data = aggregated_klines["close"].unstack().isna().sum(axis=0)[
            aggregated_klines["close"].unstack().isna().sum(axis=0) == 0].index.tolist()
        aggregated_klines = aggregated_klines.loc[(slice(None), symbols_with_complete_data), :]
        return aggregated_klines

if __name__ == "__main__":
    client = DataClient(BinanceExchange)
    print(client.symbol_info_to_symbols(client.get_symbol_info()))