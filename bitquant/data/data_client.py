import asyncio
from bitquant.data.exchange import BinanceExchange

exchange_map = {
    "BINANCE": BinanceExchange
}

class DataClient:
    def __init__(self, exchange_name):
        self.exchange = exchange_map[exchange_name]()

    def get_symbol_info(self):
        symbol_info = self.exchange.get_symbol_info()
        return symbol_info

    def get_klines(self, symbol_lis, interval, st, et):
        klines = asyncio.run(self.exchange.get_klines(symbol_lis, interval, st, et))
        return klines

    def get_aggregated_symbols_kline(self, symbol_lis, interval, st, et):
        aggregated_klines = self.exchange.get_aggregated_symbols_kline(symbol_lis, interval, st, et)
        return aggregated_klines

    def process_aggregated_symbols_kline(self, aggregated_klines):
        """
        Calculate target and filter out the symbols with nan value
        """
        aggregated_klines['return_1'] = (aggregated_klines['close'].unstack().diff(1).shift(-1) / aggregated_klines['close'].unstack()).stack()
        symbols_with_complete_data = aggregated_klines["close"].unstack().isna().sum(axis=0)[
            aggregated_klines["close"].unstack().isna().sum(axis=0) == 0].index.tolist()
        aggregated_klines = aggregated_klines.loc[(slice(None), symbols_with_complete_data), :]
        return aggregated_klines