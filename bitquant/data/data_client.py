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
        aggregated_klines = self.exchange.get_aggregated_symbols_kline(symbols, interval, st, et)
        return aggregated_klines

    def run(self, symbols: List[str], interval, st, et):
        aggregated_klines = self.get_aggregated_symbols_kline(symbols, interval, st, et)
        symbol_info = self.get_symbol_info()
        return symbol_info, aggregated_klines


if __name__ == "__main__":
    client = DataClient(BinanceExchange)
    print(client.symbol_info_to_symbols(client.get_symbol_info()))