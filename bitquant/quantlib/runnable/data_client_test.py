from bitquant.data.data_client import DataClient



if __name__ == "__main__":
    symbol_lis = ["BTCUSDT", "ETHUSDT"]
    interval = "1h"
    st = "2023-01-01 00:00:00"
    et = "2024-01-01 00:00:00"
    data_client = DataClient(exchange_name="BINANCE")
    symbol_info = data_client.exchange.get_symbol_info()
    aggregated_klines = data_client.exchange.get_aggregated_symbols_kline(symbol_lis, interval, st, et)
    print(symbol_info, aggregated_klines)