from datetime import datetime
from bitquant.data.data_client import DataClient
from bitquant.quantlib.strategy_engine import StrategyEngine
from bitquant.quantlib.signal_generation.factor_calculator import FactorCalculator, function_map
from bitquant.quantlib.signal_generation.factor_selector import FactorSelector
from bitquant.quantlib.signal_generation.factor_scaler import FactorScaler
from bitquant.quantlib.signal_generation.factor_aggregator import FactorAggregatorIC


def test():

    symbol_lis = ["ETHUSDT", "BTCUSDT", "BNBUSDT", "SOLUSDT"]
    interval = "1h"
    st = "2024-01-01 00:00:00"
    et = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    data_client = DataClient(exchange_name="BINANCE")
    aggregated_kline = data_client.get_aggregated_symbols_kline(symbol_lis=symbol_lis, interval=interval, st=st, et=et)
    data = data_client.process_aggregated_symbols_kline(aggregated_kline)

    factor_lis = ["ts_midpoint(ts_natr(high,low,close,7),14)", "ts_delta(dynamic_ts_max(ts_bbands(close,20),28),7)",
                  "ts_midpoint(ts_ht_trendmode(close),21)"]

    factor_calculator = FactorCalculator(function_map, different_axis=['ts', 'symbol', 'return_1'])
    factor_scaler = FactorScaler(scaling_window=180, orthogonalize=False, orthogonal_method='symmetry', ts_normalize=True, cross_section_normalize=False)
    factor_selector = FactorSelector()
    factor_aggregator = FactorAggregatorIC(training_window=90, rolling_type="avg", ic_type='pearson')

    strategy_engine = StrategyEngine(init_factor_lis=factor_lis, factor_calculator=factor_calculator,
                                     factor_scaler=factor_scaler, factor_selector=factor_selector, factor_aggregator=factor_aggregator)
    portfolio_weight = strategy_engine.run(data=data)
    print(portfolio_weight)

if __name__ == "__main__":
    test()
