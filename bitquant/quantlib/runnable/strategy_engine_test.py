from bitquant.quantlib.strategy_engine import StrategyEngine
from bitquant.quantlib.signal_generation.factor_calculator import FactorCalculator, function_map
from bitquant.quantlib.signal_generation.factor_selector import FactorSelector
from bitquant.quantlib.signal_generation.factor_scaler import FactorScaler
from bitquant.quantlib.signal_generation.factor_aggregator import FactorAggregatorIC

import pandas as pd
from pathlib import Path

def test():

    factor_calculator = FactorCalculator(function_map, different_axis=['ts', 'symbol', 'return_1'])
    factor_scaler = FactorScaler(scaling_window=180, orthogonalize=False, orthogonal_method='symmetry', ts_normalize=True, cross_section_normalize=False)
    factor_selector = FactorSelector()
    factor_aggregator = FactorAggregatorIC(training_window=90, rolling_type="avg", ic_type='pearson')
    factor_lis = ["ts_midpoint(ts_natr(high,low,close,7),14)", "ts_delta(dynamic_ts_max(ts_bbands(close,20),28),7)", "ts_midpoint(ts_ht_trendmode(close),21)"]

    # data = DataClient.start() # unfinished
    fp = Path(__file__).parent.parent.parent / "data/local_data/binanceusdm_4h_processed_kline.parquet"
    data = pd.read_parquet(fp)

    strategy_engine = StrategyEngine(data=data, init_factor_lis=factor_lis, factor_calculator=factor_calculator, factor_scaler=factor_scaler, factor_selector=factor_selector, factor_aggregator=factor_aggregator)
    portfolio_weight = strategy_engine.run()
    print(portfolio_weight)

if __name__ == "__main__":
    test()