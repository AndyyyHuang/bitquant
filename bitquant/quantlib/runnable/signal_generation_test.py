import pandas as pd
from pathlib import Path
# TODO: Construct the DataClient
from datetime import datetime
from bitquant.data.data_client import DataClient
from bitquant.quantlib.signal_generation.factor_calculator import FactorCalculator, function_map
from bitquant.quantlib.signal_generation.factor_selector import FactorSelector
from bitquant.quantlib.signal_generation.factor_scaler import FactorScaler
from bitquant.quantlib.signal_generation.factor_aggregator import FactorAggregatorIC

def test():


    symbol_lis = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"]
    interval = "1h"
    st = "2024-01-01 00:00:00"
    et = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    data_client = DataClient(exchange_name="BINANCE")
    aggregated_kline = data_client.get_aggregated_symbols_kline(symbol_lis=symbol_lis, interval=interval, st=st, et=et)
    data = data_client.process_aggregated_symbols_kline(aggregated_kline)


    factor_lis = ["ts_midpoint(ts_natr(high,low,close,7),14)", "ts_delta(dynamic_ts_max(ts_bbands(close,20),28),7)", "ts_midpoint(ts_ht_trendmode(close),21)"]
    factor_calculator = FactorCalculator(function_map, different_axis=['ts', 'symbol', 'return_1'])
    factor_scaler = FactorScaler(scaling_window=180, orthogonalize=False, orthogonal_method='symmetry',
                                 ts_normalize=True, cross_section_normalize=False)
    factor_selector = FactorSelector()
    factor_aggregator = FactorAggregatorIC(training_window=90, rolling_type="avg", ic_type='pearson')

    factor_df = factor_calculator.calculate_factor(data, factor_lis)
    scaled_factor_df = factor_scaler.scale_data(factor_df, factor_lis)
    filtered_factor_lis = factor_selector.filter_out_high_corr_factor(factor_df=scaled_factor_df, threshold=0.6, greater_is_better=False)
    scaled_factor_df = scaled_factor_df.loc[:, filtered_factor_lis]
    target = data.loc[scaled_factor_df.index, 'return_1']
    factor_aggregator.train()
    predictions = factor_aggregator.predict(scaled_factor_df=scaled_factor_df, target=target)
    print(predictions)

if __name__ == "__main__":
    test()