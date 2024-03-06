import pandas as pd
from pathlib import Path
from bitquant.data_stream.data_client import DataClient
from bitquant.signal_generation.factor_calculator import FactorCalculator, function_map
from bitquant.signal_generation.factor_selector import FactorSelector
from bitquant.signal_generation.factor_scaler import FactorScaler
from bitquant.signal_generation.factor_aggregator import FactorAggregatorIC

def test():
    factor_lis = ["ts_midpoint(ts_natr(high,low,close,7),14)", "ts_delta(dynamic_ts_max(ts_bbands(close,20),28),7)", "ts_midpoint(ts_ht_trendmode(close),21)"]
    # data = DataClient.start() # unfinished
    fp = Path(__file__).parent.parent / "data_stream/local_data/binanceusdm_4h_processed_kline.parquet"
    data = pd.read_parquet(fp)

    calculator = FactorCalculator(data, factor_lis, function_map, different_axis=['ts', 'symbol', 'return_1'])
    factor_df = calculator.calculate_factor()

    selector = FactorSelector(factor_df)
    filtered_factor_lis = selector.filter_out_high_corr_factor(threshold=0.6, greater_is_better=False)

    scaled_factor_df = FactorScaler(factor_df=factor_df, factor_lis=filtered_factor_lis, scaling_window=180, orthogonalize=False,
                    orthogonal_method='symmetry', ts_normalize=True, cross_section_normalize=False).scale_data()

    target = data.loc[scaled_factor_df.index, 'return_1']

    factorAggregator = FactorAggregatorIC(scaled_factor_df=scaled_factor_df,
                                            target=target,
                                            training_window=90)
    factorAggregator.train()
    predictions = factorAggregator.predict()
    print(predictions)

if __name__ == "__main__":
    test()