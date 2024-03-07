import pandas as pd
from pathlib import Path
# TODO: Construct the DataClient
# from bitquant.data.data_client import DataClient
from bitquant.quantlib.signal_generation.factor_calculator import FactorCalculator, function_map
from bitquant.quantlib.signal_generation.factor_selector import FactorSelector
from bitquant.quantlib.signal_generation.factor_scaler import FactorScaler
from bitquant.quantlib.signal_generation.factor_aggregator import FactorAggregatorIC

def test():
    factor_lis = ["ts_midpoint(ts_natr(high,low,close,7),14)", "ts_delta(dynamic_ts_max(ts_bbands(close,20),28),7)", "ts_midpoint(ts_ht_trendmode(close),21)"]
    # data = DataClient.start() # unfinished
    fp = Path(__file__).parent.parent.parent / "data/local_data/binanceusdm_4h_processed_kline.parquet"
    data = pd.read_parquet(fp)

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
    """
    calculator = FactorCalculator(data, factor_lis, function_map, different_axis=['ts', 'symbol', 'return_1'])
    factor_df = calculator.calculate_factor()

    scaled_factor_df = FactorScaler(factor_df=factor_df, factor_lis=factor_lis, scaling_window=180,
                                    orthogonalize=False,
                                    orthogonal_method='symmetry', ts_normalize=True,
                                    cross_section_normalize=False).scale_data()

    selector = FactorSelector(scaled_factor_df)
    filtered_factor_lis = selector.filter_out_high_corr_factor(threshold=0.6, greater_is_better=False)
    scaled_factor_df = scaled_factor_df.loc[:, filtered_factor_lis]

    target = data.loc[scaled_factor_df.index, 'return_1']

    factorAggregator = FactorAggregatorIC(scaled_factor_df=scaled_factor_df,
                                            target=target,
                                            training_window=90)
    factorAggregator.train()
    predictions = factorAggregator.predict()
    print(predictions)
    """
if __name__ == "__main__":
    test()