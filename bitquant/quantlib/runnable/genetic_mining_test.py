import pandas as pd
import numpy as np
from pathlib import Path
from bitquant.quantlib.factor_mining.genetic_programming.genetic import SymbolicTransformer
from bitquant.quantlib.factor_mining.genetic_programming.functions import *
from bitquant.quantlib.factor_mining.genetic_programming.functions import _function_map
from bitquant.quantlib.factor_mining.genetic_programming.utils import make_XY

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

if __name__ == "__main__":

    aggregated_kline_fp = Path(__file__).resolve().parent.parent.parent / "data/local_data/binanceusdm_4h_aggregated_kline.parquet"
    processed_kline_fp = Path(__file__).resolve().parent.parent.parent / "data/local_data/binanceusdm_4h_processed_kline.parquet"
    data = pd.read_parquet(aggregated_kline_fp)
    data = preprocess_data(data, st="2023-06-01", et="2025-01-01")
    data.to_parquet(processed_kline_fp)

    different_axis = ['ts', 'symbol', 'return_1']
    X, Y, feature_names = make_XY(data, *different_axis)
    sample_weight = [1] * X.shape[0]
    sample_weight = np.array(sample_weight)
    max_samples = X.shape[0]
    init_function_set = []
    function_set = list(_function_map.values())

    gp_transformer = SymbolicTransformer(generations=10,
                                        population_size=2000,
                                        tournament_size=400,
                                        stopping_criteria=4.0,
                                        init_depth=(2, 5),
                                        hall_of_fame=1200,
                                        n_components=400,
                                        function_set=function_set,
                                        init_function_set=init_function_set,
                                        metric="pnl",
                                        rolling_window_1=180,
                                        rolling_window_2=90,
                                        fee=0.0004,
                                        const_range=(-1, 1),
                                        p_crossover=0.6,
                                        p_hoist_mutation=0.01,
                                        p_subtree_mutation=0.1,
                                        p_point_mutation=0.1,
                                        p_point_replace=0.6,
                                        parsimony_coefficient="auto",
                                        feature_names=feature_names,
                                        max_samples=max_samples,
                                        verbose=1, low_memory=False,
                                        random_state=111, n_jobs=-1)
    gp_transformer.fit_3D(X, Y, feature_names, sample_weight=sample_weight, need_parallel=True)

    results = pd.DataFrame(columns=["formulation", "fitness"])

    for program_batch in gp_transformer._programs:
        if program_batch == None:
           pass
        else:
            for program in program_batch:
                if program == None:
                    pass
                else:
                    results = results._append({'formulation': program.__str__(), 'fitness': program.raw_fitness_}, ignore_index=True)

    results.drop_duplicates(subset='formulation', keep='last', inplace=True).sort_values(by='fitness', ascending=False, inplace=True)
