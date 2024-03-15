import numpy as np
from typing import Dict

import pandas as pd
from bitquant.utils.timeutils import TimeUtils
from bitquant.data.data_client import DataClient
from bitquant.data.exchange import BinanceExchange
from bitquant.quantlib.backtest.simulator import Simulator

class StrategyEngine:
    """
    This is the example Strategy we provide showcase construct portfolio based on multi-factor model

    Miners can define any delta-neutral trading strategy to construct their investing portfolio they want here.
    The output is the weight how do you allocate your money to the symbols.
    Example: Let's say you have 100USDT. your output is {"BTCUSDT": 0.5, "ETHUSDT": -0.2, "SOLUSDT": -0.3, "BNBUSDT": 0}.
    Then your investing portfolio is that long 50USDT BTCUSDT, short 20USDT ETHUSDT, short 30 USDT SOLUSDT
    """
    def __init__(self, init_factor_lis=None, factor_calculator=None, factor_scaler=None, factor_selector=None, factor_aggregator=None):

        self.init_factor_lis = init_factor_lis
        self.factor_calculator = factor_calculator
        self.factor_scaler = factor_scaler
        self.factor_selector = factor_selector
        self.factor_aggregator = factor_aggregator

    def check_delta_neutral(self, portfolio_weight):
        return np.abs(np.sum(portfolio_weight)) < 1e-6

    def check_portfolio_output(self, portfolio_weight):
        pass

    def get_score(self, data):

        factor_df = self.factor_calculator.calculate_factor(data, self.init_factor_lis)
        scaled_factor_df = self.factor_scaler.scale_data(factor_df, self.init_factor_lis)
        filtered_factor_lis = self.factor_selector.filter_out_high_corr_factor(factor_df=scaled_factor_df, threshold=0.6,
                                                                          greater_is_better=False)
        scaled_factor_df = scaled_factor_df.loc[:, filtered_factor_lis]
        target = data.loc[scaled_factor_df.index, 'return_1']
        self.factor_aggregator.train()
        scores = self.factor_aggregator.predict(scaled_factor_df=scaled_factor_df, target=target)
        return scores

    def run_backtest(self, symbols, interval, st, et, init_cash, order_size, taker_fee, display=True, plot=True) -> pd.DataFrame:
        # generate backtest data
        data_client = DataClient(BinanceExchange)
        symbol_info, data = data_client.run(symbols, interval, st, et)

        # calculate symbols score based on model
        scores = self.get_score(data)
        score_df = self.factor_aggregator.score_df.loc[:, symbols]

        # generate the portfolio history
        n, m = score_df.shape
        portfolio_weight_matrix = np.zeros(shape=(n, m))

        for i in range(n):
            scores = score_df.iloc[i]
            normalized_scores = scores - np.mean(scores)
            abs_sum = np.sum(np.abs(normalized_scores))
            if abs_sum != 0:
                portfolio_weight = normalized_scores / abs_sum
            else:
                portfolio_weight = normalized_scores

            portfolio_weight_matrix[i] = portfolio_weight

        # generate the price history
        ts_lis = score_df.index.tolist()

        price_matrix = data.unstack().shift(
            -1).ffill().stack().loc[(ts_lis, symbols), ["open", "low", "high", "close", "vwap"]].values.reshape(
            len(ts_lis), len(symbols), 5)

        price_matrix[-1, :, :] = data.loc[(ts_lis[-1], symbols), ["close", "close", "close", "close", "close"]].values

        # generate the volume precision and notional filter
        volume_precision = symbol_info.set_index("symbol").loc[symbols, "volume_precision"].values
        min_notional = symbol_info.set_index("symbol").loc[symbols, "notional"].values

        # backtest
        simulator = Simulator(portfolio_weight_matrix, price_matrix, ts_lis, symbols, init_cash, order_size, taker_fee,
                 volume_precision, min_notional, display=display, plot=plot)
        simulator.start_loop()
        simulator.result_analysis()


    def run(self, data) -> Dict:
        # Get scores series
        scores = self.get_score(data)
        # Construct hedge portfolio based on the scores we get from our multi-factor model

        # The most simple way is normalizing scores to have a sum of 0
        normalized_scores = scores - np.mean(scores)
        abs_sum = np.sum(np.abs(normalized_scores))
        if abs_sum != 0:
            portfolio_weight = normalized_scores / abs_sum
        else:
            portfolio_weight = normalized_scores

        """
        Another example:
        Another way you can consider is that: you can long the top 20% symbols with the highest scores and short the 20% symbols with the lowest scores
        
        portfolio_weight = deepcopy(scores)
        
        num = int((len(scores) * (0.2)) // 1)
        long_indices = np.argsort(scores)[-num:]
        short_indices = np.argsort(scores)[:num]

        portfolio_weight.iloc[:] = 0
        portfolio_weight.iloc[long_indices] = 0.5 / num
        portfolio_weight.iloc[short_indices] = -0.5 / num
        """


        if not self.check_delta_neutral(portfolio_weight):
            raise Exception("Error, not delta neutral")

        # convert it to dict that suits for bitquant.base.protocol.PortfolioRecord.portfolio
        portfolio_weight = portfolio_weight.to_dict()

        return portfolio_weight


