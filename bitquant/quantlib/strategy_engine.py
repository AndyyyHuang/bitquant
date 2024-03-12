import numpy as np


class StrategyEngine:
    """
    This is the example Strategy we provide showcase construct portfolio based on multi-factor model

    Miners can define any delta-neutral trading strategy to construct their investing portfolio you want here.
    The output is the weight how do you allocate your money to the symbols.
    Example: Let's say you have 100USDT. The symbol list is ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"], your output is [0.5, -0.2, -0.3, 0].
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

    def run(self, data):
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


