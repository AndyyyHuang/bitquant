from scipy.stats import spearmanr
from numpy.linalg import inv, det
import numpy as np
import pandas as pd


class FactorAggregatorIC:
    def __init__(self,
                 training_window,
                 rolling_type="avg",
                 ic_type='pearson'):

        self.training_window = training_window
        self.rolling_type = rolling_type
        self.ic_type = ic_type

    def train(self):
        """IC Aggregator doesn't need any pretrain"""
        pass

    def predict(self, scaled_factor_df, target):
        factor_ic_df = self.average_IC_combination(scaled_factor_df=scaled_factor_df, target=target, ic_type=self.ic_type)
        score_df = self.calculate_score(scaled_factor_df=scaled_factor_df, factor_ic_df=factor_ic_df,
                                        rolling_window=self.training_window, rolling_type=self.rolling_type)
        prediction = score_df.iloc[-1]
        return prediction


    def average_IC_combination(self, scaled_factor_df, target, ic_type):
        time_idx_lis = pd.unique(scaled_factor_df.index.get_level_values(0))
        symbol_lis = pd.unique(scaled_factor_df.index.get_level_values(1))
        factor_lis = scaled_factor_df.columns.tolist()
        n = len(time_idx_lis)
        m = len(symbol_lis)
        k = len(factor_lis)

        self.factor_ic_df = pd.DataFrame(index=time_idx_lis, columns=factor_lis)
        X_array = np.asarray(scaled_factor_df).reshape(n, m, k).astype(np.float32)
        y_array = np.asarray(target).reshape(n, m).astype(np.float32)

        for i in range(n):
            X = X_array[i]
            y = y_array[i]
            ic_lis = []
            for j in range(k):
                if ic_type == 'spearmanr':
                    ic_lis.append(spearmanr(X[:, j], y)[0])
                elif ic_type == 'pearson':
                    ic_lis.append(np.corrcoef(X[:, j], y)[0][1])
            self.factor_ic_df.iloc[i] = ic_lis
        return self.factor_ic_df

    def _simplecov_weight(self, x):
        ic_mean = x.mean()
        ic_cov = x.cov()
        if (det(ic_cov) == 0) | (len(ic_cov) == 0):
            weight_ = ic_mean / np.abs(ic_mean).sum()
        elif det(ic_cov) != 0:
            try:
                cov_inv = inv(ic_cov)
                weight = cov_inv.dot(ic_mean)
                weight_ = weight / np.abs(weight).sum()
            except:
                weight_ = ic_mean / np.abs(ic_mean).sum()
        return weight_


    def calculate_score(self, scaled_factor_df, factor_ic_df, rolling_window, rolling_type):

        factor_ic_df = factor_ic_df.shift(1).bfill()

        if rolling_type == 'ewm' and rolling_window != 1:
            self.factor_ic_df_rolling = factor_ic_df.ewm(rolling_window).mean()[rolling_window - 1:]
        elif rolling_type == 'avg':
            self.factor_ic_df_rolling = factor_ic_df.rolling(rolling_window).mean()[rolling_window - 1:]
        elif rolling_type == 'IC-IR':
            self.factor_ic_df_rolling = factor_ic_df.rolling(rolling_window).apply(lambda x: x.mean() / x.std())[
                                        rolling_window - 1:]
        elif rolling_type == 'Max IC-IR':
            self.factor_ic_df_rolling = pd.DataFrame(index=factor_ic_df.index[rolling_window - 1:],
                                                     columns=factor_ic_df.columns)
            for i in range(len(factor_ic_df) - rolling_window):
                x = factor_ic_df.iloc[i:i + rolling_window]
                self.factor_ic_df_rolling.iloc[i] = self._simplecov_weight(x)

        self.factor_exposure = scaled_factor_df.loc[self.factor_ic_df_rolling.index]

        self.score_df = pd.DataFrame(index=pd.unique(self.factor_exposure.index.get_level_values(0)),
                                             columns=pd.unique(self.factor_exposure.index.get_level_values(1)))

        for time_idx, row in self.factor_exposure.groupby(level=0):
            self.score_df.loc[time_idx] = np.dot(self.factor_ic_df_rolling.loc[time_idx], row.T)

        return self.score_df
