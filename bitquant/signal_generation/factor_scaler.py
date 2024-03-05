import numpy as np
import pandas as pd
from copy import deepcopy
from utlis import calc_zscore_2d, calc_zscore_cross_section

class FactorScaler:

    def __init__(self,
                 data,
                 selected_factor_lis,
                 scaling_window,
                 training_window,
                 predicting_window,
                 orthogonalize=False,
                 orthogonal_method='symmetry',
                 ts_normalize=True,
                 cross_section_normalize=False):

        self.selected_factor_lis = selected_factor_lis
        self.data = deepcopy(data.loc[:, self.selected_factor_lis + ['return_1']])
        self.scaling_window = scaling_window
        self.training_window = training_window
        self.predicting_window = predicting_window
        self.orthogonalize = orthogonalize
        self.orthogonal_method = orthogonal_method
        self.ts_normalize = ts_normalize
        self.cross_section_normalize = cross_section_normalize

    @classmethod
    def scale_data(cls):
        if cls.ts_normalize:
            cls.scaled_data = cls.ts_normalize(data=cls.data,
                                               selected_factor_lis=cls.selected_factor_lis,
                                               rolling_window=cls.scaling_window)
        if cls.cross_section_normalize:
            cls.scaled_data = cls.cross_section_normalize(data=cls.scaled_data,
                                                                   selected_factor_lis=cls.selected_factor_lis)
        if cls.orthogonalize:
            cls.scaled_data = cls.orthogonalize(data=cls.scaled_data,
                                                         selected_factor_lis=cls.selected_factor_lis,
                                                         orthogonal_method=cls.orthogonal_method)
        return cls.scaled_data

    @classmethod
    def ts_normalize(cls, data, selected_factor_lis, rolling_window):
        cls.normalized_data = deepcopy(data)

        for factor in selected_factor_lis:
            cls.normalized_data[factor] = pd.DataFrame(
                calc_zscore_2d(cls.normalized_data[factor].unstack().values, rolling_window),
                index=cls.normalized_data[factor].unstack().index,
                columns=cls.normalized_data[factor].unstack().columns).stack()
        cls.normalized_data = cls.normalized_data.unstack().iloc[rolling_window - 1:].stack()
        tmp_factor_exposure = cls.normalized_data.loc[:, selected_factor_lis].unstack().ffill(axis=0).dropna(
            how='all').fillna(0).stack()
        cls.normalized_data.loc[tmp_factor_exposure.index, selected_factor_lis] = tmp_factor_exposure
        return cls.normalized_data
    @classmethod
    def cross_section_normalize(cls, data, selected_factor_lis):
        cls.normalized_data = deepcopy(data)

        for factor in selected_factor_lis:
            cls.normalized_data[factor] = pd.DataFrame(
                calc_zscore_cross_section(cls.normalized_data[factor].unstack().values),
                index=cls.normalized_data[factor].unstack().index,
                columns=cls.normalized_data[factor].unstack().columns).stack()
        tmp_factor_exposure = cls.normalized_data.loc[:, selected_factor_lis].unstack().ffill(axis=0).dropna(
            how='all').fillna(0).stack()
        cls.normalized_data.loc[tmp_factor_exposure.index, selected_factor_lis] = tmp_factor_exposure
        return cls.normalized_data

    @classmethod
    def orthogonalize(cls, data, selected_factor_lis, orthogonal_method):
        cls.orthogonalized_data = deepcopy(data)
        cls.factors_df = cls.orthogonalized_data.loc[:, selected_factor_lis]
        time_idx_lis = pd.unique(cls.factors_df.index.get_level_values(0))
        for ts in time_idx_lis:
            tmp_factors_df = cls.factors_df.loc[ts].copy()
            cls.tmp_factors_df = tmp_factors_df
            orthogonal_res = Orthogonal.orthogonalize(tmp_factors_df, orthogonal_method)
            cls.orthogonalized_data.loc[ts, selected_factor_lis] = orthogonal_res.values
        return cls.orthogonalized_data



class Orthogonal:
    def __init__(self):
        pass

    def Symmetry(self, factors_df):
        try:
            col_name = factors_df.columns
            M = (factors_df.shape[0] - 1) * np.cov(factors_df.T.astype(float))
            D, U = np.linalg.eig(M)  # 获取特征值和特征向量
            U = np.mat(U)  # 转换为np中的矩阵
            d = np.mat(np.diag(D ** (-0.5)))  # 对特征根元素开(-0.5)指数
            S = U * d * U.T  # 获取过度矩阵S

            factors_orthogonal_mat = np.mat(factors_df) * S  # 获取对称正交矩阵
            factors_orthogonal = pd.DataFrame(factors_orthogonal_mat, index=factors_df.index,
                                              columns=col_name)  # 矩阵转为dataframe
            return factors_orthogonal
        except:
            return factors_df

    def Schimidt(self, factors_df):
        col_name = factors_df.columns
        row_name = factors_df.index
        factors_df = factors_df.values.copy()

        R = np.zeros((factors_df.shape[1], factors_df.shape[1]))
        Q = np.zeros(factors_df.shape)
        for k in range(0, factors_df.shape[1]):
            R[k, k] = np.sqrt(np.dot(factors_df[:, k], factors_df[:, k]))
            Q[:, k] = factors_df[:, k] / R[k, k]
            for j in range(k + 1, factors_df.shape[1]):
                R[k, j] = np.dot(Q[:, k], factors_df[:, j])
                factors_df[:, j] = factors_df[:, j] - R[k, j] * Q[:, k]

        Q = pd.DataFrame(Q, columns=col_name, index=row_name)
        return Q

    @classmethod
    def orthogonalize(cls, factors_df, orthogonal_method='symmetry'):
        if orthogonal_method == 'symmetry':
            orthogonal_res = cls.Symmetry(factors_df)
        elif orthogonal_method == 'schimidt':
            orthogonal_res = cls.Schimidt(factors_df)
        return orthogonal_res