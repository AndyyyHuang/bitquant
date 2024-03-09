from numba import jit, prange
import numpy as np
import pandas as pd
import copy
from scipy.stats import spearmanr


def rolling_window(a, window, axis=0):

    if axis == 0:
        shape = (a.shape[0] - window + 1, window, a.shape[-1])
        strides = (a.strides[0],) + a.strides
        a_rolling = np.lib.stride_tricks.as_strided(a, shape=shape, strides=strides)
    elif axis == 1:
        shape = (a.shape[-1] - window + 1,) + (a.shape[0], window)
        strides = (a.strides[-1],) + a.strides
        a_rolling = np.lib.stride_tricks.as_strided(a, shape=shape, strides=strides)
    return a_rolling


@jit(nopython=True, nogil=True, parallel=True)
def calc_zscore_2d(series, rolling_window=180):
    res = series.copy()  # 初始填充原始值，不是nan
    symbol_num = len(series[0, :])
    for i in prange(rolling_window, len(series)):
        temp = series[i + 1 - rolling_window:i + 1, :]
        for j in prange(symbol_num):
            s_mean = np.nanmean(temp[:, j])
            s_std = np.nanstd(temp[:, j])
            res[i, j] = (series[i, j] - s_mean) / max(s_std, 10e-9)
    return res


def rolling_nanmean(A, window=None):
    ret = pd.DataFrame(A)
    factor_table = copy.deepcopy(ret)
    for col in ret.columns:
        current_data = copy.deepcopy(ret[col])
        current_data.dropna(inplace=True)
        current_factor = current_data.rolling(window).mean().values
        number = 0
        for index, data in enumerate(ret[col]):

            if ret[col][index] != ret[col][index]:
                factor_table[col][index] = np.nan
            else:
                factor_table[col][index] = current_factor[number]
                number += 1
    factor = factor_table.to_numpy(dtype=np.double)
    return factor


def rolling_max(A, window=None):
    ret = pd.DataFrame(A)
    factor = ret.rolling(window).max()
    factor = factor.to_numpy(dtype=np.double)
    return factor


def rolling_nanstd(A, window=None):
    ret = pd.DataFrame(A)
    factor_table = copy.deepcopy(ret)
    for col in ret.columns:
        current_data = copy.deepcopy(ret[col])
        current_data.dropna(inplace=True)
        current_factor = current_data.rolling(window).std().values
        number = 0
        for index, data in enumerate(ret[col]):

            if ret[col][index] != ret[col][index]:
                factor_table[col][index] = np.nan
            else:
                factor_table[col][index] = current_factor[number]
                number += 1
    factor = factor_table.to_numpy(dtype=np.double)
    return factor

def cal_rolling_ic(y_pred, y, rolling_window):
    with np.errstate(divide='ignore', invalid='ignore'):
        no_future_beta = []
        for i in range(len(y_pred)):
            no_future_beta.append(spearmanr(y_pred[i], y[i])[0])
        no_future_beta = pd.Series(no_future_beta).shift(1).fillna(0)
        no_future_beta_rolling = no_future_beta.rolling(rolling_window).mean()
        weighted_factor_table = y_pred * no_future_beta_rolling

        return weighted_factor_table.to_numpy(dtype=np.double)