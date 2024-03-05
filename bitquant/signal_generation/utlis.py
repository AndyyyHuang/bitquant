import numpy as np
from numba import jit, prange
from joblib import Parallel, delayed

@jit(nopython=True,nogil=True,parallel=True)
def calc_zscore_2d(series,rolling_window):
    res=series.copy()#初始填充原始值，不是nan
    symbol_num=len(series[0,:])
    for i in prange(rolling_window,len(series)):
        temp=series[i+1-rolling_window:i+1,:]
        # s_mean=np.nanmean(temp,axis=0)
        # s_std=np.nanstd(temp,axis=0)
        for j in prange(symbol_num):
            s_mean=np.nanmean(temp[:,j])
            s_std=np.nanstd(temp[:,j])
            res[i,j] = (series[i,j]-s_mean)/max(s_std,10e-9)
    return res

def calc_zscore_2d_parallel(series,rolling_window,n_jobs=-2):
    symbol_num=len(series[0,:])
    last_num=symbol_num%10
    chunk_slice=list(np.arange(symbol_num-last_num).reshape(-1,10))
    chunk_slice.append(list(range(symbol_num-last_num,symbol_num)))
    task_res=Parallel(n_jobs=n_jobs)(delayed(calc_zscore_2d)(series[:,select_slice],rolling_window) for select_slice in chunk_slice)
    res=np.hstack(task_res)
    return res

@jit(nopython=True,nogil=True,parallel=True)
def calc_zscore_cross_section(series):
    res=series.copy()
    symbol_num=len(series[0,:])
    for i in prange(len(series)):
        temp = series[i, :]
        s_mean = np.nanmean(temp)
        s_std = np.nanstd(temp)
        for j in prange(symbol_num):
            res[i, j] = (series[i, j] - s_mean) / max(s_std, 10e-9)
    return res
