"""Utilities that are required by gplearn.

Most of these functions are slightly modified versions of some key utility
functions from scikit-learn that gplearn depends upon. They reside here in
order to maintain compatibility across different versions of scikit-learn.

"""

import numbers
import numpy as np
from joblib import cpu_count


def make_XY(df, index_name, columns_name, Y_column1,):
    '''
    return: X: ndarray[n_dates, n_feature, n_stocks], Y: ndarray[n_dates, n_stocks], X_feature_names
    '''
    df = df.pivot_table(index=[index_name], columns=[columns_name], sort=False, dropna=False)
    Y1 = df.loc[:,(Y_column1,)].to_numpy(dtype=np.double)

    df = df.drop([Y_column1,],axis=1)
    X_0_len = len(df.index)
    X_1_len = len(df.columns.levels[0]) - 1
    X_2_len = len(df.columns.levels[1])
    return df.to_numpy(dtype=np.double).reshape((X_0_len, X_1_len, X_2_len)), Y1, df.columns.levels[0].drop([Y_column1,])

def check_random_state(seed):
    """Turn seed into a np.random.RandomState instance

    Parameters
    ----------
    seed : None | int | instance of RandomState
        If seed is None, return the RandomState singleton used by np.random.
        If seed is an int, return a new RandomState instance seeded with seed.
        If seed is already a RandomState instance, return it.
        Otherwise raise ValueError.

    """
    if seed is None or seed is np.random:
        return np.random.mtrand._rand
    if isinstance(seed, (numbers.Integral, np.integer)):
        return np.random.RandomState(seed)
    if isinstance(seed, np.random.RandomState):
        return seed
    raise ValueError('%r cannot be used to seed a numpy.random.RandomState'
                     ' instance' % seed)


def _get_n_jobs(n_jobs):
    """Get number of jobs for the computation.

    This function reimplements the logic of joblib to determine the actual
    number of jobs depending on the cpu count. If -1 all CPUs are used.
    If 1 is given, no parallel computing code is used at all, which is useful
    for debugging. For n_jobs below -1, (n_cpus + 1 + n_jobs) are used.
    Thus for n_jobs = -2, all CPUs but one are used.

    Parameters
    ----------
    n_jobs : int
        Number of jobs stated in joblib convention.

    Returns
    -------
    n_jobs : int
        The actual number of jobs as positive integer.

    """
    if n_jobs < 0:
        return max(cpu_count() + 1 + n_jobs, 1)
    elif n_jobs == 0:
        raise ValueError('Parameter n_jobs == 0 has no meaning.')
    else:
        return n_jobs


def _partition_estimators(n_estimators, n_jobs):
    """Private function used to partition estimators between jobs."""
    # Compute the number of jobs
    n_jobs = min(_get_n_jobs(n_jobs), n_estimators)

    # Partition estimators between jobs
    n_estimators_per_job = (n_estimators // n_jobs) * np.ones(n_jobs,
                                                              dtype=int)
    n_estimators_per_job[:n_estimators % n_jobs] += 1
    starts = np.cumsum(n_estimators_per_job)

    return n_jobs, n_estimators_per_job.tolist(), [0] + starts.tolist()


def _syntax_adapter(formulation:str):
    '''

    Args:
        formulation: 待解析的语法字符串

    Returns: 字典{'TOT':["condition a","condition b"]}

    '''
    elements = formulation.split(" ")
    iter_combination = ""
    adapted_dictionary={}
    register_flag = ""
    for pos,element in enumerate(elements):
        if element == "TOT" or element == "TRA" or element == "OOB":
            if len(iter_combination)>0:
                adapted_dictionary[register_flag].append(iter_combination)
            register_flag = element
            iter_combination = ""
            if element not in adapted_dictionary.keys():
                adapted_dictionary[element] = []
            continue
        iter_combination = iter_combination+element
        if pos==len(elements)-1:
            adapted_dictionary[register_flag].append(iter_combination)

    return adapted_dictionary



# def contains_float(input_list):
#     for item in input_list:
#         if isinstance(item, float):
#             return True
#     return False

def check_floats(input_list,data):
    for item in input_list:
        if isinstance(item, float) and round(item, 3) == data:
            return True
    return False
# print(_syntax_adapter("TOT ((IC>=0.02) and (IR>=0.05)) TRA (IC>0.02)"))
# print(_syntax_adapter("TRA (IC>0.02) TOT (IC>=0.02) OOB (IR<0.05)"))




