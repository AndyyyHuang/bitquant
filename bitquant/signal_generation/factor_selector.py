import pandas as pd

class FactorSelector:
    def __init__(self, factor_df):
        self.corr_df = factor_df.corr()


    def filter_out_high_corr_factor(self, threshold=0.6, greater_is_better=False):
        """Backforward select"""
        filtered_factor_lis = []
        factor_lis = self.corr_df.columns.tolist()
        for i in reversed(range(len(self.corr_df))):
            corr_ser = self.corr_df.iloc[i, :i]
            if greater_is_better:
                if (corr_ser < threshold).sum() == 0:
                    filtered_factor_lis.insert(0, factor_lis[i])
            else:
                if (corr_ser > threshold).sum() == 0:
                    filtered_factor_lis.insert(0, factor_lis[i])
        return filtered_factor_lis

    def find_low_corr_combination(self, init_factor, threshold=0.6, combination_num=3, max_num_per_iter=None,
                                  greater_is_better=False):
        """tree search algorithm"""
        factor_lis = self.corr_df.columns.tolist()
        if max_num_per_iter is None:
            max_num_per_iter = len(factor_lis)

        current_idx = factor_lis.index(init_factor)
        flg = combination_num - 1
        combination_lis = [[current_idx]]

        while flg:
            new_combination_lis = []
            for i in range(len(combination_lis)):
                current_combination = combination_lis[i]
                if greater_is_better:
                    candidate_ser = (self.corr_df.iloc[:, current_combination] > threshold).sum(axis=1)
                    candidate_idx = [factor_lis.index(candidate_ser.index[j]) for j in range(len(candidate_ser)) if
                                     candidate_ser[j] == len(current_combination)][:max_num_per_iter]

                else:
                    candidate_ser = (self.corr_df.iloc[:, current_combination] < threshold).sum(axis=1)
                    candidate_idx = [factor_lis.index(candidate_ser.index[j]) for j in range(len(candidate_ser)) if
                                     candidate_ser[j] == len(current_combination)][:max_num_per_iter]

                tmp_combination_lis = [current_combination + [each] for each in candidate_idx if
                                       each not in current_combination]
                new_combination_lis.extend(tmp_combination_lis)

            combination_lis = new_combination_lis
            flg -= 1

        sorted_sublists = [sorted(sublist) for sublist in combination_lis]

        unique_tuples = set(tuple(sublist) for sublist in sorted_sublists)

        factor_combination_lis = [pd.Series(factor_lis)[list(tup)].values.tolist() for tup in unique_tuples]

        return factor_combination_lis
