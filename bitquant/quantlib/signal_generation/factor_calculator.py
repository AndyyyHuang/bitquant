from bitquant.quantlib.factor_mining.genetic_programming.functions import _function_map as function_map
from bitquant.quantlib.factor_mining.genetic_programming.functions import *
from bitquant.quantlib.factor_mining.genetic_programming.utils import make_XY
import pandas as pd
import re

# TODO: what is wrong with this?
# global function_map

class FactorCalculator:

    def __init__(self, data, factor_lis: list, function_map: dict, different_axis: list = ['ts', 'symbol', 'return_1']):
        self.data = data
        self.factor_lis = factor_lis
        self.function_map = function_map
        self.different_axis = different_axis


    def calculate_factor(self):
        factor_df = pd.DataFrame(index=self.data.index, columns=self.factor_lis)
        for factor in self.factor_lis:
            factor_df[factor] = self.calculate_factor_based_on_formulation(data=self.data, factor=factor, different_axis=self.different_axis, function_map=self.function_map)
        return factor_df


    def calculate_factor_based_on_formulation(self, data, factor, different_axis, function_map):

        X, Y, feature_names = make_XY(data, *different_axis)
        formulation = factor

        feature_dictionary = {}
        for index, feature in enumerate(feature_names):
            feature_dictionary[feature] = "X[:,{},:]".format(index)

        ts_lis = pd.unique(data.index.get_level_values(0)).tolist()
        symbol_lis = pd.unique(data.index.get_level_values(1)).tolist()

        all_cal_dictionary = dict(list(function_map.items()))

        # Use regular expressions to find the number in the string
        if len(re.findall(r'-?\d+\.\d+', formulation)) > 0:
            # Use regular expressions to find all the numbers in the string
            numbers = re.findall(r'-?\d+\.\d+', formulation)

            # Replace each number with the desired string
            new_string = formulation
            for number in numbers:
                new_string = new_string.replace(number,
                                                "np.tile({}, (X.shape[0], X.shape[2]))".format(
                                                    number))
            formulation = new_string
        for feature, feature_input in feature_dictionary.items():
            formulation = re.sub(r"(?<![a-zA-Z])" + re.escape(feature) + r"(?![a-zA-Z])", feature_input, formulation)

        for function_name, _function in all_cal_dictionary.items():
            formulation = re.sub(r"\b" + re.escape(function_name) + r"\b",
                                 "all_cal_dictionary['{}']".format(function_name),
                                 formulation)

        factor_df = pd.DataFrame(eval(formulation), index=ts_lis, columns=symbol_lis).stack()

        return factor_df