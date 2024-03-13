from datetime import datetime
import pandas as pd

from bitquant.data.data_client import DataClient
from bitquant.data.exchange import BinanceExchange
from bitquant.quantlib.functions.functions import *
from bitquant.quantlib.strategy_engine import StrategyEngine
from bitquant.quantlib.signal_generation.factor_calculator import FactorCalculator, function_map
from bitquant.quantlib.signal_generation.factor_selector import FactorSelector
from bitquant.quantlib.signal_generation.factor_scaler import FactorScaler
from bitquant.quantlib.signal_generation.factor_aggregator import FactorAggregatorIC
from bitquant.base.protocol import SymbolValueDict, PortfolioRecord
from bitquant.quantlib.evaluation.evaluator import Evaluator
from bitquant.utils.timeutils import TimeUtils

from warnings import filterwarnings
filterwarnings("ignore")


def miner():
    portfolio_record_lis = []
    for _ in range(3):
        symbols = ["ETHUSDT", "BTCUSDT", "BNBUSDT", "SOLUSDT"]
        interval = "1h"
        st = "2024-03-01 00:00:00"
        et = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        data_client = DataClient(BinanceExchange)
        symbol_info, data = data_client.run(symbols, interval, st, et)

        factor_lis = ["ts_midpoint(ts_natr(high,low,close,7),14)", "ts_delta(dynamic_ts_max(ts_bbands(close,20),28),7)",
                      "ts_midpoint(ts_ht_trendmode(close),21)"]

        factor_calculator = FactorCalculator(function_map, different_axis=['ts', 'symbol', 'return_1'])
        factor_scaler = FactorScaler(scaling_window=180, orthogonalize=False, orthogonal_method='symmetry',
                                     ts_normalize=True, cross_section_normalize=False)
        factor_selector = FactorSelector()
        factor_aggregator = FactorAggregatorIC(training_window=90, rolling_type="avg", ic_type='pearson')

        strategy_engine = StrategyEngine(init_factor_lis=factor_lis, factor_calculator=factor_calculator,
                                         factor_scaler=factor_scaler, factor_selector=factor_selector,
                                         factor_aggregator=factor_aggregator)
        portfolio_weight = strategy_engine.run(data)
        svdict = SymbolValueDict(portfolio_weight)
        portfolio_record = PortfolioRecord(portfolio=svdict)
        portfolio_record_lis.append(portfolio_record)


    return portfolio_record_lis


def validator(portfolio_record_lis):
    interval = "1m"

    flg = len(portfolio_record_lis) - 1
    portfolio_weight_container = []
    ts_ms_container = []
    for portfolio_record in portfolio_record_lis:
        # construct portfolio weight history
        portfolio_weight_container.append(portfolio_record.portfolio)
        # fake timestamp for test
        ts_ms_container.append(
            TimeUtils.now_in_ms() - flg * TimeUtils.interval_str_to_ms("30m") - 2 * TimeUtils.interval_str_to_ms(
                interval))
        flg -= 1

    # retrive the 1 minute kline data for validation
    ts_lis = [pd.to_datetime(ts_ms, unit="ms", utc=True).ceil("1min") for ts_ms in ts_ms_container]
    portfolio_weight_df = pd.DataFrame(portfolio_weight_container, index=ts_lis)
    symbols = portfolio_weight_df.columns.tolist()

    st = TimeUtils.timestamp_to_dt_str(ts_lis[0], "%Y-%m-%d %H:%M:%S")
    et = TimeUtils.timestamp_to_dt_str(ts_lis[-1] + TimeUtils.str_to_timedelta(interval), "%Y-%m-%d %H:%M:%S")

    data_client = DataClient(BinanceExchange)
    symbol_info, data = data_client.run(symbols=symbols, interval=interval, st=st, et=et)

    # prepare data for evaluation
    portfolio_weight_matrix = portfolio_weight_df.to_numpy()
    price_matrix = data.loc[
        [ts + TimeUtils.str_to_timedelta(interval) for ts in ts_lis], ["open", "low", "high", "close",
                                                                       "vwap"]].values.reshape(len(ts_lis),
                                                                                               len(symbols), 5)

    volume_precision = symbol_info.set_index("symbol").loc[symbols, "volume_precision"].values
    min_notional = symbol_info.set_index("symbol").loc[symbols, "notional"].values

    evaluator = Evaluator()
    profit_ratio = evaluator.evaluate(portfolio_weight_matrix, price_matrix, init_asset_value=10000, taker_fee=0.0004,
                                      volume_precision=volume_precision, min_notional=min_notional)
    print(f"Miner's profit ratio: {round(profit_ratio*100, 3)}%")

    return profit_ratio


if __name__ == "__main__":
    portfolio_record_lis = miner()
    profit_ratio = validator(portfolio_record_lis)