import numba as nb
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from typing import List


@nb.njit
def _start_loop(portfolio_weight_matrix, trade_price_matrix, init_cash, order_size, taker_fee, volume_precision, min_notional):
    n, m = portfolio_weight_matrix.shape
    res = np.zeros((n, m+1))
    res.fill(np.nan)
    position = np.array([0.0] * m)
    cash = init_cash
    for i in range(n):
        present_price = trade_price_matrix[i]
        # update the current portfolio weight
        current_portfolio_weight = position * present_price / order_size

        # calculate the expected portfolio weight
        expected_portfolio_weight = portfolio_weight_matrix[i]

        trade_portfolio_weight = expected_portfolio_weight - current_portfolio_weight
        orders_v = trade_portfolio_weight * order_size / present_price
        orders_v = np.array([round(orders_v[j], volume_precision[j]) for j in range(m)])

        # Filter out the order that doesn't meet the requirement of min_notional.
        # For example, you want to long 0.1 USDT BTCUSDT. But in real trading environment, the minimum notional of order is 10USDT. So this order won't be sent out.
        orders_allowance = (np.abs(orders_v) * present_price) < min_notional
        orders_v[orders_allowance] = 0

        tradeAmountInUsdt = -1 * orders_v * present_price * np.where(trade_portfolio_weight > 0, 1 + taker_fee, 1 - taker_fee)
        position_change = orders_v
        position += position_change
        cash += np.sum(tradeAmountInUsdt)
        res[i] = np.append(position, cash)

    return res


class Simulator:

    def __init__(self, portfolio_weight_matrix: np.array, price_matrix: np.array, ts_lis: List, symbol_lis: List, init_cash: float, order_size: float, taker_fee: float,
                 volume_precision: np.array, min_notional: np.array, display=True, plot=True):
        self.portfolio_weight_matrix = portfolio_weight_matrix
        self.price_matrix = price_matrix

        self.open_price_matrix = price_matrix[:, :, 0]
        self.low_price_matrix = price_matrix[:, :, 1]
        self.hight_price_matrix = price_matrix[:, :, 2]
        self.close_price_matrix = price_matrix[:, :, 3]
        self.vwap_price_matrix = price_matrix[:, :, 4]

        self.ts_lis = ts_lis
        self.symbol_lis = symbol_lis
        self.init_cash = init_cash
        self.order_size = order_size
        self.taker_fee = taker_fee
        self.volume_precision = volume_precision
        self.min_notional = min_notional
        self.display = display
        self.plot = plot
        self.n, self.m = len(self.ts_lis), len(self.symbol_lis)


    def start_loop(self):
        self.res = _start_loop(self.portfolio_weight_matrix, self.open_price_matrix, self.init_cash, self.order_size, self.taker_fee, self.volume_precision, self.min_notional)

    def result_analysis(self):
        self.position_matrix = self.res[:, :-1].copy()
        self.mark_price_matrix = self.price_matrix[:, :, 3]  # close
        self.filled_price_matrix = self.price_matrix[:, :, 0]  # open
        self.res[:, :-1] = self.res[:, :-1] * self.mark_price_matrix  # close
        if not isinstance(self.symbol_lis, list):
            self.symbol_lis = self.symbol_lis.tolist()

        freq = self.ts_lis[-1] - self.ts_lis[-2]
        # 将ts_lis往后移动！！！因为用到了下一时刻的收盘价作为mark price。因此equity的值对应的是这个信号产生后的这个interval的结束时间点的值
        self.marked_ts_lis = [each + freq for each in self.ts_lis]
        self.res = pd.DataFrame(self.res, index=self.marked_ts_lis, columns=self.symbol_lis + ['cash'])
        self.res['equity'] = self.res.sum(axis=1)
        self.res['net_leverage'] = self.res.loc[:, self.symbol_lis].sum(axis=1) / self.res.equity
        self.res['total_leverage'] = self.res.loc[:, self.symbol_lis].abs().sum(axis=1) / self.res.equity
        # 最后再dropna，否则index可能出现不匹配（）
        self.res.dropna(inplace=True)
        self.position_df = pd.DataFrame(self.position_matrix, index=self.ts_lis, columns=self.symbol_lis)
        self.mark_price_df = pd.DataFrame(self.mark_price_matrix, index=self.ts_lis,
                                          columns=self.symbol_lis)
        self.filled_price_df = pd.DataFrame(self.filled_price_matrix, index=self.ts_lis,
                                            columns=self.symbol_lis)
        time_span = self.res.index[-1] - self.res.index[0]

        position_usd_change = self.position_df.diff() * self.filled_price_df
        # 手续费计算
        taker_fee_cost = np.nansum(position_usd_change.abs()) * self.taker_fee
        maker_fee_cost = 0

        n_trades = (self.position_df.loc[:, self.symbol_lis].diff() != 0).sum().sum()
        profit = self.res.equity.iloc[-1] - self.res.equity.iloc[0]
        pnl_percentage = profit / self.res.equity.iloc[0] * 100
        equity_cummax = self.res.equity.cummax()
        max_drawdown_percentage = - (self.res.equity / equity_cummax - 1).min() * 100
        total_traded_quantity = np.nansum(position_usd_change.abs())

        sampled_return = self.res['equity'].diff() / self.res['equity'].shift(1).abs()
        sampled_return.dropna(inplace=True)
        annual_sharpe_ratio = sampled_return.mean() / sampled_return.std() * np.sqrt(
            float(pd.Timedelta(days=365) / freq))

        negative_returns = np.where(sampled_return >= 0, 0, sampled_return)
        downside_deviation = np.std(negative_returns)

        if downside_deviation == 0:
            downside_deviation = -np.inf

        # Calculate the Sortino ratio
        annual_sortino_ratio = np.mean(sampled_return) / downside_deviation * np.sqrt(
            float(pd.Timedelta(days=365) / freq))

        turnover = (self.position_df.diff().abs() * self.filled_price_df).resample('1h').agg('sum') / self.order_size

        equity_change = (self.res['equity'] - self.res['equity'].shift(1)).dropna()
        win_rate = len(equity_change[equity_change > 0]) / len(equity_change[equity_change != 0]) * 100

        # 平均盈亏比
        avg_pnl_ratio = equity_change[equity_change > 0].mean() / np.abs(equity_change[equity_change < 0].mean()) * 100

        convert_to_per_second = float(pd.Timedelta(seconds=1) / time_span)

        self.annual_pnl = pnl_percentage * float(pd.Timedelta(days=365) / time_span)
        self.annual_sharpe_ratio = annual_sharpe_ratio
        self.max_drawdown = max_drawdown_percentage
        self.win_rate = win_rate
        self.pnl_ratio = avg_pnl_ratio
        self.turnover = turnover.mean(axis=0).sum()

        self.describe_df = pd.DataFrame(
            [pnl_percentage, pnl_percentage / len(self.res), annual_sharpe_ratio, max_drawdown_percentage,
             turnover.mean(axis=0).mean(), win_rate, avg_pnl_ratio],
            index=['annual_pnl_percentage', 'each_trade_pnl_percentage', 'annual_sharpe', 'max_drawdown_percentage',
                   'turnover', 'winrate', 'pnl_ratio'])

        if self.display:
            print(
                (
                    '%d 小时, %d 笔交易, %.3f USDT, PnL %.2f USDT = %.4f%%\n' \
                    '平均每秒 %.2f 笔交易, 平均每秒 %.2f 次更新, 平均每次换仓 PnL: %.7f%%\n' \
                    '最大多仓: %.3f, 最大空仓: %.3f, maker手续费损耗: %.2f, taker手续费损耗: %.2f\n' \
                    '最大总杠杆: %.2f,最小总杠杆: %.2f,最大多头风险头寸杠杆：%.2f, 最大空头风险头寸杠杆：%.2f \n' \
                    '最大回撤: %.3f%%,交易胜率: %.3f%%,平均盈亏比: %.3f%% 年化 PnL: %.2f%%\n' \
                    '月化交易量: %d USDT, 每小时总换手率: %.4f, 每小时各标的的平均换手率: %.4f, 年化夏普率: %.2f, 年化索提诺率: %.2f\n'
                )
                %
                (
                    time_span / pd.Timedelta(hours=1), n_trades, total_traded_quantity, profit, pnl_percentage,

                    n_trades * convert_to_per_second, len(self.res) * convert_to_per_second,
                    pnl_percentage / len(self.res),

                    self.res.loc[:, self.symbol_lis].sum(axis=1).max(),
                    self.res.loc[:, self.symbol_lis].sum(axis=1).min(),
                    maker_fee_cost, taker_fee_cost,

                    self.res.total_leverage.max(), self.res.total_leverage.min(), self.res.net_leverage.max(),
                    self.res.net_leverage.min() \
                        , max_drawdown_percentage, win_rate, avg_pnl_ratio,
                    pnl_percentage * float(pd.Timedelta(days=365) / time_span),

                    total_traded_quantity * float(pd.Timedelta(days=30) / time_span),
                    turnover.mean(axis=0).sum(), turnover.mean(axis=0).mean(), annual_sharpe_ratio, annual_sortino_ratio
                )
            )

            display(self.describe_df.T.style.background_gradient(cmap='coolwarm'))

        if self.plot:
            # plot
            fig = plt.figure(figsize=(20, 8))
            ax1 = fig.add_subplot(211)
            self.filled_price_df['BTCUSDT'].plot(ax=ax1, title='BTC Price Trend', legend=None)
            # plt.legend()

            ax2 = fig.add_subplot(212)
            self.res['equity'].plot(ax=ax2, title='strategy equity curve')

            # plt.legend()
            plt.show()




