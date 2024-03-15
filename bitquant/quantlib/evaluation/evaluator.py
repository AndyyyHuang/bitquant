import numpy as np
import numba as nb

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


class Evaluator:
    def evaluate(self, portfolio_weight_matrix: np.array, price_matrix: np.array, init_cash: float, order_size:float, taker_fee: float, volume_precision: np.array, min_notional: np.array):
        open_price_matrix = price_matrix[:, :, 0]
        low_price_matrix = price_matrix[:, :, 1]
        hight_price_matrix = price_matrix[:, :, 2]
        close_price_matrix = price_matrix[:, :, 3]
        vwap_price_matrix = price_matrix[:, :, 4]

        # check delta neutral requirement
        always_delta_neutral = np.apply_along_axis(self.check_delta_neutral, axis=1, arr=portfolio_weight_matrix).sum() == portfolio_weight_matrix.shape[0]

        if not always_delta_neutral:
            raise Exception("Error, not always delta neutral")

        self.res = _start_loop(portfolio_weight_matrix, open_price_matrix, init_cash, order_size, taker_fee, volume_precision, min_notional)
        final_position = self.res[-1, :-1]
        final_cash = self.res[-1, -1]
        latest_price = close_price_matrix[-1]
        final_asset_value = np.sum(final_position * latest_price) + final_cash
        profit_ratio = final_asset_value / init_cash - 1
        return profit_ratio

    def check_delta_neutral(self, portfolio_weight):
        return np.abs(np.sum(portfolio_weight)) < 1e-6

