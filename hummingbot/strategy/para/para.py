import asyncio
import logging
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Tuple
import math

import pandas as pd

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.derivative.position import Position
from hummingbot.core.clock import Clock
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.market_order import MarketOrder
from hummingbot.core.data_type.order_candidate import OrderCandidate, PerpetualOrderCandidate
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    OrderType,
    PositionSide,
    PositionAction,
    PositionMode,
    SellOrderCompletedEvent,
    OrderFilledEvent,
    BuyOrderCreatedEvent,
    SellOrderCreatedEvent,
    OrderCancelledEvent,
    TradeType,
    LimitOrderStatus
)
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.para.proposal import Proposal, ProposalSide
from hummingbot.strategy.strategy_py_base import StrategyPyBase

NaN = float("nan")
s_decimal_zero = Decimal(0)
spa_logger = None


class StrategyState(Enum):
    POSITIONS_MATCH = 0
    OPENING_LIMIT = 1
    OPENING_MARKET = 2
    CLOSING_LIMIT = 3
    CLOSING_MARKET = 4
    REACHED_TOTAL_AMOUNT = 5
    CLOSED_ALL = 6
    WAIT_TO_CANCEL_LIMIT = 7


def check_derivative(market_info):
    from hummingbot.client.settings import AllConnectorSettings
    return market_info.market.name in AllConnectorSettings.get_derivative_names()


class ParaStrategy(StrategyPyBase):
    @classmethod
    def logger(cls) -> HummingbotLogger:
        global spa_logger
        if spa_logger is None:
            spa_logger = logging.getLogger(__name__)
        return spa_logger

    def init_params(self,
                    short_info: MarketTradingPairTuple,
                    long_info: MarketTradingPairTuple,
                    short_order_type: OrderType,
                    total_amount: Decimal,
                    chunk_size: Decimal,
                    action_open: bool,
                    limit_slip: Decimal = Decimal("0"),
                    market_delta: Decimal = Decimal("0"),
                    status_report_interval: float = 10):
        self._short_info = short_info
        self._short_is_derivative = check_derivative(short_info)
        self._long_info = long_info
        self._long_is_derivative = check_derivative(long_info)
        self._short_order_type = short_order_type
        self._long_order_type = OrderType.MARKET if short_order_type == OrderType.LIMIT_MAKER else OrderType.LIMIT_MAKER
        self._total_amount = total_amount
        self._chunk_size = chunk_size
        self._position_action = PositionAction.OPEN if action_open else PositionAction.CLOSE
        self._limit_slip = limit_slip
        self._market_delta = market_delta

        self._all_markets_ready = False
        self._ev_loop = asyncio.get_event_loop()
        self._last_timestamp = 0
        self._status_report_interval = status_report_interval
        self.add_markets([short_info.market, long_info.market])

        self._main_task = None
        self._in_flight_opening_order_ids = []
        self._completed_opening_order_ids = set()
        self._completed_closing_order_ids = set()
        self._strategy_state = StrategyState.POSITIONS_MATCH
        self._ready_to_start = False
        self._last_reported_ts = 0
        self._leverage = 10
        self.wait_to_cancel = None
        self.wait_to_fill = None
        self.partially_filled = None
        self.partially_filled_amount = 0
        self._completed_amount = 0
        # short_info.market.set_leverage(short_info.trading_pair, self._perp_leverage)
        # long_info.market.set_leverage(long_info.trading_pair, self._perp_leverage)

    @property
    def strategy_state(self) -> StrategyState:
        return self._strategy_state

    @strategy_state.setter
    def strategy_state(self, value):
        self.logger().info(f"{self._completed_amount}/{self._total_amount} {self._strategy_state} -> {value}")
        self._strategy_state = value

    @property
    def market_info_to_active_orders(self) -> Dict[MarketTradingPairTuple, List[LimitOrder]]:
        return self._sb_order_tracker.market_pair_to_active_orders

    def positions(self, market_info) -> List[Position]:
        return [s for s in market_info.market.account_positions.values() if
                s.trading_pair == market_info.trading_pair and s.amount != s_decimal_zero]

    @property
    def short_positions(self) -> List[Position]:
        return self.positions(self._short_info) if self._short_is_derivative else []

    @property
    def long_positions(self) -> List[Position]:
        return self.positions(self._long_info) if self._long_is_derivative else []

    def tick(self, timestamp: float):
        """
        Clock tick entry point, is run every second (on normal tick setting).
        :param timestamp: current tick timestamp
        """
        if not self._all_markets_ready:
            self._all_markets_ready = all([market.ready for market in self.active_markets])
            if not self._all_markets_ready:
                return
            else:
                try:
                    self.logger().info("Markets are ready.")
                    self.logger().info("Trading started.")

                    if not self.check_budget_available():
                        self.logger().info("Trading not possible.")
                        return

                    if not self.positions_match():
                        self.logger().warning(f"Positions don't match\nSHORT: {self.short_positions}\nLONG:{self.long_positions}")
                        return

                    derivatives = []
                    if self._short_is_derivative:
                        derivatives.append((self._short_info, self.short_positions, PositionSide.SHORT))
                    if self._long_is_derivative:
                        derivatives.append((self._long_info, self.long_positions, PositionSide.LONG))

                    unmatched_positions = []
                    for market_info, positions, side in derivatives:
                        if market_info.market.name == 'huobi_perpetual':
                            # market_info.market.set_trading_pair_position_mode(
                            #     market_info.trading_pair,
                            #     PositionMode.ONEWAY)
                            pass
                        elif market_info.market.position_mode != PositionMode.ONEWAY or \
                                len(positions) > 1:
                            self.logger().info(
                                f"This strategy supports only Oneway position mode. Please update your position in {market_info}"
                                "mode before starting this strategy.")
                            return

                        self._ready_to_start = True

                        if len(positions) == 1:
                            adj_amount = market_info.market.quantize_order_amount(
                                market_info.trading_pair, self._total_amount)
                            position = positions[0]
                            self._ready_to_start = False
                            if abs(position.amount) == adj_amount:
                                self.logger().info(
                                    f"There is an existing {market_info.trading_pair} "
                                    f"{position.position_side.name} position.")
                                if self._position_action == PositionAction.OPEN and position.position_side == side:
                                    self.logger().info(
                                        "Bot is configured to OPEN position but an open position with the required amount"
                                        f" {adj_amount} already exists "
                                    )
                                elif self._position_action == PositionAction.CLOSE and position.position_side != side:
                                    self.logger().info(
                                        "Bot is configured to CLOSE position but the open position with the required amount"
                                        f" {adj_amount} is not {side}"
                                    )
                                else:
                                    self.strategy_state == StrategyState.POSITIONS_MATCH
                                    self._ready_to_start = True
                            else:
                                unmatched_positions.append(position)

                    if len(unmatched_positions) == 0:
                        pass
                    elif len(unmatched_positions) == 2:
                        if abs(unmatched_positions[0].amount) == abs(unmatched_positions[1].amount):
                            amount = abs(unmatched_positions[0].amount)
                            self.logger().info(
                                f"Detected matching open positions in both exchanges for {amount}")
                            if amount < self._total_amount:
                                self.logger().info(
                                    f"Continue until {self._total_amount} is reached")
                                self._ready_to_start = True
                                self._completed_amount = amount

                            else:
                                self.logger().info(
                                    f"This is more than total amount setting {self._total_amount}. Don't know what to do.")
                        else:
                            self.logger().info(
                                f"There are non-matching positions in the two exchanges. Don't know what to do. {unmatched_positions}")
                    else:
                        self.logger().info(
                            f"There are {len(unmatched_positions)} unmatched positions. Don't know what to do. {unmatched_positions}")
                except Exception:
                    self._all_markets_ready = False
                    raise

        if self._ready_to_start and (self._main_task is None or self._main_task.done()):
            self._main_task = safe_ensure_future(self.main(timestamp))

    async def main(self, timestamp):
        """
        The main procedure for the arbitrage strategy.
        """

        # if self._strategy_state == StrategyState.ERROR:
        #     return
        if self.strategy_state in (StrategyState.OPENING_MARKET, StrategyState.CLOSING_MARKET):
            self.check_market_order_complete()
            return
        if self.strategy_state in (StrategyState.OPENING_LIMIT, StrategyState.CLOSING_LIMIT):
            await self.keep_limit_order_up_to_date()
            return
        if self.strategy_state == StrategyState.WAIT_TO_CANCEL_LIMIT:
            self.check_limit_cancelled()
            return
        if self.strategy_state == StrategyState.REACHED_TOTAL_AMOUNT and self._position_action == PositionAction.OPEN:
            return
        if self.strategy_state == StrategyState.CLOSED_ALL and self._position_action == PositionAction.CLOSE:
            return

        proposal = await self.create_base_proposal()

        self.apply_slippage_buffers(proposal)

        if self.check_budget_constraint(proposal):
            self.execute_proposal(proposal)

    def get_limit_orders(self):
        market_info_to_active_orders = self.market_info_to_active_orders
        limit_side = self.executing_proposal.limit_side
        limit_orders = market_info_to_active_orders.get(limit_side.market_info, [])

        if len(limit_orders) == 0:
            limit_orders = list(filter(
                lambda l: l.status == LimitOrderStatus.OPEN,
                limit_side.market_info.market.limit_orders
            ))
            if len(limit_orders) > 0:
                self.logger().warn(f"Recovered {len(limit_orders)} limit orders")

        return limit_orders

    def check_limit_cancelled(self):
        limit_orders = self.get_limit_orders()
        if len(limit_orders) == 0:
            self.logger().info(f"No limit order found, assume {self.wait_to_cancel} was cancelled")
            self.process_cancel(self.wait_to_cancel)
            self.wait_to_cancel = None
        else:
            if self._last_reported_ts + 5 < self.current_timestamp:
                self.logger().info(f"Limit orders: {len(limit_orders)}. Waiting to cancel.")
                self._last_reported_ts = self.current_timestamp

    async def keep_limit_order_up_to_date(self):
        limit_side = self.executing_proposal.limit_side
        market_side = self.executing_proposal.market_side
        limit_orders = self.get_limit_orders()

        if len(limit_orders) == 0:
            self.logger().info("No limit order found to keep up to date")
            return

        elif len(limit_orders) > 1:
            raise RuntimeError(f"More than 1 limit orders: {len(limit_orders)} found: {limit_orders}")
        limit_order = limit_orders[0]

        trading_pair = market_side.market_info.trading_pair
        market_amount = market_side.market_info.market.quantize_order_amount(trading_pair, self.executing_proposal.order_amount)
        market_price = await market_side.market_info.market.get_order_price(
            trading_pair, market_side.is_buy, market_amount)

        diff = (market_price - market_side.order_price) / market_side.order_price
        order_id = limit_order.client_order_id
        if abs(diff) > self._market_delta:
            self.logger().info(f"Market price {market_price:.5f} differs from proposal market price {market_side.order_price:.5f} by {100*diff:.2f}%, cancel limit order")
            self.cancel_order(
                market_trading_pair_tuple=limit_side.market_info,
                order_id=order_id)
            self.wait_to_cancel = order_id
            self.strategy_state = StrategyState.WAIT_TO_CANCEL_LIMIT
            # Now we wait for the cancel complete callback to fire
        else:
            if self._last_reported_ts + 5 < self.current_timestamp:
                self.logger().info(f"Limit order {limit_order} is up to date: diff percent {100*diff:.2f}%. Waiting to fill.")
                self._last_reported_ts = self.current_timestamp

    def positions_match(self):
        if not (self._short_is_derivative and self._long_is_derivative):
            return True

        short_positions, long_positions = self.short_positions, self.long_positions
        if len(short_positions) == 0 and len(long_positions) == 0:
            return True

        if len(short_positions) != 1:
            self.logger().warning(f"Expected 1 position in short exhange: {short_positions}")
            return False

        if len(long_positions) != 1:
            self.logger().warning(f"Expected 1 position in long exhange: {long_positions}")
            return False

        if math.isclose(abs(short_positions[0].amount), abs(long_positions[0].amount)):
            return True
            # TODO also check other position properties
        else:
            return False

    def check_market_order_complete(self):
        if self.strategy_state == StrategyState.OPENING_MARKET and len(self._completed_opening_order_ids) == 2:
            if not self.positions_match():
                self.logger().warning(f"Positions don't match\nSHORT: {self.short_positions}\nLONG:{self.long_positions}")
                return

            self._completed_opening_order_ids.clear()
            if self._short_is_derivative:
                market_info = self._short_info
            elif self._long_is_derivative:
                market_info = self._long_info
            else:
                self.logger().warning("Expected either short or long to be derivative. Now we can't check if we have reached total amount. Assume we did.")
                self.strategy_state = StrategyState.REACHED_TOTAL_AMOUNT
                return

            total_amount = market_info.market.quantize_order_amount(
                market_info.trading_pair, self._total_amount)

            self._completed_amount = abs(self.positions(market_info)[0].amount)
            if self._completed_amount == total_amount:
                self.logger().info(f"Reached total amount {self._total_amount}. Done")

                self.strategy_state = StrategyState.REACHED_TOTAL_AMOUNT
            else:
                self.strategy_state = StrategyState.POSITIONS_MATCH

        elif self.strategy_state == StrategyState.CLOSING_MARKET and len(self._completed_closing_order_ids) == 2:
            if not self.positions_match():
                self.logger().warning(f"Positions don't match\nSHORT: {self.short_positions}\nLONG:{self.long_positions}")
                return
            self._completed_closing_order_ids.clear()
            self._completed_amount += self.executing_proposal.order_amount
            if self._short_is_derivative:
                market_info = self._short_info
            elif self._long_is_derivative:
                market_info = self._long_info
            else:
                self.logger().warning("Expected either short or long to be derivative. Now we can't check if we have closed all. Assume we did.")
                self.strategy_state = StrategyState.CLOSED_ALL
                return

            if len(self.positions(market_info)) == 0:
                self.logger().info("Closed all positions. Done")
                self.strategy_state = StrategyState.CLOSED_ALL
            else:
                self.strategy_state = StrategyState.POSITIONS_MATCH

    async def create_base_proposal(self) -> List[Proposal]:
        trading_pair = self._short_info.trading_pair
        chunk_size = self._chunk_size
        short_amount = self._short_info.market.quantize_order_amount(trading_pair, chunk_size)
        long_amount = self._long_info.market.quantize_order_amount(trading_pair, chunk_size)
        tasks = [
            self._short_info.market.get_order_price(
                trading_pair, True, short_amount),
            self._short_info.market.get_order_price(
                trading_pair, False, short_amount),
            self._long_info.market.get_order_price(
                trading_pair, True, long_amount),
            self._long_info.market.get_order_price(
                trading_pair, False, long_amount)
        ]
        prices = await safe_gather(*tasks)
        short_buy, short_sell, long_buy, long_sell = [*prices]

        short_is_buy = self._position_action == PositionAction.CLOSE
        long_is_buy = not short_is_buy
        short_price, long_price = (short_buy, long_sell) if short_is_buy else (short_sell, long_buy)
        if self._short_order_type == OrderType.LIMIT_MAKER:
            # We can choose to replace short price to be closer to long price (taker price)
            fn = min if short_is_buy else max
            short_price = fn(short_price, long_price)
        else:
            # We can choose to replace long price to be closer to short price (taker price)
            fn = min if long_is_buy else max
            long_price = fn(short_price, long_price)

        proposal = Proposal(
            ProposalSide(self._short_info, short_is_buy, short_price, self._short_order_type, self._short_is_derivative),
            ProposalSide(self._long_info, long_is_buy, long_price, self._long_order_type, self._long_is_derivative),
            chunk_size)
        self.logger().debug(f"Make proposal {proposal} because short price is {short_price}, long price is {long_price}")
        return proposal

    def apply_slippage_buffer(self, order_price, is_buy, market_info):
        s_buffer = self._limit_slip
        old_price = order_price
        if not is_buy:
            s_buffer *= Decimal("-1")
        order_price *= Decimal("1") + s_buffer
        order_price = market_info.market.quantize_order_price(market_info.trading_pair, order_price)

        buy_or_sell = 'BUY' if is_buy else 'SELL'
        self.logger().debug(f"{market_info.market.display_name.capitalize()}:Applied slippage {self._limit_slip} to {buy_or_sell} price {old_price}: {order_price}")
        return order_price

    def apply_slippage_buffers(self, proposal: Proposal):
        """
        Updates arb_proposals by adjusting order price for slipper buffer percentage.
        E.g. if it is a buy order, for an order price of 100 and 1% slipper buffer, the new order price is 101,
        for a sell order, the new order price is 99.
        :param proposal: the arbitrage proposal
        """
        side = proposal.limit_side
        side.order_price = self.apply_slippage_buffer(
            side.order_price,
            side.is_buy,
            side.market_info)

    def check_budget_available(self) -> bool:
        """
        Checks if there's any balance for trading to be possible at all
        :return: True if user has available balance enough for orders submission.
        """

        for perp_market_info in [self._short_info, self._long_info]:
            perp_base, perp_quote = perp_market_info.trading_pair.split("-")

            balance_perp_quote = perp_market_info.market.get_available_balance(perp_quote)

            if balance_perp_quote == s_decimal_zero:
                self.logger().info(
                    f"Cannot trade, {perp_market_info.market.display_name} {perp_quote} balance "
                    f"({balance_perp_quote}) is 0.")
                return False

        return True

    def check_budget_constraint(self, proposal: Proposal) -> bool:
        """
        Check balances on both exchanges if there is enough to submit both orders in a proposal.
        :param proposal: An arbitrage proposal
        :return: True if user has available balance enough for both orders submission.
        """
        return self.check_spot_budget_constraint(proposal) and self.check_perpetual_budget_constraint(proposal)

    def check_spot_budget_constraint(self, proposal: Proposal) -> bool:
        for proposal_side in [proposal.buy_side, proposal.sell_side]:
            if proposal_side.is_derivative:
                continue
            order_amount = proposal.order_amount
            market_info = proposal_side.market_info
            budget_checker = market_info.market.budget_checker
            order_candidate = OrderCandidate(
                trading_pair=market_info.trading_pair,
                is_maker=False,
                order_type=OrderType.LIMIT,
                order_side=TradeType.BUY if proposal_side.is_buy else TradeType.SELL,
                amount=order_amount,
                price=proposal_side.order_price,
            )

            adjusted_candidate_order = budget_checker.adjust_candidate(order_candidate, all_or_none=True)

            if adjusted_candidate_order.amount < order_amount:
                self.logger().info(
                    f"Cannot trade, {proposal_side.market_info.market.display_name} balance"
                    f" is insufficient to place the order candidate {order_candidate}."
                )
                return False

        return True

    def check_perpetual_budget_constraint(self, proposal: Proposal) -> bool:
        """
        Check balance on spot exchange.
        :param proposal: An arbitrage proposal
        :return: True if user has available balance enough for both orders submission.
        """
        for proposal_side in [proposal.buy_side, proposal.sell_side]:
            if not proposal_side.is_derivative:
                continue
            order_amount = proposal.order_amount
            market_info = proposal_side.market_info
            budget_checker = market_info.market.budget_checker

            position_close = self._position_action == PositionAction.CLOSE

            order_candidate = PerpetualOrderCandidate(
                trading_pair=market_info.trading_pair,
                is_maker=False,
                order_type=proposal_side.order_type,
                order_side=TradeType.BUY if proposal_side.is_buy else TradeType.SELL,
                amount=order_amount,
                price=proposal_side.order_price,
                leverage=Decimal(self._leverage),
                position_close=position_close,
            )

            adjusted_candidate_order = budget_checker.adjust_candidate(order_candidate, all_or_none=True)

            if adjusted_candidate_order.amount < order_amount:
                self.logger().info(
                    f"Cannot trade, {proposal_side.market_info.market.display_name} balance"
                    f" is insufficient to place the order candidate {order_candidate}."
                )
                return False

        return True

    def execute_proposal(self, proposal: Proposal):
        """
        Execute limit on one side of proposal, after fill execute market on the other
        :param proposal: the proposal
        """
        if proposal.order_amount == s_decimal_zero:
            return
        if self.strategy_state == StrategyState.POSITIONS_MATCH:
            execute_side = proposal.limit_side
            position_action = self._position_action if execute_side.is_derivative else None
            if self._position_action == PositionAction.OPEN:
                next_state = StrategyState.OPENING_LIMIT
            else:
                next_state = StrategyState.CLOSING_LIMIT

        elif self.strategy_state in [StrategyState.OPENING_LIMIT, StrategyState.CLOSING_LIMIT]:
            execute_side = proposal.market_side
            position_action = self._position_action if execute_side.is_derivative else None
            if self._position_action == PositionAction.OPEN:
                next_state = StrategyState.OPENING_MARKET
            else:
                next_state = StrategyState.CLOSING_MARKET
        else:
            raise RuntimeError(f"Unexpected state for execute_propsal: {self.strategy_state}")

        order_fn = self.buy_with_specific_market if execute_side.is_buy else self.sell_with_specific_market
        side = "BUY" if execute_side.is_buy else "SELL"
        self.log_with_clock(
            logging.INFO,
            f"Placing {side} {execute_side.order_type} {position_action} order for {proposal.order_amount} {execute_side.market_info.base_asset} "
            f"at {execute_side.market_info.market.display_name} at {execute_side.order_price} price"
        )
        order_fn(
            market_trading_pair_tuple=execute_side.market_info,
            amount=proposal.order_amount,
            order_type=execute_side.order_type,
            price=execute_side.order_price,
            position_action=position_action,
        )
        self.strategy_state = next_state
        if next_state == StrategyState.CLOSING_LIMIT:
            self._completed_closing_order_ids.clear()
            # Now wait for limit order to fill to execute sell part of proposal
        elif next_state == StrategyState.OPENING_LIMIT:
            self._completed_opening_order_ids.clear()
            # Now wait for limit order to fill to execute sell part of proposal

        self.executing_proposal = proposal

    def active_positions_df(self) -> pd.DataFrame:
        """
        Returns a new dataframe on current active perpetual positions.
        """
        columns = ["Symbol", "Type", "Entry Price", "Amount", "Leverage", "Unrealized PnL"]
        data = []
        if self._short_is_derivative:
            for pos in self.short_positions:
                data.append([
                    pos.trading_pair,
                    str(pos.position_side),
                    round(pos.entry_price, 6),
                    pos.amount,
                    pos.leverage,
                    pos.unrealized_pnl
                ])
        if self._long_is_derivative:
            for pos in self.long_positions:
                data.append([
                    pos.trading_pair,
                    str(pos.position_side),
                    pos.entry_price,
                    pos.amount,
                    pos.leverage,
                    pos.unrealized_pnl
                ])

        return pd.DataFrame(data=data, columns=columns)

    async def format_status(self) -> str:
        """
        Returns a status string formatted to display nicely on terminal. The strings composes of 4 parts: markets,
        assets, spread and warnings(if any).
        """
        columns = ["Exchange", "Market", "Sell Price", "Buy Price", "Mid Price"]
        data = []
        not_ready = {}
        for market_info in [self._short_info, self._long_info]:
            market, trading_pair, base_asset, quote_asset = market_info
            buy_price = await market.get_quote_price(trading_pair, True, self._chunk_size)
            sell_price = await market.get_quote_price(trading_pair, False, self._chunk_size)
            mid_price = (buy_price + sell_price) / 2
            data.append([
                market.display_name,
                trading_pair,
                float(sell_price),
                float(buy_price),
                float(mid_price)
            ])
            if not market.ready:
                not_ready[market.name] = market.status_dict
        markets_df = pd.DataFrame(data=data, columns=columns)
        lines = []
        lines.extend(["", "  Markets:"] + ["    " + line for line in markets_df.to_string(index=False).split("\n")])

        # See if there're any active positions.
        if len(self.short_positions) > 0 or len(self.long_positions) > 0:
            df = self.active_positions_df()
            lines.extend(["", "  Positions:"] + ["    " + line for line in df.to_string(index=False).split("\n")])
        else:
            lines.extend(["", "  No active positions."])

        assets_df = self.wallet_balance_data_frame([self._short_info, self._long_info])
        lines.extend(["", "  Assets:"] +
                     ["    " + line for line in str(assets_df).split("\n")])

        proposal = await self.create_base_proposal()
        if proposal:
            lines.extend(["", "  Opportunity:"] + self.short_proposal_msg(proposal))

        warning_lines = self.network_warning([self._short_info])
        warning_lines.extend(self.network_warning([self._long_info]))
        warning_lines.extend(self.balance_warning([self._short_info]))
        warning_lines.extend(self.balance_warning([self._long_info]))
        if len(warning_lines) > 0:
            lines.extend(["", "*** WARNINGS ***"] + warning_lines)

        if not_ready:
            lines.append(f"Not ready: {not_ready}")

        lines.append(f"Strategy State: {self.strategy_state}")
        return "\n".join(lines)

    def short_proposal_msg(self, proposal: Proposal, indented: bool = True) -> List[str]:
        """
        Composes a short proposal message.
        :param arb_proposal: The arbitrage proposal
        :param indented: If the message should be indented (by 4 spaces)
        :return A list of messages
        """
        lines = []
        buy_side = "buy " + proposal.buy_side.market_info.market.display_name
        sell_side = "sell " + proposal.sell_side.market_info.market.display_name
        lines.append(f"{'    ' if indented else ''}{buy_side} at {proposal.buy_side.order_price}"
                     f", {sell_side} at {proposal.sell_side.order_price}: ")
        return lines

    @property
    def tracked_market_orders(self) -> List[Tuple[ConnectorBase, MarketOrder]]:
        return self._sb_order_tracker.tracked_market_orders

    @property
    def tracked_limit_orders(self) -> List[Tuple[ConnectorBase, LimitOrder]]:
        return self._sb_order_tracker.tracked_limit_orders

    def start(self, clock: Clock, timestamp: float):
        self._ready_to_start = False

    def stop(self, clock: Clock):
        if self._main_task is not None:
            self._main_task.cancel()
            self._main_task = None
        self._ready_to_start = False

    def did_fill_order(self, event: OrderFilledEvent):
        if self.partially_filled == event.order_id:
            self.partially_filled_amount += event.amount
        else:
            self.partially_filled = event.order_id
            self.partially_filled_amount = event.amount
        self.logger().info(f"Partially filled order {self.partially_filled} for total {self.partially_filled_amount}/{self._chunk_size}")

    def process_completed_order(self, order_id):
        if self.wait_to_fill != order_id:
            self.logger().warn(f"Unexpected order {order_id} got filled while waiting on {self.wait_to_fill}. Ignore")
            return
        self.update_complete_order_id_lists(order_id)
        if self.strategy_state in [StrategyState.OPENING_LIMIT, StrategyState.CLOSING_LIMIT]:
            # Execute second part of proposal
            self.execute_proposal(self.executing_proposal)
        elif self.strategy_state in [StrategyState.OPENING_MARKET, StrategyState.CLOSING_MARKET]:
            self.logger().warn(f"Market order {order_id} completed")
        else:
            self.logger().warn(f"Unexpected state {self.strategy_state} on completed order.")

    def did_complete_buy_order(self, event: BuyOrderCompletedEvent):
        self.process_completed_order(event.order_id)

    def did_complete_sell_order(self, event: SellOrderCompletedEvent):
        self.process_completed_order(event.order_id)

    def process_cancel(self, order_id):
        if self.partially_filled == order_id:
            # If any of the cancelled order got filled, process it as if the partially filled amount was a complete order
            # That is, hedge it in Market side.
            self.executing_proposal.order_amount = self.partially_filled_amount
            self.process_completed_order(order_id)
        else:
            self.strategy_state = StrategyState.POSITIONS_MATCH

    def did_cancel_order(self, event: OrderCancelledEvent):
        if event.order_id not in [self.wait_to_cancel, self.wait_to_fill]:
            self.logger().info(f"Canceled event {event} does not match {self.wait_to_cancel}")
            return
        if self.strategy_state in [StrategyState.OPENING_LIMIT, StrategyState.CLOSING_LIMIT, StrategyState.WAIT_TO_CANCEL_LIMIT]:
            self.process_cancel(event.order_id)
        else:
            self.logger().warn(f"Unexpected state {self.strategy_state} when order got canceled. Ignore if this is cleaning of standing orders at start/end")

    def did_create_buy_order(self, event: BuyOrderCreatedEvent):
        self.logger().debug(f"Created {event.type} BUY order {event.order_id}")
        self.wait_to_fill = event.order_id

    def did_create_sell_order(self, event: SellOrderCreatedEvent):
        self.logger().debug(f"Created {event.type} SELL order {event.order_id}")
        self.wait_to_fill = event.order_id

    def update_complete_order_id_lists(self, order_id: str):
        if self.strategy_state in [StrategyState.OPENING_LIMIT, StrategyState.OPENING_MARKET]:
            self._completed_opening_order_ids.add(order_id)
        elif self.strategy_state in [StrategyState.CLOSING_LIMIT, StrategyState.CLOSING_MARKET]:
            self._completed_closing_order_ids.add(order_id)
