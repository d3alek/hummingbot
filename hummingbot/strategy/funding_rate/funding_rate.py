import asyncio
import logging
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Tuple

import pandas as pd

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.derivative.position import Position
from hummingbot.core.clock import Clock
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.market_order import MarketOrder
from hummingbot.core.data_type.order_candidate import PerpetualOrderCandidate
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    OrderType,
    PositionAction,
    PositionMode,
    SellOrderCompletedEvent,
    OrderFilledEvent,
    # BuyOrderEventCreated,
    # SellOrderEventCreated,
    TradeType
)
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.funding_rate.proposal import Proposal, ProposalSide
from hummingbot.strategy.strategy_py_base import StrategyPyBase

NaN = float("nan")
s_decimal_zero = Decimal(0)
spa_logger = None


class StrategyState(Enum):
    Closed = 0
    Opening1Limit = 1
    Opening2Market = 2
    Opened = 3
    Closing1Limit = 4
    Closing2Market = 5


class FundingRateStrategy(StrategyPyBase):
    """
    This strategy arbitrages between a spot and a perpetual exchange.
    For a given order amount, the strategy checks for price discrepancy between buy and sell price on the 2 exchanges.
    Since perpetual contract requires closing position before profit is realised, there are 2 stages to this arbitrage
    operation - first to open and second to close.
    """

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global spa_logger
        if spa_logger is None:
            spa_logger = logging.getLogger(__name__)
        return spa_logger

    def init_params(self,
                    perp_market1_info: MarketTradingPairTuple,
                    perp_market2_info: MarketTradingPairTuple,
                    order_amount: Decimal,
                    perp_leverage: int,
                    min_opening_funding_rate_pct: Decimal,
                    min_closing_funding_rate_pct: Decimal,
                    perp_market_slippage_buffer: Decimal = Decimal("0"),
                    next_opening_delay: float = 120,
                    next_closing_delay: float = 120,
                    status_report_interval: float = 10):
        """
        :param spot_market_info: The spot market info
        :param perp_market_info: The perpetual market info
        :param order_amount: The order amount
        :param perp_leverage: The leverage level to use on perpetual market
        :param min_opening_funding_rate_pct: The minimum spread to open arbitrage position (e.g. 0.0003 for 0.3%)
        :param min_closing_funding_rate_pct: The minimum spread to close arbitrage position (e.g. 0.0003 for 0.3%)
        :param spot_market_slippage_buffer: The buffer for which to adjust order price for higher chance of
        the order getting filled on spot market.
        :param perp_market_slippage_buffer: The slipper buffer for perpetual market.
        :param next_arbitrage_opening_delay: The number of seconds to delay before the next arb position can be opened
        :param status_report_interval: Amount of seconds to wait to refresh the status report
        """
        self._perp_market1_info = perp_market1_info
        self._perp_market2_info = perp_market2_info
        self._min_opening_funding_rate_pct = min_opening_funding_rate_pct
        self._min_closing_funding_rate_pct = min_closing_funding_rate_pct
        self._order_amount = order_amount
        self._perp_leverage = perp_leverage
        self._perp_market_slippage_buffer = perp_market_slippage_buffer
        self._next_opening_delay = next_opening_delay
        self._next_closing_delay = next_closing_delay
        self._next_opening_ts = 0  # next arbitrage opening timestamp
        self._next_closing_ts = 0  # next arbitrage closing timestamp
        self._all_markets_ready = False
        self._ev_loop = asyncio.get_event_loop()
        self._last_timestamp = 0
        self._status_report_interval = status_report_interval
        self.add_markets([perp_market1_info.market, perp_market2_info.market])

        self._main_task = None
        self._in_flight_opening_order_ids = []
        self._completed_opening_order_ids = []
        self._completed_closing_order_ids = []
        self._strategy_state = StrategyState.Closed
        self._ready_to_start = False
        self._last_arb_op_reported_ts = 0
        perp_market1_info.market.set_leverage(perp_market1_info.trading_pair, self._perp_leverage)
        perp_market2_info.market.set_leverage(perp_market2_info.trading_pair, self._perp_leverage)

        self.market1_order_type = OrderType.MARKET

    @property
    def strategy_state(self) -> StrategyState:
        return self._strategy_state

    @property
    def min_opening_funding_rate_pct(self) -> Decimal:
        return self._min_opening_funding_rate_pct

    @property
    def min_closing_funding_rate_pct(self) -> Decimal:
        return self._min_closing_funding_rate_pct

    @property
    def order_amount(self) -> Decimal:
        return self._order_amount

    @order_amount.setter
    def order_amount(self, value):
        self._order_amount = value

    @property
    def market_info_to_active_orders(self) -> Dict[MarketTradingPairTuple, List[LimitOrder]]:
        return self._sb_order_tracker.market_pair_to_active_orders

    def perp_positions(self, market_info) -> List[Position]:
        return [s for s in market_info.market.account_positions.values() if
                s.trading_pair == market_info.trading_pair and s.amount != s_decimal_zero]

    @property
    def perp1_positions(self) -> List[Position]:
        return self.perp_positions(self._perp_market1_info)

    @property
    def perp2_positions(self) -> List[Position]:
        return self.perp_positions(self._perp_market2_info)

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
                self.logger().info("Markets are ready.")
                self.logger().info("Trading started.")

                if not self.check_budget_available():
                    self.logger().info("Trading not possible.")
                    return

                for perp_market_info, perp_positions in [(self._perp_market1_info, self.perp1_positions),
                                                         (self._perp_market2_info, self.perp2_positions)]:
                    if perp_market_info.market.position_mode != PositionMode.ONEWAY or \
                            len(perp_positions) > 1:
                        self.logger().info(
                            f"This strategy supports only Oneway position mode. Please update your position in {perp_market_info}"
                            "mode before starting this strategy.")
                        return

                    self._ready_to_start = True

                    if len(perp_positions) == 1:
                        adj_perp_amount = perp_market_info.market.quantize_order_amount(
                            perp_market_info.trading_pair, self._order_amount)
                        if abs(perp_positions[0].amount) == adj_perp_amount:
                            self.logger().info(
                                f"There is an existing {perp_market_info.trading_pair} "
                                f"{perp_positions[0].position_side.name} position. The bot resumes "
                                f"operation to close out the arbitrage position")
                            self._strategy_state = StrategyState.Opened
                            # TODO check it also exists in perp2_positions
                        else:
                            self.logger().info(
                                f"There is an existing {perp_market_info.trading_pair} "
                                f"{perp_positions[0].position_side.name} position with unmatched "
                                f"position amount. Please manually close out the position before starting "
                                f"this strategy.")
                            self._ready_to_start = False
                            return

        if self._ready_to_start and (self._main_task is None or self._main_task.done()):
            self._main_task = safe_ensure_future(self.main(timestamp))

    async def main(self, timestamp):
        """
        The main procedure for the arbitrage strategy.
        """
        self.update_strategy_state()
        if self._strategy_state in (StrategyState.Opening2Market, StrategyState.Closing2Market):
            return
        if self._strategy_state in (StrategyState.Opening1Limit, StrategyState.Closing1Limit):
            # TODO what if funding rates no longer are good for us?
            await self.keep_limit_order_up_to_date()
            return
        if self.strategy_state == StrategyState.Opened and self._next_closing_ts > self.current_timestamp:
            return
        if self.strategy_state == StrategyState.Closed and self._next_opening_ts > self.current_timestamp:
            return
        proposals = await self.create_base_proposals()
        if self._strategy_state == StrategyState.Opened:
            perp1_is_buy = False if self.perp1_positions[0].amount > 0 else True
            # TODO p.perp1_side
            proposals = [p for p in proposals if p.perp1_side.is_buy == perp1_is_buy and p.profit_pct() >=
                         self._min_closing_funding_rate_pct]
        else:
            proposals = [p for p in proposals if p.profit_pct() >= self._min_opening_funding_rate_pct]
        if len(proposals) == 0:
            return
        proposal = proposals[0]
        if self._last_arb_op_reported_ts + 60 < self.current_timestamp:
            pos_txt = "closing" if self._strategy_state == StrategyState.Opened else "opening"
            self.logger().info(f"Finding rate position {pos_txt} opportunity found.")
            self.logger().info(f"Profitability ({proposal.profit_pct():.2%}) is now above min_{pos_txt}_pct.")
            self._last_arb_op_reported_ts = self.current_timestamp
        self.apply_slippage_buffers(proposal)
        if self.check_budget_constraint(proposal):
            self.execute_proposal(proposal)

    async def keep_limit_order_up_to_date(self):
        market_info_to_active_orders = self.market_info_to_active_orders
        limit_side = self.executing_proposal.maker_side
        limit_orders = market_info_to_active_orders.get(limit_side.market_info, [])
        market_side = self.executing_proposal.taker_side

        if len(limit_orders) == 0:
            self.logger().info("No limit order found to keep up to date")
            return
        elif len(limit_orders) > 1:
            raise RuntimeError(f"More than 1 limit orders: {len(limit_orders)} found: {limit_orders}")

        limit_order = limit_orders[0]

        market_price = await market_side.market_info.market.get_order_price(
            market_side.market_info.trading_pair, market_side.is_buy,
            self.executing_proposal.order_amount)

        diff = (market_price - market_side.order_price) / market_side.order_price
        diff_percent = 100 * diff
        if abs(diff_percent) > 0.02:
            self.logger().info(f"Market price {market_price:.5f} differs from proposal market price {market_side.order_price:.5f} by {diff_percent:.2f}, cancel limit order")
            self.cancel_order(
                market_trading_pair_tuple=limit_side.market_info,
                order_id=limit_order.client_order_id)

            if self._strategy_state == StrategyState.Opening1Limit:
                self._strategy_state = StrategyState.Closed
            elif self._strategy_state == StrategyState.Closing1Limit:
                self._strategy_state = StrategyState.Opened
            else:
                raise RuntimeError(f"Unexpected state {self._strategy_state}")
        else:
            if self._last_arb_op_reported_ts + 5 < self.current_timestamp:
                self.logger().info(f"Limit order {limit_order} is up to date: diff percent {diff_percent:.2f}. Waiting to fill.")
                self._last_arb_op_reported_ts = self.current_timestamp

    def update_strategy_state(self):
        """
        Updates strategy state to either Opened or Closed if the condition is right.
        """
        if self._strategy_state == StrategyState.Opening2Market and len(self._completed_opening_order_ids) == 2 and \
                self.perp1_positions and self.perp2_positions:
            self._strategy_state = StrategyState.Opened
            self._completed_opening_order_ids.clear()
            self._next_closing_ts = self.current_timestamp + self._next_closing_delay
        elif self._strategy_state == StrategyState.Closing2Market and len(self._completed_closing_order_ids) == 2 and \
                len(self.perp1_positions) == 0 and len(self.perp2_positions) == 0:
            self._strategy_state = StrategyState.Closed
            self._completed_closing_order_ids.clear()
            self._next_opening_ts = self.current_timestamp + self._next_opening_delay

    async def create_base_proposals(self) -> List[Proposal]:
        """
        Creates a list of 2 base proposals, no filter.
        :return: A list of 2 base proposals.
        """
        tasks = [
            self._perp_market1_info.market.get_order_price(
                self._perp_market1_info.trading_pair, True,
                self._order_amount),
            self._perp_market1_info.market.get_order_price(
                self._perp_market1_info.trading_pair, False,
                self._order_amount),
            self._perp_market2_info.market.get_order_price(
                self._perp_market2_info.trading_pair, True,
                self._order_amount),
            self._perp_market2_info.market.get_order_price(
                self._perp_market2_info.trading_pair, False,
                self._order_amount)
        ]
        prices = await safe_gather(*tasks, return_exceptions=True)
        perp1_buy, perp1_sell, perp2_buy, perp2_sell = [*prices]
        funding_info1 = self._perp_market1_info.market.get_funding_info(self._perp_market1_info.trading_pair)
        funding_info2 = self._perp_market2_info.market.get_funding_info(self._perp_market2_info.trading_pair)
        if not funding_info1:
            self.logger().error("Funding Info 1 is None")
            return []
        if not funding_info2:
            self.logger().error("Funding Info 2 is None")
            return []

        market1_order_type = self.market1_order_type
        if market1_order_type == OrderType.MARKET:
            market2_order_type = OrderType.LIMIT
            perp2_sell = perp1_buy
            perp2_buy = perp1_sell
        else:
            market2_order_type = OrderType.MARKET
            perp1_buy = perp2_sell
            perp1_sell = perp2_buy
        return [
            Proposal(
                ProposalSide(self._perp_market1_info, True, perp1_buy, funding_info1, market1_order_type),
                ProposalSide(self._perp_market2_info, False, perp2_sell, funding_info2, market2_order_type),
                self._order_amount),
            # Proposal(
            # ProposalSide(self._perp_market1_info, False, perp1_sell, funding_info1, market1_order_type),
            # ProposalSide(self._perp_market2_info, True, perp2_buy, funding_info2, market2_order_type),
            # self._order_amount)
        ]

    def apply_slippage_buffer(self, order_price, is_buy, market_info):
        s_buffer = self._perp_market_slippage_buffer
        old_price = order_price
        if not is_buy:
            s_buffer *= Decimal("-1")
        order_price *= Decimal("1") + s_buffer
        order_price = market_info.market.quantize_order_price(market_info.trading_pair, order_price)

        buy_or_sell = 'BUY' if is_buy else 'SELL'
        self.logger().info(f"{market_info.market.display_name.capitalize()}:Applied slippage {self._perp_market_slippage_buffer} to {buy_or_sell} price {old_price}: {order_price}")
        return order_price

    def apply_slippage_buffers(self, proposal: Proposal):
        """
        Updates arb_proposals by adjusting order price for slipper buffer percentage.
        E.g. if it is a buy order, for an order price of 100 and 1% slipper buffer, the new order price is 101,
        for a sell order, the new order price is 99.
        :param proposal: the arbitrage proposal
        """
        for arb_side in [proposal.maker_side]:
            arb_side.order_price = self.apply_slippage_buffer(
                arb_side.order_price,
                arb_side.is_buy,
                arb_side.market_info)

    def check_budget_available(self) -> bool:
        """
        Checks if there's any balance for trading to be possible at all
        :return: True if user has available balance enough for orders submission.
        """

        for perp_market_info in [self._perp_market1_info, self._perp_market2_info]:
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
        return self.check_perpetual_budget_constraint(proposal)

    def check_perpetual_budget_constraint(self, proposal: Proposal) -> bool:
        """
        Check balance on spot exchange.
        :param proposal: An arbitrage proposal
        :return: True if user has available balance enough for both orders submission.
        """
        for proposal_side in [proposal.buy_side, proposal.sell_side]:
            order_amount = proposal.order_amount
            market_info = proposal_side.market_info
            budget_checker = market_info.market.budget_checker

            position_close = False
            perp_positions = self.perp_positions(market_info)
            if perp_positions and abs(perp_positions[0].amount) == order_amount:
                cur_perp_pos_is_buy = True if perp_positions[0].amount > 0 else False
                if proposal_side.is_buy != cur_perp_pos_is_buy:
                    position_close = True
                else:
                    self.logger().error(f"Current perp position is buy {cur_perp_pos_is_buy} but proposal is also buy {proposal_side.is_buy}")
                    return False

            order_candidate = PerpetualOrderCandidate(
                trading_pair=market_info.trading_pair,
                is_maker=False,
                order_type=proposal_side.order_type,
                order_side=TradeType.BUY if proposal_side.is_buy else TradeType.SELL,
                amount=order_amount,
                price=proposal_side.order_price,
                leverage=Decimal(self._perp_leverage),
                position_close=position_close,
            )

            adjusted_candidate_order = budget_checker.adjust_candidate(order_candidate, all_or_none=True)

            if adjusted_candidate_order.amount < order_amount:
                self.logger().info(
                    f"Cannot arbitrage, {proposal_side.market_info.market.display_name} balance"
                    f" is insufficient to place the order candidate {order_candidate}."
                )
                return False

        return True

    def execute_proposal(self, proposal: Proposal):
        """
        Execute limit on one side of proposal, after full execute market on the other
        :param proposal: the proposal
        """
        if proposal.order_amount == s_decimal_zero:
            return
        if self._strategy_state in [StrategyState.Closed, StrategyState.Opened]:
            execute_side = proposal.maker_side
            position_action = PositionAction.CLOSE if self._strategy_state == StrategyState.Opened else PositionAction.OPEN

        elif self._strategy_state in [StrategyState.Opening1Limit, StrategyState.Closing1Limit]:
            execute_side = proposal.taker_side
            position_action = PositionAction.CLOSE if self._strategy_state == StrategyState.Closing1Limit else PositionAction.OPEN
        else:
            raise RuntimeError("Unexpected state for execute_propsal:", self._strategy_state)

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
        )

        if self._strategy_state == StrategyState.Opened:
            self._strategy_state = StrategyState.Closing1Limit
            self._completed_closing_order_ids.clear()
            # Now wait for limit order to fill to execute sell part of proposal
        elif self._strategy_state == StrategyState.Closing1Limit:
            self._strategy_state = StrategyState.Closing2Market
            # Now wait for market order to fill to finish the proposal
        elif self._strategy_state == StrategyState.Closed:
            self._strategy_state = StrategyState.Opening1Limit
            self._completed_opening_order_ids.clear()
            # Now wait for limit order to fill to execute sell part of proposal
        elif self._strategy_state == StrategyState.Opening1Limit:
            self._strategy_state = StrategyState.Opening2Market
            # Now wait for market order to fill to finish the proposal
        else:
            raise RuntimeError("Unexpected state for execute_propsal:", self._strategy_state)

        self.executing_proposal = proposal

    def active_positions_df(self) -> pd.DataFrame:
        """
        Returns a new dataframe on current active perpetual positions.
        """
        columns = ["Symbol", "Type", "Entry Price", "Amount", "Leverage", "Unrealized PnL"]
        data = []
        for pos in self.perp1_positions:
            data.append([
                pos.trading_pair,
                "LONG" if pos.amount > 0 else "SHORT",
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
        for market_info in [self._perp_market1_info, self._perp_market2_info]:
            market, trading_pair, base_asset, quote_asset = market_info
            buy_price = await market.get_quote_price(trading_pair, True, self._order_amount)
            sell_price = await market.get_quote_price(trading_pair, False, self._order_amount)
            mid_price = (buy_price + sell_price) / 2
            data.append([
                market.display_name,
                trading_pair,
                float(sell_price),
                float(buy_price),
                float(mid_price)
            ])
        markets_df = pd.DataFrame(data=data, columns=columns)
        lines = []
        lines.extend(["", "  Markets:"] + ["    " + line for line in markets_df.to_string(index=False).split("\n")])

        # See if there're any active positions.
        if len(self.perp1_positions) > 0:
            df = self.active_positions_df()
            lines.extend(["", "  Positions1:"] + ["    " + line for line in df.to_string(index=False).split("\n")])
        else:
            lines.extend(["", "  No active positions."])

        assets_df = self.wallet_balance_data_frame([self._perp_market1_info, self._perp_market2_info])
        lines.extend(["", "  Assets:"] +
                     ["    " + line for line in str(assets_df).split("\n")])

        proposals = await self.create_base_proposals()
        if proposals:
            lines.extend(["", "  Opportunity:"] + self.short_proposal_msg(proposals))

        warning_lines = self.network_warning([self._perp_market1_info])
        warning_lines.extend(self.network_warning([self._perp_market2_info]))
        warning_lines.extend(self.balance_warning([self._perp_market1_info]))
        warning_lines.extend(self.balance_warning([self._perp_market2_info]))
        if len(warning_lines) > 0:
            lines.extend(["", "*** WARNINGS ***"] + warning_lines)

        return "\n".join(lines)

    def short_proposal_msg(self, arb_proposal: List[Proposal], indented: bool = True) -> List[str]:
        """
        Composes a short proposal message.
        :param arb_proposal: The arbitrage proposal
        :param indented: If the message should be indented (by 4 spaces)
        :return A list of messages
        """
        lines = []
        for proposal in arb_proposal:
            buy_side = "buy " + proposal.buy_side.market_info.market.display_name
            sell_side = "sell " + proposal.sell_side.market_info.market.display_name
            profit_pct = proposal.profit_pct()
            lines.append(f"{'    ' if indented else ''}{buy_side} at {proposal.buy_side.order_price} (rate {proposal.buy_side.funding_info.rate})"
                         f", {sell_side} at {proposal.sell_side.order_price} (rate {proposal.sell_side.funding_info.rate}): "
                         f"{profit_pct:.2%}")
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
        self.logger().info(f"Partially filled order {event.order_id}")

    def did_complete_buy_order(self, event: BuyOrderCompletedEvent):
        self.update_complete_order_id_lists(event.order_id)
        if self._strategy_state in [StrategyState.Opening1Limit, StrategyState.Closing1Limit]:
            # Execute second part of proposal
            self.execute_proposal(self.executing_proposal)

    def did_complete_sell_order(self, event: SellOrderCompletedEvent):
        self.update_complete_order_id_lists(event.order_id)
        if self._strategy_state in [StrategyState.Opening1Limit, StrategyState.Closing1Limit]:
            # Execute second part of proposal
            self.execute_proposal(self.executing_proposal)

    # def did_create_buy_order(self, event: BuyOrderEventCreated):
    #     self.logger().info(f"Created {event.type} BUY order {event.order_id}")

    # def did_create_sell_order(self, event: SellOrderEventCreated):
    #     self.logger().info(f"Created {event.type} SELL order {event.order_id}")

    def update_complete_order_id_lists(self, order_id: str):
        if self._strategy_state in [StrategyState.Opening1Limit, StrategyState.Opening2Market]:
            self._completed_opening_order_ids.append(order_id)
        elif self._strategy_state in [StrategyState.Closing1Limit, StrategyState.Closing2Market]:
            self._completed_closing_order_ids.append(order_id)
