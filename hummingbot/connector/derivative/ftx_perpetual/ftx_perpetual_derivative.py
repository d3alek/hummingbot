import asyncio
import copy
import logging
import time
from decimal import Decimal
from typing import (
    Any,
    AsyncIterable,
    Dict,
    List,
    Optional,
)

import aiohttp
import requests
import simplejson
from async_timeout import timeout

from hummingbot.connector.exchange_base import NaN
from hummingbot.connector.derivative.ftx_perpetual.ftx_perpetual_auth import FtxPerpetualAuth
from hummingbot.connector.derivative.ftx_perpetual.ftx_perpetual_in_flight_order import FtxPerpetualInFlightOrder
from hummingbot.connector.derivative.ftx_perpetual.ftx_perpetual_order_book_tracker import FtxPerpetualOrderBookTracker
from hummingbot.connector.derivative.ftx_perpetual.ftx_perpetual_user_stream_tracker import FtxPerpetualUserStreamTracker
from hummingbot.connector.derivative.ftx_perpetual.ftx_perpetual_utils import (
    convert_from_exchange_trading_pair,
    convert_to_exchange_trading_pair
)
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.derivative.position import Position

from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.core.event.events import (
    PositionAction,
    PositionSide,
    FundingInfo,
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    MarketEvent,
    MarketOrderFailureEvent,
    MarketTransactionFailureEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    OrderType,
    SellOrderCompletedEvent,
    SellOrderCreatedEvent,
    TradeType,
)
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.core.utils.estimate_fee import estimate_fee
from hummingbot.core.utils.tracking_nonce import get_tracking_nonce
from hummingbot.logger import HummingbotLogger
from hummingbot.core.data_type.transaction_tracker import TransactionTracker

from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.connector.perpetual_trading import PerpetualTrading

bm_logger = None
s_decimal_0 = Decimal(0)
UNRECOGNIZED_ORDER_DEBOUCE = 20  # seconds


class FtxPerpetualDerivative(ExchangeBase, PerpetualTrading):
    MARKET_RECEIVED_ASSET_EVENT_TAG = MarketEvent.ReceivedAsset
    MARKET_BUY_ORDER_COMPLETED_EVENT_TAG = MarketEvent.BuyOrderCompleted
    MARKET_SELL_ORDER_COMPLETED_EVENT_TAG = MarketEvent.SellOrderCompleted
    MARKET_WITHDRAW_ASSET_EVENT_TAG = MarketEvent.WithdrawAsset
    MARKET_ORDER_CANCELLED_EVENT_TAG = MarketEvent.OrderCancelled
    MARKET_TRANSACTION_FAILURE_EVENT_TAG = MarketEvent.TransactionFailure
    MARKET_ORDER_FAILURE_EVENT_TAG = MarketEvent.OrderFailure
    MARKET_ORDER_FILLED_EVENT_TAG = MarketEvent.OrderFilled
    MARKET_BUY_ORDER_CREATED_EVENT_TAG = MarketEvent.BuyOrderCreated
    MARKET_SELL_ORDER_CREATED_EVENT_TAG = MarketEvent.SellOrderCreated

    API_CALL_TIMEOUT = 10.0
    UPDATE_ORDERS_INTERVAL = 10.0
    ORDER_NOT_EXIST_CONFIRMATION_COUNT = 3

    FTX_API_ENDPOINT = "https://ftx.com/api"

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global bm_logger
        if bm_logger is None:
            bm_logger = logging.getLogger(__name__)
        return bm_logger

    def __init__(self,
                 ftx_secret_key: str,
                 ftx_api_key: str,
                 ftx_subaccount_name: str = None,
                 poll_interval: float = 5.0,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True):
        super().__init__()
        self._real_time_balance_update = False
        self._account_available_balances = {}
        self._account_balances = {}
        self._account_id = ""
        self._ftx_auth = FtxPerpetualAuth(ftx_api_key, ftx_secret_key, ftx_subaccount_name)
        self._ev_loop = asyncio.get_event_loop()
        self._in_flight_orders = {}
        self._last_poll_timestamp = 0
        self._last_timestamp = 0
        self._order_book_tracker = FtxPerpetualOrderBookTracker(trading_pairs=trading_pairs)
        self._order_not_found_records = {}
        self._poll_notifier = asyncio.Event()
        self._poll_interval = poll_interval
        self._shared_client = None
        self._status_polling_task = None
        self._trading_required = trading_required
        self._trading_rules = {}
        self._trading_rules_polling_task = None
        self._tx_tracker = FtxPerpetualDerivativeTransactionTracker(self)
        self._user_stream_event_listener_task = None
        self._user_stream_tracker = FtxPerpetualUserStreamTracker(
            ftx_auth=self._ftx_auth,
            trading_pairs=trading_pairs)
        self._user_stream_tracker_task = None
        self._check_network_interval = 60.0
        self._trading_pairs = trading_pairs

        ExchangeBase.__init__(self)
        PerpetualTrading.__init__(self)

        self._positions_initialized = False

    @property
    def name(self) -> str:
        return "ftx_perpetual"

    @property
    def order_books(self) -> Dict[str, OrderBook]:
        return self._order_book_tracker.order_books

    @property
    def ftx_auth(self) -> FtxPerpetualAuth:
        return self._ftx_auth

    @property
    def status_dict(self) -> Dict[str, bool]:
        return {
            "order_book_initialized": self._order_book_tracker.ready,
            "account_balance": len(self._account_balances) > 0 if self._trading_required else True,
            "trading_rule_initialized": len(self._trading_rules) > 0 if self._trading_required else True,
            "positions_initialized": self._positions_initialized
        }

    @property
    def ready(self) -> bool:
        return all(self.status_dict.values())

    @property
    def limit_orders(self) -> List[LimitOrder]:
        return [
            in_flight_order.to_limit_order()
            for in_flight_order in self._in_flight_orders.values()
        ]

    @property
    def tracking_states(self) -> Dict[str, any]:
        return {
            key: value.to_json()
            for key, value in self._in_flight_orders.items()
        }

    @property
    def user_stream_tracker(self) -> FtxPerpetualUserStreamTracker:
        return self._user_stream_tracker

    def restore_tracking_states(self, saved_states: Dict[str, any]):
        self._in_flight_orders.update({
            key: FtxPerpetualInFlightOrder.from_json(value)
            for key, value in saved_states.items()
        })

    # def start(self, Clock clock, double timestamp):
    #     self._tx_tracker.c_start(clock, timestamp)

    def tick(self, timestamp: float):
        poll_interval = self._poll_interval
        last_tick = int(self._last_timestamp / poll_interval)
        current_tick = int(timestamp / poll_interval)

        if current_tick > last_tick:
            if not self._poll_notifier.is_set():
                self._poll_notifier.set()

        self._tx_tracker.tick(timestamp)
        self._last_timestamp = timestamp

    async def _update_inflight_order(self, tracked_order: FtxPerpetualInFlightOrder, event: Dict[str, Any]):
        issuable_events: List[MarketEvent] = tracked_order.update(event)

        # Issue relevent events
        for (market_event, new_amount, new_price, new_fee) in issuable_events:
            base, quote = self.split_trading_pair(tracked_order.trading_pair)
            if market_event == MarketEvent.OrderCancelled:
                self.logger().info(f"Successfully cancelled order {tracked_order.client_order_id}")
                self.stop_tracking_order(tracked_order.client_order_id)
                self.trigger_event(self.MARKET_ORDER_CANCELLED_EVENT_TAG,
                                   OrderCancelledEvent(self.current_timestamp,
                                                       tracked_order.client_order_id))

            elif market_event == MarketEvent.OrderFailure:
                self.trigger_event(self.MARKET_ORDER_FAILURE_EVENT_TAG,
                                   MarketOrderFailureEvent(self.current_timestamp,
                                                           tracked_order.client_order_id,
                                                           tracked_order.order_type))

            elif market_event in [MarketEvent.BuyOrderCompleted, MarketEvent.SellOrderCompleted]:
                event = (self.MARKET_BUY_ORDER_COMPLETED_EVENT_TAG
                         if market_event == MarketEvent.BuyOrderCompleted
                         else self.MARKET_SELL_ORDER_COMPLETED_EVENT_TAG)
                event_class = (BuyOrderCompletedEvent
                               if market_event == MarketEvent.BuyOrderCompleted
                               else SellOrderCompletedEvent)

                try:
                    await asyncio.wait_for(tracked_order.wait_until_completely_filled(), timeout=2)
                except asyncio.TimeoutError:
                    self.logger().warning(
                        f"The order fill updates did not arrive on time for {tracked_order.client_order_id}. "
                        f"The complete update will be processed with estimated fees.")
                    fee_asset = tracked_order.quote_asset
                    fee = self.get_fee(
                        base,
                        quote,
                        tracked_order.order_type,
                        tracked_order.trade_type,
                        new_amount,
                        new_price)
                    fee_amount = fee.fee_amount_in_quote(
                        tracked_order.trading_pair, new_price, tracked_order.amount, self
                    )
                else:
                    fee_asset = tracked_order.fee_asset
                    fee_amount = tracked_order.fee_paid

                self.logger().info(f"The market {tracked_order.trade_type.name.lower()} order "
                                   f"{tracked_order.client_order_id} has completed according to user stream.")
                self.trigger_event(
                    event,
                    event_class(self.current_timestamp,
                                tracked_order.client_order_id,
                                base,
                                quote,
                                fee_asset,
                                tracked_order.executed_amount_base,
                                tracked_order.executed_amount_quote,
                                fee_amount,
                                tracked_order.order_type))
            # Complete the order if relevant
            if tracked_order.is_done:
                self.stop_tracking_order(tracked_order.client_order_id)

    async def _update_positions(self):
        positions = await self._api_request(
            "GET",
            path_url="/positions",
        )

        for position in positions["result"]:
            trading_pair = convert_from_exchange_trading_pair(position.get("future"))
            position_side = PositionSide.LONG if position.get("side") == 'buy' else PositionSide.SHORT
            unrealized_pnl = Decimal(position.get("unrealizedPnl"))
            entry_price = Decimal(position.get("cost"))
            amount = Decimal(position.get("size"))
            leverage = None  # TODO not getting that
            pos_key = self.position_key(trading_pair, position_side)
            if amount != 0:
                self._account_positions[pos_key] = Position(
                    trading_pair=trading_pair,
                    position_side=position_side,
                    unrealized_pnl=unrealized_pnl,
                    entry_price=entry_price,
                    amount=amount,
                    leverage=leverage
                )
            else:
                if pos_key in self._account_positions:
                    del self._account_positions[pos_key]

        self._positions_initialized = True

    async def _update_balances(self):
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()

        tasks = [self._api_request("GET", path_url="/wallet/balances")]

        results = await safe_gather(*tasks, return_exceptions=True)
        account_balances = results[0]

        for balance_entry in account_balances["result"]:
            asset_name = balance_entry["coin"]
            available_balance = Decimal(balance_entry["availableWithoutBorrow"])
            total_balance = Decimal(balance_entry["total"])
            self._account_available_balances[asset_name] = available_balance
            self._account_balances[asset_name] = total_balance
            remote_asset_names.add(asset_name)

        asset_names_to_remove = local_asset_names.difference(remote_asset_names)
        for asset_name in asset_names_to_remove:
            del self._account_available_balances[asset_name]
            del self._account_balances[asset_name]

        self._in_flight_orders_snapshot = {k: copy.copy(v) for k, v in self._in_flight_orders.items()}
        self._in_flight_orders_snapshot_timestamp = self.current_timestamp

    def _format_trading_rules(self, market_dict: Dict[str, Any]) -> List[TradingRule]:
        retval = []

        for market in market_dict.values():
            if market.get("name") in ['TRUMP2024', 'BOLSONARO2022']:
                continue
            try:
                trading_pair = convert_from_exchange_trading_pair(market.get("name"))
                min_trade_size = Decimal(market.get("minProvideSize"))
                price_increment = Decimal(market.get("priceIncrement"))
                size_increment = Decimal(market.get("sizeIncrement"))
                min_quote_amount_increment = price_increment * size_increment
                min_order_value = min_trade_size * price_increment

                # Trading Rules info from ftx API response
                retval.append(TradingRule(trading_pair,
                                          min_order_size=min_trade_size,
                                          min_price_increment=price_increment,
                                          min_base_amount_increment=size_increment,
                                          min_quote_amount_increment=min_quote_amount_increment,
                                          min_order_value=min_order_value,
                                          ))
            except Exception:
                self.logger().error(f"Error parsing the trading pair rule {market}. Skipping.", exc_info=True)
        return retval

    async def _update_trading_rules(self):
        # The poll interval for withdraw rules is 60 seconds.
        last_tick = self._last_timestamp / 60.0
        current_tick = self.current_timestamp / 60.0
        if current_tick > last_tick or len(self._trading_rules) <= 0:
            market_path_url = "/markets"
            # ticker_path_url = "/markets/tickers"

            market_list = await self._api_request("GET", path_url=market_path_url)

            result_list = {market["name"]: market for market in market_list["result"] if market["type"] == "future"}

            trading_rules_list = self._format_trading_rules(result_list)
            self._trading_rules.clear()
            for trading_rule in trading_rules_list:
                self._trading_rules[trading_rule.trading_pair] = trading_rule

    @property
    def in_flight_orders(self) -> Dict[str, FtxPerpetualInFlightOrder]:
        return self._in_flight_orders

    def supported_order_types(self):
        return [OrderType.LIMIT, OrderType.MARKET, OrderType.LIMIT_MAKER]

    async def _update_order_status(self):
        # This is intended to be a backup measure to close straggler orders, in case ftx's user stream events
        # are not capturing the updates as intended. Also handles filled events that are not captured by
        # _user_stream_event_listener
        # The poll interval for order status is 10 seconds.
        last_tick = self._last_poll_timestamp / self.UPDATE_ORDERS_INTERVAL
        current_tick = self.current_timestamp / self.UPDATE_ORDERS_INTERVAL

        if current_tick > last_tick and len(self._in_flight_orders) > 0:
            tracked_orders = list(self._in_flight_orders.values())
            for tracked_order in tracked_orders:
                try:
                    response = await self._api_request(
                        "GET",
                        path_url=f"/orders/by_client_id/{tracked_order.client_order_id}")
                    order = response["result"]

                    await self._update_inflight_order(tracked_order, order)
                except RuntimeError as e:
                    if ("Order not found" in str(e)
                            and tracked_order.creation_timestamp < (time.time() - UNRECOGNIZED_ORDER_DEBOUCE)):
                        tracked_order.set_status("FAILURE")
                        self.trigger_event(
                            self.MARKET_ORDER_FAILURE_EVENT_TAG,
                            MarketOrderFailureEvent(self.current_timestamp,
                                                    tracked_order.client_order_id,
                                                    tracked_order.order_type)
                        )
                        self.stop_tracking_order(tracked_order.client_order_id)
                        self.logger().warning(
                            f"Order {tracked_order.client_order_id} not found on exchange after "
                            f"{UNRECOGNIZED_ORDER_DEBOUCE} seconds. Marking as failed"
                        )
                    else:
                        self.logger().error(
                            f"Unexpected error when polling for {tracked_order.client_order_id} status.", exc_info=True
                        )
                    continue
                except Exception:
                    self.logger().error(
                        f"Unexpected error when polling for {tracked_order.client_order_id} status.", exc_info=True
                    )
                    continue

    async def _iter_user_stream_queue(self) -> AsyncIterable[Dict[str, Any]]:
        while True:
            try:
                yield await self._user_stream_tracker.user_stream.get()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unknown error. Retrying after 1 second.", exc_info=True)
                await asyncio.sleep(1.0)

    async def _user_stream_event_listener(self):
        async for stream_message in self._iter_user_stream_queue():
            try:
                channel = stream_message.get("channel")
                data = stream_message.get("data")
                event_type = stream_message.get("type")

                if channel == "orders" and event_type == "update":  # Updates track order status
                    safe_ensure_future(self._process_order_update_event(data))
                elif channel == "fills" and event_type == "update":
                    safe_ensure_future(self._process_trade_event(data))
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await asyncio.sleep(5.0)

    async def _process_order_update_event(self, event_message: Dict[str, Any]):
        try:
            tracked_order = self._in_flight_orders[event_message['clientId']]
            await self._update_inflight_order(tracked_order, event_message)
        except KeyError:
            self.logger().debug(f"Unknown order id from user stream order status updates: {event_message['clientId']}")
        except Exception as e:
            self.logger().error(f"Unexpected error from user stream order status updates: {e}, "
                                f"{event_message}", exc_info=True)

    async def _process_trade_event(self, event_message: Dict[str, Any]):
        tracked_order = None
        for order in list(self._in_flight_orders.values()):
            exchange_id = await order.get_exchange_order_id()
            if exchange_id == str(event_message["orderId"]):
                tracked_order = order
                break
        if tracked_order:
            updated = tracked_order.update_with_trade_update(event_message)
            if updated:
                execute_amount_diff = Decimal(event_message["size"])
                self.logger().info(
                    f"Filled {execute_amount_diff} out of {tracked_order.amount} of the "
                    f"{tracked_order.order_type_description} order {tracked_order.client_order_id}")
                # exchange_order_id = tracked_order.exchange_order_id

                self.trigger_event(
                    self.MARKET_ORDER_FILLED_EVENT_TAG,
                    OrderFilledEvent(
                        self.current_timestamp,
                        tracked_order.client_order_id,
                        tracked_order.trading_pair,
                        tracked_order.trade_type,
                        tracked_order.order_type,
                        Decimal(event_message["price"]),
                        execute_amount_diff,
                        AddedToCostTradeFee(
                            flat_fees=[
                                TokenAmount(
                                    event_message["feeCurrency"], Decimal(event_message["fee"])
                                )
                            ]
                        ),
                        exchange_trade_id=event_message["tradeId"])
                )
                await self._update_positions()

    async def _status_polling_loop(self):
        while True:
            try:
                self._poll_notifier = asyncio.Event()
                await self._poll_notifier.wait()

                await safe_gather(
                    self._update_balances(),
                    self._update_order_status(),
                    self._update_positions(),
                )
                self._last_poll_timestamp = self.current_timestamp
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network("Unexpected error while polling updates.",
                                      exc_info=True,
                                      app_warning_msg="Could not fetch updates from ftx. "
                                                      "Check API key and network connection.")
                await asyncio.sleep(5.0)

    async def _trading_rules_polling_loop(self):
        while True:
            try:
                await self._update_trading_rules()
                await asyncio.sleep(60 * 5)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network("Unexpected error while fetching trading rule updates.",
                                      exc_info=True,
                                      app_warning_msg="Could not fetch updates from ftx. "
                                                      "Check API key and network connection.")
                await asyncio.sleep(0.5)

    def get_order_book(self, trading_pair: str) -> OrderBook:
        order_books = self._order_book_tracker.order_books

        if trading_pair not in order_books:
            raise ValueError(f"No order book exists for '{trading_pair}'.")
        return order_books[trading_pair]

    def start_tracking_order(self,
                             order_id: str,
                             exchange_order_id: str,
                             trading_pair: str,
                             order_type,
                             trade_type,
                             price,
                             amount):
        self._in_flight_orders[order_id] = FtxPerpetualInFlightOrder(
            order_id,
            exchange_order_id,
            trading_pair,
            order_type,
            trade_type,
            price,
            amount,
            creation_timestamp=self.current_timestamp
        )

    def stop_tracking_order(self, order_id: str):
        if order_id in self._in_flight_orders:
            del self._in_flight_orders[order_id]

    def did_timeout_tx(self, tracking_id: str):
        self.trigger_event(self.MARKET_TRANSACTION_FAILURE_EVENT_TAG,
                           MarketTransactionFailureEvent(self.current_timestamp, tracking_id))

    def get_order_price_quantum(self, trading_pair: str, price):
        trading_rule: TradingRule = self._trading_rules[trading_pair]
        return Decimal(trading_rule.min_price_increment)

    def get_order_size_quantum(self, trading_pair: str, order_size):
        trading_rule: TradingRule = self._trading_rules[trading_pair]
        return Decimal(trading_rule.min_base_amount_increment)

    def quantize_order_amount(self, trading_pair: str, amount, price=0.0):
        trading_rule: TradingRule = self._trading_rules[trading_pair]
        quantized_amount = ExchangeBase.quantize_order_amount(self, trading_pair, amount)

        global s_decimal_0
        if quantized_amount < trading_rule.min_order_size:
            return s_decimal_0

        if (price > s_decimal_0) and (quantized_amount * price) < trading_rule.min_order_value:
            return s_decimal_0

        return quantized_amount

    async def place_order(self,
                          order_id: str,
                          trading_pair: str,
                          amount: Decimal,
                          is_buy: bool,
                          order_type: OrderType,
                          price: Optional[Decimal],
                          position_action: PositionAction) -> Dict[str, Any]:

        path_url = "/orders"

        body = {}
        if order_type.is_limit_type():
            body = {
                "market": convert_to_exchange_trading_pair(trading_pair),
                "side": "buy" if is_buy else "sell",
                "price": price,
                "size": amount,
                "type": "limit",
                "ioc": False,
                "postOnly": order_type is OrderType.LIMIT_MAKER,
                "clientId": str(order_id),
                "reduceOnly": True if position_action == PositionAction.CLOSE else False
            }
        elif order_type is OrderType.MARKET:
            body = {
                "market": convert_to_exchange_trading_pair(trading_pair),
                "side": "buy" if is_buy else "sell",
                "price": None,
                "type": "market",
                "size": amount,
                "ioc": False,
                "postOnly": False,
                "clientId": str(order_id),
                "reduceOnly": True if position_action == PositionAction.CLOSE else False
            }
        else:
            raise ValueError(f"Unknown order_type for FTX: {order_type}")

        api_response = await self._api_request("POST", path_url=path_url, body=body)

        return api_response

    async def execute_buy(self,
                          order_id: str,
                          trading_pair: str,
                          amount: Decimal,
                          order_type: OrderType,
                          price: Optional[Decimal] = s_decimal_0,
                          position_action: PositionAction = None
                          ):
        trading_rule: TradingRule = self._trading_rules[trading_pair]

        decimal_amount = self.quantize_order_amount(trading_pair, amount)
        decimal_price = (self.quantize_order_price(trading_pair, price)
                         if order_type.is_limit_type()
                         else s_decimal_0)

        if decimal_amount < trading_rule.min_order_size:
            raise ValueError(f"Buy order amount {decimal_amount} is lower than the minimum order size "
                             f"{trading_rule.min_order_size}.")

        try:
            order_result = None
            self.start_tracking_order(
                order_id,
                None,
                trading_pair,
                order_type,
                TradeType.BUY,
                decimal_price,
                decimal_amount
            )
            if order_type.is_limit_type():
                try:
                    order_result = await self.place_order(order_id,
                                                          trading_pair,
                                                          decimal_amount,
                                                          True,
                                                          order_type,
                                                          decimal_price,
                                                          position_action)
                except asyncio.TimeoutError:
                    # We timed out while placing this order. We may have successfully submitted the order, or we may have had connection
                    # issues that prevented the submission from taking place. We'll assume that the order is live and let our order status
                    # updates mark this as cancelled if it doesn't actually exist.
                    self.logger().warning(f"Order {order_id} has timed out and putatively failed. Order will be tracked until reconciled.")
                    return True
            elif order_type is OrderType.MARKET:
                try:
                    order_result = await self.place_order(order_id,
                                                          trading_pair,
                                                          decimal_amount,
                                                          True,
                                                          order_type,
                                                          None,
                                                          position_action)
                except asyncio.TimeoutError:
                    # We timed out while placing this order. We may have successfully submitted the order, or we may have had connection
                    # issues that prevented the submission from taking place. We'll assume that the order is live and let our order status
                    # updates mark this as cancelled if it doesn't actually exist.
                    self.logger().warning(f"Order {order_id} has timed out and putatively failed. Order will be tracked until reconciled.")
                    return True
            else:
                raise ValueError(f"Invalid OrderType {order_type}. Aborting.")

            # Verify the response from the exchange
            if "success" not in order_result.keys():
                raise Exception(order_result)

            success = order_result["success"]
            if not success:
                raise Exception(order_result)

            exchange_order_id = str(order_result["result"]["id"])

            tracked_order = self._in_flight_orders.get(order_id)
            if tracked_order is not None and exchange_order_id:
                tracked_order.update_exchange_order_id(exchange_order_id)
                order_type_str = "MARKET" if order_type == OrderType.MARKET else "LIMIT"
                self.logger().info(f"Created {order_type_str} buy order {order_id} for "
                                   f"{decimal_amount} {trading_pair}")
                self.trigger_event(
                    self.MARKET_BUY_ORDER_CREATED_EVENT_TAG,
                    BuyOrderCreatedEvent(
                        self.current_timestamp,
                        order_type,
                        trading_pair,
                        decimal_amount,
                        decimal_price,
                        order_id,
                        tracked_order.creation_timestamp,
                    ))

        except asyncio.CancelledError:
            raise
        except Exception:
            tracked_order = self._in_flight_orders.get(order_id)
            tracked_order.set_status("FAILURE")
            self.stop_tracking_order(order_id)
            self.logger().error(
                f"Error submitting buy {order_type} order to ftx for "
                f"{decimal_amount} {trading_pair} "
                f"{decimal_price}. Response={order_result}",
                exc_info=True
            )
            self.trigger_event(
                self.MARKET_ORDER_FAILURE_EVENT_TAG,
                MarketOrderFailureEvent(
                    self.current_timestamp,
                    order_id,
                    order_type
                ))

    def buy(self,
            trading_pair: str,
            amount,
            order_type=OrderType.LIMIT,
            price=NaN,
            position_action=None,
            **kwargs) -> str:
        tracking_nonce = get_tracking_nonce()
        order_id: str = str(f"FTX-buy-{trading_pair}-{tracking_nonce}")
        safe_ensure_future(self.execute_buy(order_id, trading_pair, amount, order_type, price, position_action))
        return order_id

    async def execute_sell(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           order_type: OrderType = OrderType.LIMIT,
                           price: Optional[Decimal] = NaN,
                           position_action: PositionAction = None):
        trading_rule: TradingRule = self._trading_rules[trading_pair]

        decimal_amount = self.quantize_order_amount(trading_pair, amount)
        decimal_price = (self.quantize_order_price(trading_pair, price)
                         if order_type.is_limit_type()
                         else s_decimal_0)

        if decimal_amount < trading_rule.min_order_size:
            raise ValueError(f"Sell order amount {decimal_amount} is lower than the minimum order size "
                             f"{trading_rule.min_order_size}")

        try:
            order_result = None

            self.start_tracking_order(
                order_id,
                None,
                trading_pair,
                order_type,
                TradeType.SELL,
                decimal_price,
                decimal_amount
            )

            if order_type.is_limit_type():
                try:
                    order_result = await self.place_order(order_id,
                                                          trading_pair,
                                                          decimal_amount,
                                                          False,
                                                          order_type,
                                                          decimal_price,
                                                          position_action)
                except asyncio.TimeoutError:
                    # We timed out while placing this order. We may have successfully submitted the order, or we may have had connection
                    # issues that prevented the submission from taking place. We'll assume that the order is live and let our order status
                    # updates mark this as cancelled if it doesn't actually exist.
                    self.logger().warning(f"Order {order_id} has timed out and putatively failed. Order will be tracked until reconciled.")
                    return True
            elif order_type is OrderType.MARKET:
                try:
                    order_result = await self.place_order(order_id,
                                                          trading_pair,
                                                          decimal_amount,
                                                          False,
                                                          order_type,
                                                          None,
                                                          position_action)
                except asyncio.TimeoutError:
                    # We timed out while placing this order. We may have successfully submitted the order, or we may have had connection
                    # issues that prevented the submission from taking place. We'll assume that the order is live and let our order status
                    # updates mark this as cancelled if it doesn't actually exist.
                    self.logger().warning(f"Order {order_id} has timed out and putatively failed. Order will be tracked until reconciled.")
                    return True
            else:
                raise ValueError(f"Invalid OrderType {order_type}. Aborting.")

            # Verify the response from the exchange
            if "success" not in order_result.keys():
                raise Exception(order_result)

            success = order_result["success"]
            if not success:
                raise Exception(order_result)

            exchange_order_id = str(order_result["result"]["id"])

            tracked_order = self._in_flight_orders.get(order_id)
            if tracked_order is not None and exchange_order_id:
                tracked_order.update_exchange_order_id(exchange_order_id)
                order_type_str = "MARKET" if order_type == OrderType.MARKET else "LIMIT"
                self.logger().info(f"Created {order_type_str} sell order {order_id} for "
                                   f"{decimal_amount} {trading_pair}.")
                self.trigger_event(
                    self.MARKET_SELL_ORDER_CREATED_EVENT_TAG,
                    SellOrderCreatedEvent(
                        self.current_timestamp,
                        order_type,
                        trading_pair,
                        decimal_amount,
                        decimal_price,
                        order_id,
                        tracked_order.creation_timestamp,
                    ))
        except asyncio.CancelledError:
            raise
        except Exception:
            tracked_order = self._in_flight_orders.get(order_id)
            tracked_order.set_status("FAILURE")
            self.stop_tracking_order(order_id)
            self.logger().error(
                f"Error submitting sell {order_type} order to ftx for "
                f"{decimal_amount} {trading_pair} "
                f"{decimal_price}. Response={order_result}",
                exc_info=True
            )
            self.trigger_event(
                self.MARKET_ORDER_FAILURE_EVENT_TAG,
                MarketOrderFailureEvent(self.current_timestamp, order_id, order_type))

    def sell(self,
             trading_pair: str,
             amount,
             order_type=OrderType.MARKET,
             price=0.0,
             position_action=None,
             **kwargs) -> str:
        tracking_nonce = get_tracking_nonce()
        order_id: str = str(f"FTX-sell-{trading_pair}-{tracking_nonce}")

        safe_ensure_future(self.execute_sell(order_id, trading_pair, amount, order_type, price, position_action))
        return order_id

    async def execute_cancel(self, trading_pair: str, order_id: str):
        tracked_order = self._in_flight_orders.get(order_id)
        if tracked_order is None:
            self.logger().error(f"The order {order_id} is not tracked. ")
            raise ValueError

        path_url = f"/orders/by_client_id/{order_id}"
        try:
            cancel_result = await self._api_request("DELETE", path_url=path_url)

            if cancel_result["success"] or (cancel_result["error"] in ["Order already closed", "Order already queued for cancellation"]):
                self.logger().info(f"Requested cancellation of order {order_id}.")
                return order_id
            else:
                self.logger().info(f"Could not request cancellation of order {order_id} as FTX returned: {cancel_result}")
                return order_id
        except Exception as e:
            self.logger().network(
                f"Failed to cancel order {order_id}. {e}",
                exc_info=True,
                app_warning_msg=f"Failed to cancel the order {order_id} on ftx. "
                                f"Check API key and network connection."
            )
        return None

    def cancel(self, trading_pair: str, order_id: str):
        safe_ensure_future(self.execute_cancel(trading_pair, order_id))
        return order_id

    async def cancel_all(self, timeout_seconds: float) -> List[CancellationResult]:
        # TODO close positions?
        incomplete_orders = [order for order in self._in_flight_orders.values() if not order.is_done]

        tasks = [self.execute_cancel(o.trading_pair, o.client_order_id) for o in incomplete_orders]
        order_id_set = set([o.client_order_id for o in incomplete_orders])
        successful_cancellation = []

        try:
            async with timeout(timeout_seconds):
                api_responses = await safe_gather(*tasks, return_exceptions=True)
                for order_id in api_responses:
                    if order_id:
                        order_id_set.remove(order_id)
                        successful_cancellation.append(CancellationResult(order_id, True))
        except Exception:
            self.logger().network(
                "Unexpected error cancelling orders.",
                app_warning_msg="Failed to cancel order on ftx. Check API key and network connection."
            )

        failed_cancellation = [CancellationResult(oid, False) for oid in order_id_set]
        return successful_cancellation + failed_cancellation

    async def _http_client(self) -> aiohttp.ClientSession:
        if self._shared_client is None:
            self._shared_client = aiohttp.ClientSession()
        return self._shared_client

    async def _api_request(self,
                           http_method: str,
                           path_url: str = None,
                           params: Dict[str, any] = None,
                           body: Dict[str, any] = None) -> Dict[str, Any]:
        assert path_url is not None

        url = f"{self.FTX_API_ENDPOINT}{path_url}"

        headers = self.ftx_auth.generate_auth_dict(http_method, url, params, body)

        if http_method == 'POST':
            res = requests.post(url, json=body, headers=headers)
            res_body = res.text
            return simplejson.loads(res_body, parse_float=Decimal)
        else:
            client = await self._http_client()
            async with client.request(http_method,
                                      url=url,
                                      headers=headers,
                                      data=body,
                                      timeout=self.API_CALL_TIMEOUT) as response:
                res_body = await response.text()
                data = simplejson.loads(res_body, parse_float=Decimal)
                if http_method == 'DELETE':
                    return data
                if response.status not in [200, 201]:  # HTTP Response code of 20X generally means it is successful
                    raise RuntimeError(f"Error fetching response from {http_method}-{url}. HTTP Status Code {response.status}: "
                                       f"{data}")
                return data

    async def check_network(self) -> NetworkStatus:
        try:
            await self._api_request("GET", path_url="/wallet/balances")
        except asyncio.CancelledError:
            raise
        except Exception:
            return NetworkStatus.NOT_CONNECTED
        return NetworkStatus.CONNECTED

    def _stop_network(self):
        self._order_book_tracker.stop()
        if self._status_polling_task is not None:
            self._status_polling_task.cancel()
        if self._user_stream_tracker_task is not None:
            self._user_stream_tracker_task.cancel()
        if self._user_stream_event_listener_task is not None:
            self._user_stream_event_listener_task.cancel()
        self._status_polling_task = self._user_stream_tracker_task = \
            self._user_stream_event_listener_task = None

    async def stop_network(self):
        self._stop_network()

    async def start_network(self):
        self._stop_network()
        self._order_book_tracker.start()
        self._trading_rules_polling_task = safe_ensure_future(self._trading_rules_polling_loop())
        if self._trading_required:
            self._status_polling_task = safe_ensure_future(self._status_polling_loop())
            self._user_stream_tracker_task = safe_ensure_future(self._user_stream_tracker.start())
            self._user_stream_event_listener_task = safe_ensure_future(self._user_stream_event_listener())

    def get_fee(self,
                base_currency: str,
                quote_currency: str,
                order_type: OrderType,
                order_side: TradeType,
                amount: Decimal,
                price: Decimal = Decimal('NaN'),
                is_maker: Optional[bool] = None) -> AddedToCostTradeFee:
        is_maker = order_type is OrderType.LIMIT_MAKER
        return estimate_fee("ftx", is_maker)

    def get_funding_info(self, trading_pair: str) -> Optional[FundingInfo]:
        """
        Retrieves the Funding Info for the specified trading pair.
        Note: This function should NOT be called when the connector is not yet ready.
        :param: trading_pair: The specified trading pair.
        """
        if trading_pair in self._order_book_tracker.data_source.funding_info:
            return self._order_book_tracker.data_source.funding_info[trading_pair]
        else:
            self.logger().error(f"Funding Info for {trading_pair} not found. Proceeding to fetch using REST API.")
            safe_ensure_future(self._order_book_tracker.data_source.get_funding_info(trading_pair))
            return None

    # def set_position_mode(self, position_mode: PositionMode):
    #     safe_ensure_future(self._set_position_mode(position_mode))

    # async def _set_position_mode(self, position_mode: PositionMode):
    #     initial_mode = await self._get_position_mode()
    #     if initial_mode != position_mode:
    #         params = {
    #             "dualSidePosition": position_mode.value
    #         }
    #         response = await self.__api_request(
    #             method=RESTMethod.POST,
    #             path=CONSTANTS.CHANGE_POSITION_MODE_URL,
    #             data=params,
    #             add_timestamp=True,
    #             is_auth_required=True,
    #             limit_id=CONSTANTS.POST_POSITION_MODE_LIMIT_ID,
    #             return_err=True
    #         )
    #         if response["msg"] == "success" and response["code"] == 200:
    #             self.logger().info(f"Using {position_mode.name} position mode.")
    #             self._position_mode = position_mode
    #         else:
    #             self.logger().error(f"Unable to set postion mode to {position_mode.name}.")
    #             self.logger().info(f"Using {initial_mode.name} position mode.")
    #             self._position_mode = initial_mode
    #     else:
    #         self.logger().info(f"Using {position_mode.name} position mode.")
    #         self._position_mode = position_mode

    # async def _get_position_mode(self) -> Optional[PositionMode]:
    #     # To-do: ensure there's no active order or contract before changing position mode
    #     if self._position_mode is None:
    #         response = await self.__api_request(
    #             method=RESTMethod.GET,
    #             path=CONSTANTS.CHANGE_POSITION_MODE_URL,
    #             add_timestamp=True,
    #             is_auth_required=True,
    #             limit_id=CONSTANTS.GET_POSITION_MODE_LIMIT_ID,
    #             return_err=True
    #         )
    #         self._position_mode = PositionMode.HEDGE if response["dualSidePosition"] else PositionMode.ONEWAY

    #     return self._position_mode


class FtxPerpetualDerivativeTransactionTracker(TransactionTracker):
    def __init__(self, owner):
        super().__init__()
        self._owner = owner

    def did_timeout_tx(self, tx_id: str):
        TransactionTracker.did_timeout_tx(self, tx_id)
        self._owner.did_timeout_tx(tx_id)
