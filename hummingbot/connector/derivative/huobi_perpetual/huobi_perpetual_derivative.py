import asyncio
import logging
import time
from decimal import Decimal
from typing import (
    Any,
    AsyncIterable,
    Dict,
    List,
    Optional
)

import ujson

from hummingbot.connector.derivative.position import Position
import hummingbot.connector.exchange.huobi.huobi_constants as CONSTANTS
from hummingbot.connector.exchange.huobi.huobi_auth import HuobiAuth
from hummingbot.connector.exchange.huobi.huobi_in_flight_order import HuobiInFlightOrder
from hummingbot.connector.exchange.huobi.huobi_order_book_tracker import HuobiOrderBookTracker
from hummingbot.connector.exchange.huobi.huobi_user_stream_tracker import HuobiUserStreamTracker
from hummingbot.connector.exchange.huobi.huobi_utils import (
    build_api_factory,
    convert_to_exchange_trading_pair,
    get_new_client_order_id,
    OrderStatus
)
from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.connector.perpetual_trading import PerpetualTrading
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.core.data_type.transaction_tracker import TransactionTracker
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
    PositionMode
)
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils.async_call_scheduler import AsyncCallScheduler
from hummingbot.core.utils.async_utils import (
    safe_ensure_future,
    safe_gather,
)
from hummingbot.core.utils.estimate_fee import estimate_fee
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest
from hummingbot.core.web_assistant.rest_assistant import RESTAssistant
from hummingbot.logger import HummingbotLogger

hm_logger = None
s_decimal_0 = Decimal(0)
s_decimal_NaN = Decimal("NaN")
HUOBI_ROOT_API = "https://api.huobi.pro/v1"


class HuobiAPIError(IOError):
    def __init__(self, error_payload: Dict[str, Any]):
        super().__init__(str(error_payload))
        self.error_payload = error_payload


class HuobiPerpetualDerivative(ExchangeBase, PerpetualTrading):
    MARKET_BUY_ORDER_COMPLETED_EVENT_TAG = MarketEvent.BuyOrderCompleted
    MARKET_SELL_ORDER_COMPLETED_EVENT_TAG = MarketEvent.SellOrderCompleted
    MARKET_ORDER_CANCELLED_EVENT_TAG = MarketEvent.OrderCancelled
    MARKET_TRANSACTION_FAILURE_EVENT_TAG = MarketEvent.TransactionFailure
    MARKET_ORDER_FAILURE_EVENT_TAG = MarketEvent.OrderFailure
    MARKET_ORDER_FILLED_EVENT_TAG = MarketEvent.OrderFilled
    MARKET_BUY_ORDER_CREATED_EVENT_TAG = MarketEvent.BuyOrderCreated
    MARKET_SELL_ORDER_CREATED_EVENT_TAG = MarketEvent.SellOrderCreated
    API_CALL_TIMEOUT = 10.0
    UPDATE_ORDERS_INTERVAL = 10.0
    SHORT_POLL_INTERVAL = 5.0
    LONG_POLL_INTERVAL = 120.0

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global hm_logger
        if hm_logger is None:
            hm_logger = logging.getLogger(__name__)
        return hm_logger

    def __init__(self,
                 huobi_api_key: str,
                 huobi_secret_key: str,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True):

        super().__init__()

        self._async_scheduler = AsyncCallScheduler(call_interval=0.5)
        self._ev_loop = asyncio.get_event_loop()
        self._huobi_auth = HuobiAuth(api_key=huobi_api_key, secret_key=huobi_secret_key)
        self._in_flight_orders = {}
        self._last_poll_timestamp = 0
        self._last_timestamp = 0
        self._api_factory = build_api_factory()
        self._order_book_tracker = HuobiOrderBookTracker(
            trading_pairs=trading_pairs,
            api_factory=self._api_factory,
        )
        self._poll_notifier = asyncio.Event()
        self._rest_assistant = None
        self._status_polling_task = None
        self._trading_required = trading_required
        self._trading_rules = {}
        self._trading_rules_polling_task = None
        self._tx_tracker = HuobiPerpetualDerivativeTransactionTracker(self)

        self._user_stream_event_listener_task = None
        self._user_stream_tracker = HuobiUserStreamTracker(huobi_auth=self._huobi_auth,
                                                           api_factory=self._api_factory)
        self._user_stream_tracker_task = None
        self._leverage = {}

        ExchangeBase.__init__(self)
        PerpetualTrading.__init__(self)

        self._positions_initialized = False

    @property
    def name(self) -> str:
        return "huobi_perpetual"

    @property
    def order_book_tracker(self) -> HuobiOrderBookTracker:
        return self._order_book_tracker

    @property
    def order_books(self) -> Dict[str, OrderBook]:
        return self._order_book_tracker.order_books

    @property
    def trading_rules(self) -> Dict[str, TradingRule]:
        return self._trading_rules

    @property
    def in_flight_orders(self) -> Dict[str, HuobiInFlightOrder]:
        return self._in_flight_orders

    @property
    def limit_orders(self) -> List[LimitOrder]:
        return [
            in_flight_order.to_limit_order()
            for in_flight_order in self._in_flight_orders.values()
        ]

    @property
    def tracking_states(self) -> Dict[str, Any]:
        return {
            key: value.to_json()
            for key, value in self._in_flight_orders.items()
        }

    @property
    def user_stream_tracker(self) -> HuobiUserStreamTracker:
        return self._user_stream_tracker

    @property
    def position_mode(self):
        raise RuntimeError("Huobi has per trading_pair position_mode, use get_position_mode(trading_pair) instead")

    def restore_tracking_states(self, saved_states: Dict[str, Any]):
        self._in_flight_orders.update({
            key: HuobiInFlightOrder.from_json(value)
            for key, value in saved_states.items()
        })

    async def start_network(self):
        self._stop_network()
        self._order_book_tracker.start()
        self._trading_rules_polling_task = safe_ensure_future(self._trading_rules_polling_loop())
        self._user_stream_event_listener_task = safe_ensure_future(self._user_stream_event_listener())
        if self._trading_required:
            self._user_stream_tracker_task = safe_ensure_future(self._user_stream_tracker.start())
            self._status_polling_task = safe_ensure_future(self._status_polling_loop())

    def _stop_network(self):
        self._order_book_tracker.stop()
        if self._status_polling_task is not None:
            self._status_polling_task.cancel()
            self._status_polling_task = None
        if self._trading_rules_polling_task is not None:
            self._trading_rules_polling_task.cancel()
            self._trading_rules_polling_task = None
        if self._user_stream_event_listener_task is not None:
            self._user_stream_event_listener_task.cancel()
            self._user_stream_event_listener_task = None
        if self._user_stream_tracker_task is not None:
            self._user_stream_tracker_task.cancel()
            self._user_stream_tracker_task = None

    async def stop_network(self):
        self._stop_network()

    async def check_network(self) -> NetworkStatus:
        try:
            await self._api_request(method="get", path_url=CONSTANTS.SERVER_TIME_URL)
        except asyncio.CancelledError:
            raise
        except Exception:
            return NetworkStatus.NOT_CONNECTED
        return NetworkStatus.CONNECTED

    def tick(self, timestamp: float):
        """
        Is called automatically by the clock for each clock's tick (1 second by default).
        It checks if status polling task is due for execution.
        """
        now = time.time()
        poll_interval = (self.SHORT_POLL_INTERVAL
                         if now - self._user_stream_tracker.last_recv_time > 60.0
                         else self.LONG_POLL_INTERVAL)
        last_tick = int(self._last_timestamp / poll_interval)
        current_tick = int(timestamp / poll_interval)
        # self.logger().info(f"tick {now - self._user_stream_tracker.last_recv_time} {poll_interval} {last_tick} {current_tick}")
        if current_tick > last_tick:
            if not self._poll_notifier.is_set():
                self._poll_notifier.set()

        self._last_timestamp = timestamp

    async def _get_rest_assistant(self) -> RESTAssistant:
        if self._rest_assistant is None:
            self._rest_assistant = await self._api_factory.get_rest_assistant()
        return self._rest_assistant

    async def _api_request(self,
                           method,
                           path_url,
                           params: Optional[Dict[str, Any]] = None,
                           data=None,
                           is_auth_required: bool = False) -> Dict[str, Any]:
        content_type = "application/json" if method == "post" else "application/x-www-form-urlencoded"
        headers = {"Content-Type": content_type}
        url = CONSTANTS.FUTURES_URL + path_url
        client = await self._get_rest_assistant()

        if is_auth_required:
            params = self._huobi_auth.add_auth_to_params(method, path_url, params)

        request = RESTRequest(method=RESTMethod[method.upper()],
                              url=url,
                              data=ujson.dumps(data) if data else None,
                              params=params,
                              headers=headers,
                              is_auth_required=is_auth_required,
                              )

        response = await client.call(request)

        if response.status != 200:
            self.logger().error(await response.text())
            raise IOError(f"Error fetching data from {url}. HTTP status is {response.status}.")
        try:
            parsed_response = await response.json(content_type=None)
        except Exception:
            raise IOError(f"Error parsing data from {url}.")

        if path_url == CONSTANTS.SERVER_TIME_URL:
            data = parsed_response
        else:
            data = parsed_response.get("data")
        if data is None:
            self.logger().error(f"Error received for {url}. Response is {parsed_response}.")
            raise HuobiAPIError({"error": parsed_response})
        return data

    async def _update_balances(self):
        self.logger().info("update balances start")
        new_available_balances = {}
        new_balances = {}
        data = await self._api_request(
            "post",
            path_url=CONSTANTS.ACCOUNT_BALANCE_URL,
            is_auth_required=True)
        if len(data) > 0:
            for balance_entry in data:
                asset_name = balance_entry["margin_asset"].upper()
                balance = Decimal(balance_entry["margin_balance"])
                if balance == s_decimal_0:
                    continue
                if asset_name not in new_available_balances:
                    new_available_balances[asset_name] = s_decimal_0
                if asset_name not in new_balances:
                    new_balances[asset_name] = s_decimal_0

                new_balances[asset_name] += balance
                new_available_balances[asset_name] = balance

            self._account_available_balances.clear()
            self._account_available_balances = new_available_balances
            self._account_balances.clear()
            self._account_balances = new_balances

        self.logger().info("update balances done")

    async def _update_positions(self, positions = None):
        if positions is None:
            params = {"margin_account": "USDT"}
            data = await self._api_request(
                "post",
                path_url=CONSTANTS.POSITION_INFO,
                is_auth_required=True,
                data=params,
            )
            positions = data['positions']

        for position in positions:
            trading_pair = position.get("contract_code")
            position_side = PositionSide.LONG if position.get("direction") == 'buy' else PositionSide.SHORT
            unrealized_pnl = Decimal(position.get("profit_unreal"))
            entry_price = Decimal(position.get("cost_open"))
            amount = Decimal(position.get("volume"))
            if trading_pair == 'LOOKS-USDT':
                amount /= 10
            leverage = Decimal(position.get("lever_rate"))
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

    def get_fee(self,
                base_currency: str,
                quote_currency: str,
                order_type: OrderType,
                order_side: TradeType,
                amount: Decimal,
                price: Decimal = s_decimal_NaN,
                is_maker: Optional[bool] = None) -> AddedToCostTradeFee:
        # https://www.hbg.com/en-us/about/fee/
        is_maker = order_type is OrderType.LIMIT_MAKER
        return estimate_fee("huobi", is_maker)

    async def _update_trading_rules(self):
        # The poll interval for trade rules is 60 seconds.
        last_tick = self._last_timestamp / 60.0
        current_tick = self.current_timestamp / 60.0
        if current_tick > last_tick or len(self._trading_rules) < 1:
            params = {'contract_type': 'swap'}
            exchange_info = await self._api_request("get", path_url=CONSTANTS.SWAP_INFO, params=params)
            trading_rules_list = self._format_trading_rules(exchange_info)
            self._trading_rules.clear()
            for trading_rule in trading_rules_list:
                self._trading_rules[trading_rule.trading_pair] = trading_rule

    def _format_trading_rules(self, raw_trading_pair_info: List[Dict[str, Any]]) -> List[TradingRule]:
        trading_rules = []

        for info in raw_trading_pair_info:
            try:
                if info['contract_status'] != 1:
                    continue
                min_order_size = Decimal(info["contract_size"])
                price_increment = Decimal("0.0001")  # info['price_tick'] normally
                size_increment = 1  # in contracts
                min_quote_amount_increment = price_increment * size_increment
                min_order_value = min_order_size * price_increment
                trading_rules.append(
                    TradingRule(trading_pair=info["contract_code"],
                                min_order_size=min_order_size,
                                min_price_increment=price_increment,
                                min_base_amount_increment=size_increment,
                                min_quote_amount_increment=min_quote_amount_increment,
                                min_order_value=min_order_value))
            except Exception:
                self.logger().error(f"Error parsing the trading pair rule {info}. Skipping.", exc_info=True)
        return trading_rules

    async def get_order_status(self, exchange_order_id: str, trading_pair: str) -> Dict[str, Any]:
        """
        Example:
        {
            "business_type": "futures",
            "contract_type": "quarter",
            "pair": "BTC-USDT",
            "symbol": "BTC",
            "contract_code": "BTC-USDT-211231",
            "volume": 1.000000000000000000,
            "price": 66000.000000000000000000,
            "order_price_type": "post_only",
            "order_type": 1,
            "direction": "sell",
            "offset": "open",
            "lever_rate": 1,
            "order_id": 917361800293453824,
            "client_order_id": null,
            "created_at": 1638757696945,
            "trade_volume": 0E-18,
            "trade_turnover": 0E-18,
            "fee": 0E-18,
            "trade_avg_price": null,
            "margin_frozen": 66.000000000000000000,
            "profit": 0E-18,
            "status": 3,
            "order_source": "api",
            "order_id_str": "917361800293453824",
            "fee_asset": "USDT",
            "liquidation_type": "0",
            "canceled_at": 0,
            "margin_asset": "USDT",
            "margin_account": "USDT",
            "margin_mode": "cross",
            "is_tpsl": 0,
            "real_profit": 0,
            "reduce_only": 0
        }
        """
        path_url = CONSTANTS.ORDER_DETAIL_URL
        params = {
            "order_id": exchange_order_id,
            "contract_code": trading_pair
        }
        return (await self._api_request("post", path_url=path_url, data=params, is_auth_required=True))[0]

    def parse_status(self, status):
        if status == 3:
            return OrderStatus.Submitted
        elif status == 4:
            return OrderStatus.ParialFilled
        elif status == 5:
            return OrderStatus.PartialFilledCanceled
        elif status == 6:
            return OrderStatus.Filled
        elif status == 7:
            return OrderStatus.Canceled
        else:
            # see https://huobiapi.github.io/docs/usdt_swap/v1/en/#cross-get-information-of-order
            raise RuntimeError(f"Unknown order status {status}")

    async def _update_order_status(self):
        self.logger().info("Update order status start")
        # The poll interval for order status is 10 seconds.
        last_tick = self._last_poll_timestamp / self.UPDATE_ORDERS_INTERVAL
        current_tick = self.current_timestamp / self.UPDATE_ORDERS_INTERVAL
        self.logger().info(f"_update_order_status {last_tick} {current_tick}")
        if current_tick > last_tick and len(self._in_flight_orders) > 0:
            tracked_orders = list(self._in_flight_orders.values())
            for tracked_order in tracked_orders:
                exchange_order_id = await tracked_order.get_exchange_order_id()
                try:
                    order_update = await self.get_order_status(exchange_order_id, tracked_order.trading_pair)
                except HuobiAPIError as e:
                    self.stop_tracking_order(tracked_order.client_order_id)
                    self.logger().info(f"Fail to retrieve order update for {tracked_order.client_order_id} - {e.error_payload}")
                    self.trigger_event(
                        self.MARKET_ORDER_FAILURE_EVENT_TAG,
                        MarketOrderFailureEvent(
                            self.current_timestamp,
                            tracked_order.client_order_id,
                            tracked_order.order_type
                        )
                    )
                    continue

                if order_update is None:
                    self.logger().network(
                        f"Error fetching status update for the order {tracked_order.client_order_id}: "
                        f"{order_update}.",
                        app_warning_msg=f"Could not fetch updates for the order {tracked_order.client_order_id}. "
                                        f"The order has either been filled or canceled."
                    )
                    continue

                order_status = self.parse_status(order_update['status'])

                # Calculate the newly executed amount for this update.
                tracked_order.last_state = str(order_status.value)
                new_confirmed_amount = Decimal(order_update["trade_volume"])  # probably typo in API (filled)
                execute_amount_diff = new_confirmed_amount - tracked_order.executed_amount_base

                if execute_amount_diff > s_decimal_0:
                    tracked_order.executed_amount_base = new_confirmed_amount
                    tracked_order.executed_amount_quote = Decimal(order_update["trade_turnover"])
                    tracked_order.fee_paid = Decimal(order_update["fee"])
                    execute_price = Decimal(order_update["trade_avg_price"])
                    order_filled_event = OrderFilledEvent(
                        self.current_timestamp,
                        tracked_order.client_order_id,
                        tracked_order.trading_pair,
                        tracked_order.trade_type,
                        tracked_order.order_type,
                        execute_price,
                        execute_amount_diff,
                        self.get_fee(
                            tracked_order.base_asset,
                            tracked_order.quote_asset,
                            tracked_order.order_type,
                            tracked_order.trade_type,
                            execute_price,
                            execute_amount_diff,
                        ),
                        # Unique exchange trade ID not available in client order status
                        # But can use validate an order using exchange order ID:
                        # https://huobiapi.github.io/docs/spot/v1/en/#query-order-by-order-id
                        exchange_trade_id=str(int(self._time() * 1e6))
                    )
                    self.logger().info(f"Filled {execute_amount_diff} out of {tracked_order.amount} of the "
                                       f"order {tracked_order.client_order_id}.")
                    self.trigger_event(self.MARKET_ORDER_FILLED_EVENT_TAG, order_filled_event)

                if tracked_order.is_open:
                    continue

                if tracked_order.is_done:
                    if not tracked_order.is_cancelled:  # Handles "filled" order
                        self.stop_tracking_order(tracked_order.client_order_id)
                        if tracked_order.trade_type is TradeType.BUY:
                            self.logger().info(f"The market buy order {tracked_order.client_order_id} has completed "
                                               f"according to order status API.")
                            self.trigger_event(
                                self.MARKET_BUY_ORDER_COMPLETED_EVENT_TAG,
                                BuyOrderCompletedEvent(
                                    self.current_timestamp,
                                    tracked_order.client_order_id,
                                    tracked_order.base_asset,
                                    tracked_order.quote_asset,
                                    tracked_order.fee_asset or tracked_order.base_asset,
                                    tracked_order.executed_amount_base,
                                    tracked_order.executed_amount_quote,
                                    tracked_order.fee_paid,
                                    tracked_order.order_type))
                        else:
                            self.logger().info(f"The market sell order {tracked_order.client_order_id} has completed "
                                               f"according to order status API.")
                            self.trigger_event(
                                self.MARKET_SELL_ORDER_COMPLETED_EVENT_TAG,
                                SellOrderCompletedEvent(
                                    self.current_timestamp,
                                    tracked_order.client_order_id,
                                    tracked_order.base_asset,
                                    tracked_order.quote_asset,
                                    tracked_order.fee_asset or tracked_order.quote_asset,
                                    tracked_order.executed_amount_base,
                                    tracked_order.executed_amount_quote,
                                    tracked_order.fee_paid,
                                    tracked_order.order_type))
                    else:  # Handles "canceled" or "partial-canceled" order
                        self.stop_tracking_order(tracked_order.client_order_id)
                        self.logger().info(f"The market order {tracked_order.client_order_id} "
                                           f"has been cancelled according to order status API.")
                        self.trigger_event(
                            self.MARKET_ORDER_CANCELLED_EVENT_TAG,
                            OrderCancelledEvent(self.current_timestamp,
                                                tracked_order.client_order_id))
        else:
            if current_tick > last_tick:
                self.logger().info("In flight orders are empty")
        self.logger().info("Update order status done")

    async def _status_polling_loop(self):
        while True:
            self.logger().info("_status_polling_loop run")
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
                self.logger().network("Unexpected error while fetching account updates.",
                                      exc_info=True,
                                      app_warning_msg="Could not fetch account updates from Huobi. "
                                                      "Check API key and network connection.")
                await asyncio.sleep(0.5)
            finally:
                self.logger().info("_status_polling_loop done")

    async def _trading_rules_polling_loop(self):
        while True:
            try:
                await self._update_trading_rules()
                await asyncio.sleep(60 * 5)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network("Unexpected error while fetching trading rules.",
                                      exc_info=True,
                                      app_warning_msg="Could not fetch new trading rules from Huobi. "
                                                      "Check network connection.")
                await asyncio.sleep(0.5)

    async def _iter_user_stream_queue(self) -> AsyncIterable[Dict[str, Any]]:
        while True:
            try:
                yield await self._user_stream_tracker.user_stream.get()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Unknown error. Retrying after 1 second. {e}", exc_info=True)
                await asyncio.sleep(1.0)

    async def _user_stream_event_listener(self):
        async for stream_message in self._iter_user_stream_queue():
            # self.logger().info(f"Stream message {stream_message}")
            try:
                channel = stream_message.get("topic", None)
                data = stream_message.get("data", [])
                if len(data) == 0 and stream_message.get("err-code", -1) == 0:
                    # This is a subcribtion confirmation.
                    self.logger().info(f"Successfully subscribed to {channel}")
                    continue

                if channel in [CONSTANTS.HUOBI_ACCOUNT_UPDATE_TOPIC, CONSTANTS.HUOBI_ACCOUNT_UPDATE_TOPIC2] or \
                        CONSTANTS.HUOBI_ACCOUNT_UPDATE_TOPIC2 in channel:
                    for asset in data:
                        asset_name = asset["margin_asset"].upper()
                        balance = asset["margin_balance"]
                        available_balance = asset["margin_static"]  # TODO not sure here, see https://huobiapi.github.io/docs/usdt_swap/v1/en/#cross-subscribe-account-equity-updates-data-sub

                        self._account_balances.update({asset_name: Decimal(balance)})
                        self._account_available_balances.update({asset_name: Decimal(available_balance)})
                elif CONSTANTS.HUOBI_ORDER_UPDATE_TOPIC2 in channel:
                    safe_ensure_future(self._process_order_update(stream_message))
                elif CONSTANTS.HUOBI_POSITION_UPDATE_TOPIC2 in channel:
                    await self._update_positions(data)
                else:
                    self.logger().info(f"Dont know how to handle stream message {stream_message}")

            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Unexpected error in user stream listener loop. {e}", exc_info=True)
                await asyncio.sleep(5.0)

    async def _process_order_update(self, order_update: Dict[str, Any]):
        client_order_id = str(order_update["client_order_id"])
        order_status = self.parse_status(order_update["status"])

        tracked_order = self._in_flight_orders.get(client_order_id, None)

        if tracked_order is None:
            return

        if order_status == OrderStatus.ParialFilled:
            for trade in order_update['trade']:
                execute_price = Decimal(trade["trade_price"])
                execute_amount_diff = Decimal(trade["trade_volume"])

                tracked_order = self._in_flight_orders.get(client_order_id, None)

                if tracked_order:
                    updated = tracked_order.update_with_trade_update(trade)
                    if updated:
                        self.logger().info(
                            f"Filled {execute_amount_diff} out of {tracked_order.amount} of order "
                            f"{tracked_order.order_type.name}-{tracked_order.client_order_id}")
                        self.trigger_event(
                            self.MARKET_ORDER_FILLED_EVENT_TAG,
                            OrderFilledEvent(
                                self.current_timestamp,
                                tracked_order.client_order_id,
                                tracked_order.trading_pair,
                                tracked_order.trade_type,
                                tracked_order.order_type,
                                execute_price,
                                execute_amount_diff,
                                AddedToCostTradeFee(
                                    flat_fees=[
                                        TokenAmount(tracked_order.fee_asset, Decimal(trade["trade_fee"]))
                                    ]
                                ),
                                exchange_trade_id=str(trade["id"])
                            ))
        if order_status == OrderStatus.Filled:
            tracked_order.last_state = str(order_status.value)

            event = (self.MARKET_BUY_ORDER_COMPLETED_EVENT_TAG
                     if tracked_order.trade_type == TradeType.BUY
                     else self.MARKET_SELL_ORDER_COMPLETED_EVENT_TAG)
            event_class = (BuyOrderCompletedEvent
                           if tracked_order.trade_type == TradeType.BUY
                           else SellOrderCompletedEvent)

            try:
                await asyncio.wait_for(tracked_order.wait_until_completely_filled(), timeout=1)
            except asyncio.TimeoutError:
                self.logger().warning(
                    f"The order fill updates did not arrive on time for {tracked_order.client_order_id}. "
                    f"The complete update will be processed with estimated fees.")

            self.logger().info(f"The {tracked_order.trade_type.name} order {tracked_order.client_order_id} "
                               f"has completed according to order delta websocket API.")
            self.trigger_event(event,
                               event_class(
                                   self.current_timestamp,
                                   tracked_order.client_order_id,
                                   tracked_order.base_asset,
                                   tracked_order.quote_asset,
                                   tracked_order.fee_asset or tracked_order.quote_asset,
                                   tracked_order.executed_amount_base,
                                   tracked_order.executed_amount_quote,
                                   tracked_order.fee_paid,
                                   tracked_order.order_type
                               ))
            self.stop_tracking_order(tracked_order.client_order_id)

        if order_status == OrderStatus.Canceled:
            tracked_order.last_state = str(order_status.value)
            self.logger().info(f"The order {tracked_order.client_order_id} has been cancelled "
                               f"according to order delta websocket API.")
            self.trigger_event(
                self.MARKET_ORDER_CANCELLED_EVENT_TAG,
                OrderCancelledEvent(self.current_timestamp,
                                    tracked_order.client_order_id))
            self.stop_tracking_order(tracked_order.client_order_id)

    @property
    def status_dict(self) -> Dict[str, bool]:
        return {
            "order_books_initialized": self._order_book_tracker.ready,
            "account_balance": len(self._account_balances) > 0 if self._trading_required else True,
            "trading_rule_initialized": len(self._trading_rules) > 0,
            "positions_initialized": self._positions_initialized
        }

    @property
    def ready(self) -> bool:
        return all(self.status_dict.values())

    def supported_order_types(self):
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER]

    async def place_order(self,
                          order_id: str,
                          trading_pair: str,
                          amount: Decimal,
                          is_buy: bool,
                          order_type: OrderType,
                          price: Decimal,
                          position_action: PositionAction) -> str:
        if trading_pair == 'LOOKS-USDT':
            amount *= 10
        path_url = CONSTANTS.PLACE_ORDER_URL
        side = "buy" if is_buy else "sell"
        # source: https://github.com/hbdmapi/huobi_futures_Python/blob/master/alpha/platforms/huobi_usdt_swap_cross_trade.py#L306
        order_type_str = "limit" if order_type == OrderType.LIMIT else "optimal_20" if order_type == OrderType.MARKET else "post_only"
        # TODO see https://huobiapi.github.io/docs/usdt_swap/v1/en/#cross-place-an-order for other options like IOC (immediate or cancel)
        params = {
            "price": f"{price:f}",
            "volume": f"{amount:f}",
            "client_order_id": int(order_id),
            "contract_code": convert_to_exchange_trading_pair(trading_pair),
            "direction": side,
            "lever_rate": self._leverage.get(trading_pair, 20),
            "order_price_type": order_type_str,
            # "reduce_only": 1 if position_action == PositionAction.CLOSE else 0
            "offset": "close" if position_action == PositionAction.CLOSE else "open"
        }
        self.logger().info(f"Place Order params {params}")
        exchange_order_id = await self._api_request(
            "post",
            path_url=path_url,
            params=params,
            data=params,
            is_auth_required=True
        )
        return exchange_order_id["order_id_str"]

    async def execute_buy(self,
                          order_id: str,
                          trading_pair: str,
                          amount: Decimal,
                          order_type: OrderType,
                          price: Optional[Decimal] = s_decimal_0,
                          position_action: PositionAction = None):
        trading_rule: TradingRule = self._trading_rules[trading_pair]

        decimal_amount = self.quantize_order_amount(trading_pair, amount)  # not needed for Maker but set them anyway
        decimal_price = self.quantize_order_price(trading_pair, price)
        if order_type is OrderType.LIMIT or order_type is OrderType.LIMIT_MAKER:
            if decimal_amount < trading_rule.min_order_size:
                raise ValueError(f"Buy order amount {decimal_amount} is lower than the minimum order size "
                                 f"{trading_rule.min_order_size}.")

        try:
            exchange_order_id = await self.place_order(order_id, trading_pair, decimal_amount, True, order_type, decimal_price, position_action)
            self.start_tracking_order(
                client_order_id=order_id,
                exchange_order_id=exchange_order_id,
                trading_pair=trading_pair,
                order_type=order_type,
                trade_type=TradeType.BUY,
                price=decimal_price,
                amount=decimal_amount
            )
            tracked_order = self._in_flight_orders.get(order_id)
            if tracked_order is not None:
                self.logger().info(f"Created {order_type} buy order {order_id} for {decimal_amount} {trading_pair}.")
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
            self.stop_tracking_order(order_id)
            order_type_str = order_type.name.lower()
            self.logger().network(
                f"Error submitting buy {order_type_str} order to Huobi for "
                f"{decimal_amount} {trading_pair} "
                f"{decimal_price}.",
                exc_info=True,
                app_warning_msg="Failed to submit buy order to Huobi. Check API key and network connection."
            )
            self.trigger_event(
                self.MARKET_ORDER_FAILURE_EVENT_TAG,
                MarketOrderFailureEvent(self.current_timestamp, order_id, order_type))

    def buy(self, trading_pair: str, amount: Decimal, order_type=OrderType.MARKET,
            price: Decimal = s_decimal_NaN, position_action=None, **kwargs) -> str:
        order_id: str = get_new_client_order_id(TradeType.BUY, trading_pair)

        safe_ensure_future(self.execute_buy(order_id, trading_pair, amount, order_type, price, position_action))
        return order_id

    async def execute_sell(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           order_type: OrderType,
                           price: Optional[Decimal] = s_decimal_0,
                           position_action: PositionAction = None):
        trading_rule: TradingRule = self._trading_rules[trading_pair]

        decimal_amount = self.quantize_order_amount(trading_pair, amount)
        decimal_price = self.quantize_order_price(trading_pair, price)

        if decimal_amount < trading_rule.min_order_size:
            raise ValueError(f"Sell order amount {decimal_amount} is lower than the minimum order size "
                             f"{trading_rule.min_order_size}.")

        try:
            exchange_order_id = await self.place_order(order_id, trading_pair, decimal_amount, False, order_type, decimal_price, position_action)
            self.start_tracking_order(
                client_order_id=order_id,
                exchange_order_id=exchange_order_id,
                trading_pair=trading_pair,
                order_type=order_type,
                trade_type=TradeType.SELL,
                price=decimal_price,
                amount=decimal_amount
            )
            tracked_order = self._in_flight_orders.get(order_id)
            if tracked_order is not None:
                self.logger().info(f"Created {order_type} sell order {order_id} for {decimal_amount} {trading_pair}.")
            self.trigger_event(
                self.MARKET_SELL_ORDER_CREATED_EVENT_TAG,
                SellOrderCreatedEvent(
                    self.current_timestamp,
                    order_type,
                    trading_pair,
                    decimal_amount,
                    decimal_price,
                    order_id,
                    tracked_order.creation_timestamp
                ))
        except asyncio.CancelledError:
            raise
        except Exception:
            self.stop_tracking_order(order_id)
            order_type_str = order_type.name.lower()
            self.logger().network(
                f"Error submitting sell {order_type_str} order to Huobi for "
                f"{decimal_amount} {trading_pair} "
                f"{decimal_price}.",
                exc_info=True,
                app_warning_msg="Failed to submit sell order to Huobi. Check API key and network connection."
            )
            self.trigger_event(
                self.MARKET_ORDER_FAILURE_EVENT_TAG,
                MarketOrderFailureEvent(self.current_timestamp, order_id, order_type))

    def sell(self, trading_pair: str, amount: Decimal, order_type=OrderType.MARKET,
             price: Decimal = s_decimal_NaN, position_action=None, **kwargs) -> str:
        order_id = get_new_client_order_id(TradeType.SELL, trading_pair)
        safe_ensure_future(self.execute_sell(order_id, trading_pair, amount, order_type, price, position_action))
        return order_id

    async def execute_cancel(self, trading_pair: str, order_id: str):
        # TODO fire event, or find what needs to fire it
        try:
            tracked_order = self._in_flight_orders.get(order_id)
            if tracked_order is None:
                raise ValueError(f"Failed to cancel order - {order_id}. Order not found.")
            path_url = CONSTANTS.CANCEL_ORDER_URL
            exchange_order_id = tracked_order.exchange_order_id
            params = {'order_id': exchange_order_id, 'contract_code': trading_pair}
            data = await self._api_request("post", path_url=path_url, data=params, is_auth_required=True)
            successes = data["successes"].split(',')
            if exchange_order_id in successes:
                # TODO maybe don't do this
                # self.stop_tracking_order(order_id)
                return order_id
            else:
                for error in data['errors']:
                    if exchange_order_id == str(error.get('order_id', None)):
                        message = error['err_msg']
                        if 'has been executed' in message:
                            self.logger().info(f"Order has already been executed {order_id}")
                            return order_id

                        if 'doesnt exist' in message or 'has been canceled' in message:
                            self.logger().info(f"Order not found so assume it is canceled {order_id}")
                            # self.stop_tracking_order(order_id)
                            return order_id

                self.logger().info(f"Could not cancel order {order_id} {exchange_order_id}: {data}")
                return None
        except Exception as e:
            if "Order not found" in str(e):
                self.logger().info(f"Order not found so assume it is canceled {order_id}")
                # self.stop_tracking_order(order_id)
                return order_id

            self.logger().network(
                f"Failed to cancel order {order_id}: {str(e)}",
                exc_info=True,
                app_warning_msg=f"Failed to cancel the order {order_id} on Huobi. "
                                f"Check API key and network connection."
            )

    def cancel(self, trading_pair: str, order_id: str):
        safe_ensure_future(self.execute_cancel(trading_pair, order_id))
        return order_id

    async def cancel_all(self, timeout_seconds: float) -> List[CancellationResult]:
        open_orders = [o for o in self._in_flight_orders.values() if o.is_open]
        if len(open_orders) == 0:
            return []
        cancel_orders = [(o.client_order_id, o.trading_pair) for o in open_orders]
        self.logger().debug(f"cancel_orders {cancel_orders} {open_orders}")
        results = []
        for client_order_id, trading_pair in cancel_orders:
            result = await self.execute_cancel(trading_pair, client_order_id)
            if result:
                results.append(CancellationResult(client_order_id, True))
            else:
                results.append(CancellationResult(client_order_id, False))

        return results

    def get_order_book(self, trading_pair: str) -> OrderBook:
        order_books = self._order_book_tracker.order_books

        if trading_pair not in order_books:
            raise ValueError(f"No order book exists for '{trading_pair}'.")
        return order_books.get(trading_pair)

    def did_timeout_tx(self, tracking_id: str):
        self.trigger_event(self.MARKET_TRANSACTION_FAILURE_EVENT_TAG,
                           MarketTransactionFailureEvent(self.current_timestamp, tracking_id))

    def start_tracking_order(self,
                             client_order_id: str,
                             exchange_order_id: str,
                             trading_pair: str,
                             order_type: OrderType,
                             trade_type: TradeType,
                             price: Decimal,
                             amount: Decimal):
        """Helper method for testing."""
        self._in_flight_orders[client_order_id] = HuobiInFlightOrder(
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=trading_pair,
            order_type=order_type,
            trade_type=trade_type,
            price=price,
            amount=amount,
            creation_timestamp=self.current_timestamp
        )

    def stop_tracking_order(self, order_id: str):
        if order_id in self._in_flight_orders:
            del self._in_flight_orders[order_id]

    def get_order_price_quantum(self, trading_pair: str, price):
        trading_rule = self._trading_rules[trading_pair]
        return trading_rule.min_price_increment

    def get_order_size_quantum(self, trading_pair: str, order_size):
        trading_rule = self._trading_rules[trading_pair]
        return Decimal(trading_rule.min_base_amount_increment)

    def quantize_order_amount(self, trading_pair: str, amount, price=s_decimal_0):
        trading_rule = self._trading_rules[trading_pair]
        quantized_amount = ExchangeBase.quantize_order_amount(self, trading_pair, amount)

        # Check against min_order_size. If not passing check, return 0.
        if quantized_amount < trading_rule.min_order_size:
            return s_decimal_0

        # Check against max_order_size. If not passing check, return maximum.
        if quantized_amount > trading_rule.max_order_size:
            return trading_rule.max_order_size

        notional_size = price * quantized_amount
        # Add 1% as a safety factor in case the prices changed while making the order.
        if notional_size < trading_rule.min_notional_size * Decimal("1.01"):
            return s_decimal_0

        return quantized_amount

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

    def set_leverage(self, trading_pair: str, leverage: int = 1):
        self._leverage[trading_pair] = leverage

    def set_trading_pair_position_mode(self, trading_pair: str, position_mode: PositionMode):
        safe_ensure_future(self._set_trading_pair_position_mode(trading_pair, position_mode))

    async def _set_trading_pair_position_mode(self, trading_pair, position_mode: PositionMode):
        initial_mode = await self._get_trading_pair_position_mode(trading_pair)
        if initial_mode != position_mode:
            exchange_position_mode = "dual_side" if position_mode == PositionMode.HEDGE else "single_side"
            params = {
                "margin_account": "USDT",
                "position_mode": exchange_position_mode
            }
            data = await self._api_request("post", path_url=CONSTANTS.SWITCH_POSITION, data=params, is_auth_required=True)
            if data["position_mode"] == exchange_position_mode and data["margin_account"] == trading_pair:
                self.logger().info(f"{trading_pair} using {position_mode.name} position mode.")
            else:
                self.logger().error(f"Unexpected response set position mode: {data}")
        else:
            self.logger().info(f"{trading_pair} already in {position_mode.name} position mode.")

    async def _get_trading_pair_position_mode(self, trading_pair: str) -> Optional[PositionMode]:
        # To-do: ensure there's no active order or contract before changing position mode

        params = {"margin_account": "USDT"}
        data = await self._api_request("post", path_url=CONSTANTS.POSITION_INFO, data=params, is_auth_required=True)

        return PositionMode.HEDGE if data['position_mode'] == 'dual_side' else PositionMode.ONEWAY


class HuobiPerpetualDerivativeTransactionTracker(TransactionTracker):
    _owner: HuobiPerpetualDerivative = None

    def __init__(self, owner: HuobiPerpetualDerivative):
        super().__init__()
        self._owner = owner

    def did_timeout_tx(self, tx_id: str):
        TransactionTracker.did_timeout_tx(self, tx_id)
        self._owner.did_timeout_tx(tx_id)
