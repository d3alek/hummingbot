from decimal import Decimal
from typing import (
    Any,
    Dict,
    Optional,
    Tuple,
)
import asyncio
from hummingbot.core.event.events import (
    OrderType,
    TradeType
)
from hummingbot.connector.in_flight_order_base import InFlightOrderBase


class DigifinexInFlightOrder(InFlightOrderBase):
    def __init__(self,
                 client_order_id: str,
                 exchange_order_id: Optional[str],
                 trading_pair: str,
                 order_type: OrderType,
                 trade_type: TradeType,
                 price: Decimal,
                 amount: Decimal,
                 creation_timestamp: float,
                 initial_state: str = "OPEN"):
        super().__init__(
            client_order_id,
            exchange_order_id,
            trading_pair,
            order_type,
            trade_type,
            price,
            amount,
            creation_timestamp,
            initial_state,
        )
        self.trade_id_set = set()
        self.cancelled_event = asyncio.Event()

    @property
    def is_done(self) -> bool:
        return self.last_state in {"2", "3", "4"}

    @property
    def is_failure(self) -> bool:
        return False
        # return self.last_state in {"REJECTED"}

    @property
    def is_cancelled(self) -> bool:
        return self.last_state in {"3", "4"}

    def update_with_rest_order_detail(self, trade_update: Dict[str, Any]) -> bool:
        """
        Updates the in flight order with trade update (from private/get-order-detail end point)
        return: True if the order gets updated otherwise False
        """
        trade_id = trade_update["tid"]
        # trade_update["orderId"] is type int
        if trade_id in self.trade_id_set:
            # trade already recorded
            return False
        self.trade_id_set.add(trade_id)
        self.executed_amount_base += Decimal(str(trade_update["executed_amount"]))
        # self.fee_paid += Decimal(str(trade_update["fee"]))
        self.executed_amount_quote += (Decimal(str(trade_update["executed_price"])) *
                                       Decimal(str(trade_update["executed_amount"])))
        # if not self.fee_asset:
        #     self.fee_asset = trade_update["fee_currency"]
        return True

    def update_with_order_update(self, order_update) -> Tuple[Decimal, Decimal]:
        """
        Updates the in flight order with trade update (from order message)
        return: (delta_trade_amount, delta_trade_price)
        """

        # todo: order_msg contains no trade_id. may be re-processed
        if order_update['filled'] == '0':
            return (0, 0)

        self.trade_id_set.add("N/A")
        executed_amount_base = Decimal(order_update['filled'])
        if executed_amount_base == self.executed_amount_base:
            return (0, 0)
        delta_trade_amount = executed_amount_base - self.executed_amount_base
        self.executed_amount_base = executed_amount_base

        executed_amount_quote = executed_amount_base * Decimal(order_update['price_avg'] or order_update['price'])
        delta_trade_price = (executed_amount_quote - self.executed_amount_quote) / delta_trade_amount
        self.executed_amount_quote = executed_amount_base * Decimal(order_update['price_avg'] or order_update['price'])
        return (delta_trade_amount, delta_trade_price)
