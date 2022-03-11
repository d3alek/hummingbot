from decimal import Decimal
from typing import (
    Any,
    Dict
)

from hummingbot.connector.in_flight_order_base import InFlightOrderBase
from hummingbot.core.event.events import (
    OrderType,
    TradeType
)

cdef class HuobiInFlightOrder(InFlightOrderBase):
    def __init__(self,
                 client_order_id: str,
                 exchange_order_id: str,
                 trading_pair: str,
                 order_type: OrderType,
                 trade_type: TradeType,
                 price: Decimal,
                 amount: Decimal,
                 creation_timestamp: float,
                 initial_state: OrderStatus = OrderStatus.Submitted):
        super().__init__(
            client_order_id,
            exchange_order_id,
            trading_pair,
            order_type,
            trade_type,
            price,
            amount,
            creation_timestamp,
            str(initial_state.value), # serializing enum
        )

        self.trade_id_set = set()

    @property
    def last_state_enum(self) -> OrderStatus:
        return OrderStatus(int(self.last_state))

    @property
    def is_done(self) -> bool:
        return self.last_state_enum in {OrderStatus.Filled, OrderStatus.Canceled, OrderStatus.PartialFilledCanceled}

    @property
    def is_cancelled(self) -> bool:
        return self.last_state_enum in {OrderStatus.PartialFilledCanceled, OrderStatus.Canceled}

    @property
    def is_failure(self) -> bool:
        return self.last_state_enum in {OrderStatus.Canceled}

    @property
    def is_open(self) -> bool:
        return self.last_state_enum in {OrderStatus.Submitted, OrderStatus.ParialFilled}

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> InFlightOrderBase:
        order = super().from_json(data)
        order.check_filled_condition()
        return order

    def update_with_trade_update(self, trade_update: Dict[str, Any]) -> bool:
        """
        Updates the in flight order with trade update (from GET /trade_history end point)
        :param trade_update: the event message received for the order fill (or trade event)
        :return: True if the order gets updated otherwise False
        """
        trade_id = trade_update["trade_id"]
        if trade_id in self.trade_id_set:
            return False
        self.trade_id_set.add(trade_id)
        trade_amount = Decimal(str(trade_update["trade_volume"]))
        trade_price = Decimal(str(trade_update["trade_price"]))
        quote_amount = trade_amount * trade_price

        self.executed_amount_base += trade_amount
        self.executed_amount_quote += quote_amount
        self.fee_paid += Decimal(str(trade_update["trade_fee"]))
        self.fee_asset = trade_update["fee_asset"].upper()

        self.check_filled_condition()

        return True
