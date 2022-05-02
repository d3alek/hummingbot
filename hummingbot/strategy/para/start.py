from decimal import Decimal
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.para.para import ParaStrategy
from hummingbot.strategy.para.para_config_map import para_config_map
from hummingbot.core.event.events import OrderType


def start(self):
    short = para_config_map.get("short").value.lower()
    long = para_config_map.get("long").value.lower()
    short_order_type = para_config_map.get("short_order_type").value
    if short_order_type.lower() == 'limit':
        short_order_type = OrderType.LIMIT_MAKER
    else:
        short_order_type = OrderType.MARKET
    trading_pair = para_config_map.get("trading_pair").value

    total_amount = para_config_map.get("total_amount").value
    chunk_size = para_config_map.get("chunk_size").value
    action_open = para_config_map.get("action_open").value

    limit_slip = para_config_map.get("limit_slip").value / Decimal("100")
    market_delta = para_config_map.get("market_delta").value / Decimal("100")

    self._initialize_markets([(short, [trading_pair]), (long, [trading_pair])])
    base, quote = trading_pair.split("-")

    short_info = MarketTradingPairTuple(self.markets[short], trading_pair, base, quote)
    long_info = MarketTradingPairTuple(self.markets[long], trading_pair, base, quote)

    self.market_trading_pair_tuples = [short_info, long_info]
    self.strategy = ParaStrategy()
    self.strategy.init_params(short_info=short_info,
                              long_info=long_info,
                              short_order_type=short_order_type,
                              total_amount=total_amount,
                              chunk_size=chunk_size,
                              action_open=action_open,
                              limit_slip=limit_slip,
                              market_delta=market_delta)
