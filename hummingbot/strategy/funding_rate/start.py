from decimal import Decimal
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.funding_rate.funding_rate import FundingRateStrategy
from hummingbot.strategy.funding_rate.funding_rate_config_map import funding_rate_config_map
from hummingbot.core.event.events import OrderType


def start(self):
    short = funding_rate_config_map.get("short").value.lower()
    long = funding_rate_config_map.get("long").value.lower()
    short_order_type = funding_rate_config_map.get("short_order_type").value
    if short_order_type.lower() == 'limit':
        short_order_type = OrderType.LIMIT_MAKER
    else:
        short_order_type = OrderType.MARKET
    trading_pair = funding_rate_config_map.get("trading_pair").value

    total_amount = funding_rate_config_map.get("total_amount").value
    chunk_size = funding_rate_config_map.get("chunk_size").value
    action_open = funding_rate_config_map.get("action_open").value

    limit_slip = funding_rate_config_map.get("limit_slip").value / Decimal("100")
    market_delta = funding_rate_config_map.get("market_delta").value / Decimal("100")

    self._initialize_markets([(short, [trading_pair]), (long, [trading_pair])])
    base, quote = trading_pair.split("-")

    short_info = MarketTradingPairTuple(self.markets[short], trading_pair, base, quote)
    long_info = MarketTradingPairTuple(self.markets[long], trading_pair, base, quote)

    self.market_trading_pair_tuples = [short_info, long_info]
    self.strategy = FundingRateStrategy()
    self.strategy.init_params(short_info=short_info,
                              long_info=long_info,
                              short_order_type=short_order_type,
                              total_amount=total_amount,
                              chunk_size=chunk_size,
                              action_open=action_open,
                              limit_slip=limit_slip,
                              market_delta=market_delta)
