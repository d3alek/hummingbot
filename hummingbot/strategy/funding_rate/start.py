from decimal import Decimal
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.funding_rate.funding_rate import FundingRateStrategy
from hummingbot.strategy.funding_rate.funding_rate_config_map import funding_rate_config_map


def start(self):
    perpetual_connector1 = funding_rate_config_map.get("perpetual_connector1").value.lower()
    perpetual_connector2 = funding_rate_config_map.get("perpetual_connector2").value.lower()
    perpetual_market = funding_rate_config_map.get("perpetual_market").value
    order_amount = funding_rate_config_map.get("order_amount").value
    perpetual_leverage = funding_rate_config_map.get("perpetual_leverage").value
    min_opening_funding_rate_pct = funding_rate_config_map.get("min_opening_funding_rate_pct").value / Decimal("100")
    min_closing_funding_rate_pct = funding_rate_config_map.get("min_closing_funding_rate_pct").value / Decimal("100")
    perpetual_market_slippage_buffer = funding_rate_config_map.get("perpetual_market_slippage_buffer").value / Decimal("100")
    next_opening_delay = funding_rate_config_map.get("next_opening_delay").value
    next_closing_delay = funding_rate_config_map.get("next_closing_delay").value

    self._initialize_markets([(perpetual_connector1, [perpetual_market]), (perpetual_connector2, [perpetual_market])])
    base, quote = perpetual_market.split("-")

    perpetual_market1_info = MarketTradingPairTuple(self.markets[perpetual_connector1], perpetual_market, base, quote)
    perpetual_market2_info = MarketTradingPairTuple(self.markets[perpetual_connector2], perpetual_market, base, quote)

    self.market_trading_pair_tuples = [perpetual_market1_info, perpetual_market2_info]
    self.strategy = FundingRateStrategy()
    self.strategy.init_params(perpetual_market1_info,
                              perpetual_market2_info,
                              order_amount,
                              perpetual_leverage,
                              min_opening_funding_rate_pct,
                              min_closing_funding_rate_pct,
                              perpetual_market_slippage_buffer,
                              next_opening_delay,
                              next_closing_delay)
