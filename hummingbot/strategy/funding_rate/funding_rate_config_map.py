from hummingbot.client.config.config_var import ConfigVar
from hummingbot.client.config.config_validators import (
    validate_market_trading_pair,
    validate_derivative,
    validate_decimal,
    validate_int
)
from hummingbot.client.settings import (
    required_exchanges,
    requried_connector_trading_pairs,
    AllConnectorSettings,
)
from decimal import Decimal


def exchange_on_validated(value: str) -> None:
    required_exchanges.append(value)


def perpetual_market_validator(value: str) -> None:
    exchange1 = funding_rate_config_map["perpetual_connector1"].value
    exchange2 = funding_rate_config_map["perpetual_connector2"].value
    return validate_market_trading_pair(exchange1, value) and validate_market_trading_pair(exchange2, value)


def perpetual_market_on_validated(value: str) -> None:
    requried_connector_trading_pairs[funding_rate_config_map["perpetual_connector1"].value] = [value]
    requried_connector_trading_pairs[funding_rate_config_map["perpetual_connector2"].value] = [value]


def perpetual_market_prompt() -> str:
    connector1 = funding_rate_config_map.get("perpetual_connector1").value
    connector2 = funding_rate_config_map.get("perpetual_connector2").value
    example = AllConnectorSettings.get_example_pairs().get(connector1)
    return "Enter the token trading pair you would like to trade on %s%s >>> " \
           % (f"{connector1} and {connector2}", f" (e.g. {example})" if example else "")


def order_amount_prompt() -> str:
    trading_pair = funding_rate_config_map["perpetual_market"].value
    base_asset, quote_asset = trading_pair.split("-")
    return f"What is the amount of {base_asset} per order? >>> "


funding_rate_config_map = {
    "strategy": ConfigVar(
        key="strategy",
        prompt="",
        default="funding_rate"),
    "perpetual_connector1": ConfigVar(
        key="perpetual_connector1",
        prompt="Enter a derivative name 1 (Exchange/AMM) >>> ",
        prompt_on_new=True,
        validator=validate_derivative,
        on_validated=exchange_on_validated),
    "perpetual_connector2": ConfigVar(
        key="perpetual_connector2",
        prompt="Enter a derivative name 2 (Exchange/AMM) >>> ",
        prompt_on_new=True,
        validator=validate_derivative,
        on_validated=exchange_on_validated),
    "perpetual_market": ConfigVar(
        key="perpetual_market",
        prompt=perpetual_market_prompt,
        prompt_on_new=True,
        validator=perpetual_market_validator,
        on_validated=perpetual_market_on_validated),
    "order_amount": ConfigVar(
        key="order_amount",
        prompt=order_amount_prompt,
        type_str="decimal",
        prompt_on_new=True),
    "perpetual_leverage": ConfigVar(
        key="perpetual_leverage",
        prompt="How much leverage would you like to use on the perpetual exchange? (Enter 1 to indicate 1X) >>> ",
        type_str="int",
        default=1,
        validator= lambda v: validate_int(v),
        prompt_on_new=True),
    "min_opening_funding_rate_pct": ConfigVar(
        key="min_opening_funding_rate_pct",
        prompt="What is the minimum funding rate percentage between the two perpetual markets before opening "
               "a position? (Enter 1 to indicate 1%) >>> ",
        prompt_on_new=True,
        default=Decimal("1"),
        validator=lambda v: validate_decimal(v, Decimal(-100), 100, inclusive=False),
        type_str="decimal"),
    "min_closing_funding_rate_pct": ConfigVar(
        key="min_closing_funding_rate_pct",
        prompt="What is the minimum funding_rate percentage between the spot and perpetual market price before closing "
               "an existing position? (Enter 1 to indicate 1%) (This can be negative value to close out the "
               "position with lesser profit at higher chance of closing) >>> ",
        prompt_on_new=True,
        default=Decimal("-0.1"),
        validator=lambda v: validate_decimal(v, Decimal(-100), 100, inclusive=False),
        type_str="decimal"),
    "perpetual_market_slippage_buffer": ConfigVar(
        key="perpetual_market_slippage_buffer",
        prompt="How much buffer do you want to add to the price to account for slippage for orders on the perpetual "
               "market (Enter 1 for 1%)? >>> ",
        prompt_on_new=True,
        default=Decimal("0.05"),
        validator=lambda v: validate_decimal(v),
        type_str="decimal"),
    "next_opening_delay": ConfigVar(
        key="next_opening_delay",
        prompt="How long do you want the strategy to wait before opening the next position (in seconds)?",
        type_str="float",
        validator=lambda v: validate_decimal(v, min_value=0, inclusive=False),
        default=120),
    "next_closing_delay": ConfigVar(
        key="next_closing_delay",
        prompt="How long do you want the strategy to wait before closing the position (in seconds)?",
        type_str="float",
        validator=lambda v: validate_decimal(v, min_value=0, inclusive=False),
        default=120),
}
