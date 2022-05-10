from hummingbot.client.config.config_var import ConfigVar
from hummingbot.client.config.config_validators import (
    validate_market_trading_pair,
    validate_connector,
    validate_decimal,
    validate_bool
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
    exchange1 = para_config_map["short"].value
    exchange2 = para_config_map["long"].value
    return validate_market_trading_pair(exchange1, value) and validate_market_trading_pair(exchange2, value)


def perpetual_market_on_validated(value: str) -> None:
    requried_connector_trading_pairs[para_config_map["short"].value] = [value]
    requried_connector_trading_pairs[para_config_map["long"].value] = [value]


def perpetual_market_prompt() -> str:
    connector1 = para_config_map.get("short").value
    connector2 = para_config_map.get("long").value
    example = AllConnectorSettings.get_example_pairs().get(connector1)
    return "Enter the token trading pair you would like to trade on %s%s >>> " \
           % (f"{connector1} and {connector2}", f" (e.g. {example})" if example else "")


def order_amount_prompt() -> str:
    trading_pair = para_config_map["trading_pair"].value
    base_asset, quote_asset = trading_pair.split("-")
    return f"What is the amount of {base_asset} per order? >>> "


def total_amount_prompt() -> str:
    trading_pair = para_config_map["trading_pair"].value
    base_asset, quote_asset = trading_pair.split("-")
    return f"What is the total of {base_asset} to trade per exchange? >>> "


def validate_order_type(value):
    options = {"limit", "market"}
    if value.lower() not in options:
        return f"Invalid order type, please choose value from {options}"


para_config_map = {
    "strategy": ConfigVar(
        key="strategy",
        prompt="",
        default="para"),
    "short": ConfigVar(
        key="short",
        prompt="Where to SHORT (Exchange/AMM) >>> ",
        prompt_on_new=True,
        validator=validate_connector,
        on_validated=exchange_on_validated),
    "long": ConfigVar(
        key="long",
        prompt="Where to LONG (Exchange/AMM) >>> ",
        prompt_on_new=True,
        validator=validate_connector,
        on_validated=exchange_on_validated),
    "short_order_type": ConfigVar(
        key="short_order_type",
        prompt="Either LIMIT or MARKET >>> ",
        prompt_on_new=True,
        validator=lambda v: validate_order_type(v)),
    "trading_pair": ConfigVar(
        key="trading_pair",
        prompt=perpetual_market_prompt,
        prompt_on_new=True,
        validator=perpetual_market_validator,
        on_validated=perpetual_market_on_validated),
    "total_amount": ConfigVar(
        key="total_amount",
        prompt=total_amount_prompt,
        type_str="decimal",
        prompt_on_new=True),
    "chunk_size": ConfigVar(
        key="chunk_size",
        prompt=order_amount_prompt,
        type_str="decimal",
        prompt_on_new=True),
    "action_open": ConfigVar(
        key="action_open",
        prompt="If True, OPEN positions, if False, CLOSE",
        validator=lambda v: validate_bool(v),
        type_str="bool",
        prompt_on_new=True),
    "limit_slip": ConfigVar(
        key="limit_slip",
        prompt="How much buffer do you want to add to the LIMIT price (Enter 1 for 1%)? >>> ",
        prompt_on_new=True,
        default=Decimal("0.2"),
        validator=lambda v: validate_decimal(v),
        type_str="decimal"),
    "market_delta": ConfigVar(
        key="market_delta",
        prompt="How much change to MARKET price is acceptable (Enter 1 for 1%) >>> ",
        prompt_on_new=True,
        default=Decimal("0.05"),
        validator=lambda v: validate_decimal(v),
        type_str="decimal"),
}
