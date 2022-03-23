from hummingbot.client.config.config_var import ConfigVar
from hummingbot.client.config.config_validators import (
    validate_market_trading_pair,
    validate_derivative,
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
    exchange1 = funding_rate_config_map["short"].value
    exchange2 = funding_rate_config_map["long"].value
    return validate_market_trading_pair(exchange1, value) and validate_market_trading_pair(exchange2, value)


def perpetual_market_on_validated(value: str) -> None:
    requried_connector_trading_pairs[funding_rate_config_map["short"].value] = [value]
    requried_connector_trading_pairs[funding_rate_config_map["long"].value] = [value]


def perpetual_market_prompt() -> str:
    connector1 = funding_rate_config_map.get("short").value
    connector2 = funding_rate_config_map.get("long").value
    example = AllConnectorSettings.get_example_pairs().get(connector1)
    return "Enter the token trading pair you would like to trade on %s%s >>> " \
           % (f"{connector1} and {connector2}", f" (e.g. {example})" if example else "")


def order_amount_prompt() -> str:
    trading_pair = funding_rate_config_map["trading_pair"].value
    base_asset, quote_asset = trading_pair.split("-")
    return f"What is the amount of {base_asset} per order? >>> "


def total_amount_prompt() -> str:
    trading_pair = funding_rate_config_map["trading_pair"].value
    base_asset, quote_asset = trading_pair.split("-")
    return f"What is the total of {base_asset} to trade per exchange? >>> "


funding_rate_config_map = {
    "strategy": ConfigVar(
        key="strategy",
        prompt="",
        default="funding_rate"),
    "short": ConfigVar(
        key="short",
        prompt="Where to SHORT (Exchange/AMM) >>> ",
        prompt_on_new=True,
        validator=validate_derivative,
        on_validated=exchange_on_validated),
    "long": ConfigVar(
        key="long",
        prompt="Where to LONG (Exchange/AMM) >>> ",
        prompt_on_new=True,
        validator=validate_derivative,
        on_validated=exchange_on_validated),
    "short_maker": ConfigVar(
        key="short_maker",
        prompt="If True, do MAKER order in SHORT exchange. If False, do MAKER order in LONG exchange >>> ",
        prompt_on_new=True,
        validator=lambda v: validate_bool(v),
        type_str="bool"),
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
        key="action",
        prompt="If True, OPEN positions, if False, CLOSE",
        validator=lambda v: validate_bool(v),
        type_str="bool",
        prompt_on_new=True),
    # "leverage": ConfigVar(
    #     key="leverage",
    #     prompt="How much leverage would you like to use on the perpetual exchange? (Enter 20 to indicate 20X) >>> ",
    #     type_str="int",
    #     default=20,
    #     validator= lambda v: validate_int(v),
    #     prompt_on_new=True),
    "maker_slip": ConfigVar(
        key="maker_slip",
        prompt="How much buffer do you want to add to the MAKER price (Enter 1 for 1%)? >>> ",
        prompt_on_new=True,
        default=Decimal("0.2"),
        validator=lambda v: validate_decimal(v),
        type_str="decimal"),
    "taker_delta": ConfigVar(
        key="taker_delta",
        prompt="How much change to TAKER price is acceptable (Enter 1 for 1%) >>> ",
        prompt_on_new=True,
        default=Decimal("0.05"),
        validator=lambda v: validate_decimal(v),
        type_str="decimal"),
}
