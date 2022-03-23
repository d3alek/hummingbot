# A single source of truth for constant variables related to the exchange

EXCHANGE_NAME = "huobi"

REST_URL = "https://api.huobi.pro"
FUTURES_URL = "https://api.hbdm.com"
FUTURES_EX_URL = "https://api.hbdm.com"
WS_PUBLIC_URL = "wss://api.huobi.pro/ws"
WS_FUTURE_PUBLIC_URL = "wss://api.hbdm.com/linear-swap-ws"
WS_PRIVATE_URL = "wss://api.hbdm.com/linear-swap-notification"

SYMBOLS_URL = "/linear-swap-api/v1/swap_contract_info"
TICKER_URL = "/linear-swap-api/v1/market/tickers"
FUNDING_RATE_URL = "/linear-swap-api/v1/swap_funding_rate"
SWAP_INFO = "/linear-swap-api/v1/swap_contract_info"
POSITION_INFO = "/linear-swap-api/v1/swap_cross_account_position_info"
SWITCH_POSITION = "/linear-swap-api/v1/swap_cross_switch_position_mode"
FUTURE_LAST_TRADE_URL = "/linear-swap-ex/market/trade"
DEPTH_URL = "/linear-swap-ex/market/depth"

API_VERSION = "/v1"

SERVER_TIME_URL = "/api/v1/timestamp"
ACCOUNT_BALANCE_URL = "/linear-swap-api/v1/swap_cross_account_info"
ORDER_DETAIL_URL = "/linear-swap-api/v1/swap_cross_order_info"
PLACE_ORDER_URL = "/linear-swap-api/v1/swap_cross_order"
CANCEL_ORDER_URL = "/linear-swap-api/v1/swap_cross_cancel"
CANCELALL_ORDER_URL = "/linear-swap-api/v1/swap_cross_cancelall"

HUOBI_ACCOUNT_UPDATE_TOPIC = "accounts_cross.*"
HUOBI_ACCOUNT_UPDATE_TOPIC2 = "accounts_cross"
HUOBI_ORDER_UPDATE_TOPIC = "orders_cross.*"
HUOBI_ORDER_UPDATE_TOPIC2 = "orders_cross"
HUOBI_POSITION_UPDATE_TOPIC = "positions_cross.*"
HUOBI_POSITION_UPDATE_TOPIC2 = "positions_cross"

HUOBI_SUBSCRIBE_TOPICS = {HUOBI_ORDER_UPDATE_TOPIC, HUOBI_ACCOUNT_UPDATE_TOPIC, HUOBI_ACCOUNT_UPDATE_TOPIC2}
