#!/usr/bin/env python
from hummingbot.connector.exchange.ftx.ftx_api_order_book_data_source import FtxAPIOrderBookDataSource

EXCHANGE_NAME = "ftx_perpetual"

FTX_REST_URL = "https://ftx.com/api"
FTX_EXCHANGE_INFO_PATH = "/markets"
FTX_WS_FEED = "wss://ftx.com/ws/"

MAX_RETRIES = 20
SNAPSHOT_TIMEOUT = 10.0
NaN = float("nan")
API_CALL_TIMEOUT = 5.0


class FtxPerpetualAPIOrderBookDataSource(FtxAPIOrderBookDataSource):
    pass
