#!/usr/bin/env python
import aiohttp
import asyncio
import cachetools.func
import logging
import requests

import simplejson
import time
import ujson
import websockets

from decimal import Decimal
from signalr_aio import Connection
from typing import Optional, List, Dict, AsyncIterable, Any
from websockets.exceptions import ConnectionClosed

from hummingbot.connector.derivative.ftx_perpetual.ftx_perpetual_order_book import FtxPerpetualOrderBook
from hummingbot.connector.derivative.ftx_perpetual.ftx_perpetual_utils import convert_from_exchange_trading_pair, convert_to_exchange_trading_pair
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.logger import HummingbotLogger

from hummingbot.core.data_type.funding_info import FundingInfo
import copy

import datetime

EXCHANGE_NAME = "ftx_perpetual"

FTX_REST_URL = "https://ftx.com/api"
FTX_EXCHANGE_INFO_PATH = "/markets"
FTX_FUTURE_INFO_PATH = "/futures"
FTX_WS_FEED = "wss://ftx.com/ws/"

MAX_RETRIES = 20
SNAPSHOT_TIMEOUT = 10.0
NaN = float("nan")
API_CALL_TIMEOUT = 5.0


class FtxPerpetualAPIOrderBookDataSource(OrderBookTrackerDataSource):
    MESSAGE_TIMEOUT = 30.0
    PING_TIMEOUT = 10.0

    _ftxaobds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._ftxaobds_logger is None:
            cls._ftxaobds_logger = logging.getLogger(__name__)
        return cls._ftxaobds_logger

    def __init__(self, trading_pairs: Optional[List[str]] = None):
        super().__init__(trading_pairs)
        self._websocket_connection: Optional[Connection] = None
        self._snapshot_msg: Dict[str, any] = {}
        self._funding_info: Dict[str, FundingInfo] = {}

    @property
    def funding_info(self) -> Dict[str, FundingInfo]:
        return copy.deepcopy(self._funding_info)

    def is_funding_info_initialized(self) -> bool:
        return all(trading_pair in self._funding_info for trading_pair in self._trading_pairs)

    @classmethod
    async def get_last_traded_prices(cls, trading_pairs: List[str]) -> Dict[str, float]:
        async with aiohttp.ClientSession() as client:
            async with await client.get(f"{FTX_REST_URL}{FTX_EXCHANGE_INFO_PATH}", timeout=API_CALL_TIMEOUT) as response:
                response_json = await response.json()
                results = response_json['result']
                return {convert_from_exchange_trading_pair(result['name']): float(result['last'])
                        for result in results if convert_from_exchange_trading_pair(result['name']) in trading_pairs}

    async def get_trading_pairs(self) -> List[str]:
        return self._trading_pairs

    @staticmethod
    @cachetools.func.ttl_cache(ttl=10)
    def get_mid_price(trading_pair: str) -> Optional[Decimal]:
        resp = requests.get(url=f"{FTX_REST_URL}{FTX_EXCHANGE_INFO_PATH}/{convert_to_exchange_trading_pair(trading_pair)}")
        record = resp.json()["result"]
        result = (Decimal(record.get("bid", "0")) + Decimal(record.get("ask", "0"))) / Decimal("2")
        return result if result else None

    async def _get_funding_info_from_exchange(self, trading_pair: str) -> FundingInfo:
        """
        Fetches the funding information of the given trading pair from the exchange REST API. Parses and returns the
        respsonse as a FundingInfo data object.

        :param trading_pair: Trading pair of which its Funding Info is to be fetched
        :type trading_pair: str
        :return: Funding Information of the given trading pair
        :rtype: FundingInfo
        """
        exchange_trading_pair = convert_to_exchange_trading_pair(trading_pair)
        async with aiohttp.ClientSession() as client:
            async with await client.get(f"{FTX_REST_URL}{FTX_FUTURE_INFO_PATH}/{exchange_trading_pair}/stats", timeout=API_CALL_TIMEOUT) as response:
                response_json = await response.json()
                self.logger().info(f"Response json: {response_json}")
                stats = response_json['result']
                funding_info = FundingInfo(
                    trading_pair=trading_pair,
                    index_price=None,
                    mark_price=None,
                    next_funding_utc_timestamp=int(datetime.datetime.fromisoformat(stats["nextFundingTime"]).timestamp()),
                    rate=Decimal(stats["nextFundingRate"]),
                )

        return funding_info

    async def get_funding_info(self, trading_pair: str) -> FundingInfo:
        """
        Returns the FundingInfo of the specified trading pair. If it does not exist, it will query the REST API.
        """
        if trading_pair not in self._funding_info:
            self._funding_info[trading_pair] = await self._get_funding_info_from_exchange(trading_pair)
        return self._funding_info[trading_pair]

    @staticmethod
    async def fetch_trading_pairs() -> List[str]:
        try:
            async with aiohttp.ClientSession() as client:
                async with client.get(f"{FTX_REST_URL}{FTX_EXCHANGE_INFO_PATH}", timeout=API_CALL_TIMEOUT) as response:
                    if response.status == 200:
                        all_trading_pairs: Dict[str, Any] = await response.json()
                        valid_trading_pairs: list = []
                        for item in all_trading_pairs["result"]:
                            if item["type"] == "spot":
                                valid_trading_pairs.append(item["name"])
                        trading_pair_list: List[str] = []
                        for raw_trading_pair in valid_trading_pairs:
                            converted_trading_pair: Optional[str] = \
                                convert_from_exchange_trading_pair(raw_trading_pair)
                            if converted_trading_pair is not None:
                                trading_pair_list.append(converted_trading_pair)
                        return trading_pair_list
        except Exception:
            pass

        return []

    async def get_snapshot(self, client: aiohttp.ClientSession, trading_pair: str, limit: int = 1000) -> Dict[str, Any]:
        async with client.get(f"{FTX_REST_URL}{FTX_EXCHANGE_INFO_PATH}/{convert_to_exchange_trading_pair(trading_pair)}/orderbook?depth=100") as response:
            response: aiohttp.ClientResponse = response
            if response.status != 200:
                raise IOError(f"Error fetching FTX market snapshot for {trading_pair}. "
                              f"HTTP status is {response.status}.")
            data: Dict[str, Any] = simplejson.loads(await response.text(), parse_float=Decimal)

            return data

    async def get_new_order_book(self, trading_pair: str) -> OrderBook:
        async with aiohttp.ClientSession() as client:
            snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair, 1000)
            snapshot_timestamp: float = time.time()
            snapshot_msg: OrderBookMessage = FtxPerpetualOrderBook.restful_snapshot_message_from_exchange(
                snapshot,
                snapshot_timestamp,
                metadata={"trading_pair": trading_pair}
            )
            order_book: OrderBook = self.order_book_create_function()
            order_book.apply_snapshot(snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.update_id)
            return order_book

    async def _inner_messages(self,
                              ws: websockets.WebSocketClientProtocol) -> AsyncIterable[str]:
        # Terminate the recv() loop as soon as the next message timed out, so the outer loop can reconnect.
        try:
            while True:
                try:
                    msg: str = await asyncio.wait_for(ws.recv(), timeout=self.MESSAGE_TIMEOUT)
                    yield msg
                except asyncio.TimeoutError:
                    pong_waiter = await ws.ping()
                    await asyncio.wait_for(pong_waiter, timeout=self.PING_TIMEOUT)
        except asyncio.TimeoutError:
            self.logger().warning("WebSocket ping timed out. Going to reconnect...")
            return
        except ConnectionClosed:
            return
        finally:
            await ws.close()

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                async with websockets.connect(FTX_WS_FEED) as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    for pair in self._trading_pairs:
                        subscribe_request: Dict[str, Any] = {
                            "op": "subscribe",
                            "channel": "trades",
                            "market": convert_to_exchange_trading_pair(pair)
                        }
                        await ws.send(ujson.dumps(subscribe_request))
                    async for raw_msg in self._inner_messages(ws):
                        msg = simplejson.loads(raw_msg, parse_float=Decimal)
                        if "channel" in msg:
                            if msg["channel"] == "trades" and msg["type"] == "update":
                                for trade in msg["data"]:
                                    trade_msg: OrderBookMessage = FtxPerpetualOrderBook.trade_message_from_exchange(msg, trade)
                                    output.put_nowait(trade_msg)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...",
                                    exc_info=True)
                await asyncio.sleep(30.0)

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                async with websockets.connect(FTX_WS_FEED) as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    for pair in self._trading_pairs:
                        subscribe_request: Dict[str, Any] = {
                            "op": "subscribe",
                            "channel": "orderbook",
                            "market": convert_to_exchange_trading_pair(pair)
                        }
                        await ws.send(ujson.dumps(subscribe_request))
                    async for raw_msg in self._inner_messages(ws):
                        msg = simplejson.loads(raw_msg, parse_float=Decimal)
                        if "channel" in msg:
                            if msg["channel"] == "orderbook" and msg["type"] == "update":
                                order_book_message: OrderBookMessage = FtxPerpetualOrderBook.diff_message_from_exchange(msg, msg["data"]["time"])
                                output.put_nowait(order_book_message)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...",
                                    exc_info=True)
                await asyncio.sleep(30.0)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                async with websockets.connect(FTX_WS_FEED) as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    for pair in self._trading_pairs:
                        subscribe_request: Dict[str, Any] = {
                            "op": "subscribe",
                            "channel": "orderbook",
                            "market": convert_to_exchange_trading_pair(pair)
                        }
                        await ws.send(ujson.dumps(subscribe_request))
                    async for raw_msg in self._inner_messages(ws):
                        msg = simplejson.loads(raw_msg, parse_float=Decimal)
                        if "channel" in msg:
                            if msg["channel"] == "orderbook" and msg["type"] == "partial":
                                order_book_message: OrderBookMessage = FtxPerpetualOrderBook.snapshot_message_from_exchange(msg, msg["data"]["time"])
                                output.put_nowait(order_book_message)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...",
                                    exc_info=True)
                await asyncio.sleep(30.0)
