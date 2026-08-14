import asyncio
import json
import re
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import Awaitable
from unittest.mock import AsyncMock, patch

from aioresponses import aioresponses
from bidict import bidict

from hummingbot.connector.exchange.bitkub import bitkub_constants as CONSTANTS, bitkub_web_utils as web_utils
from hummingbot.connector.exchange.bitkub.bitkub_api_order_book_data_source import (
    TRADING_PAIR_KEY,
    BitkubAPIOrderBookDataSource,
)
from hummingbot.connector.exchange.bitkub.bitkub_exchange import BitkubExchange
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


class BitkubAPIOrderBookDataSourceTests(IsolatedAsyncioWrapperTestCase):
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "BTC"
        cls.quote_asset = "THB"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.exchange_trading_pair = f"{cls.base_asset}_{cls.quote_asset}"
        cls.pairing_id = 1

    def setUp(self) -> None:
        super().setUp()
        self.log_records = []

        self.connector = BitkubExchange(
            bitkub_api_key="testAPIKey",
            bitkub_secret_key="testSecret",
            trading_pairs=[self.trading_pair])
        self.connector._set_trading_pair_symbol_map(bidict({self.exchange_trading_pair: self.trading_pair}))
        self.connector._pairing_ids = {self.exchange_trading_pair: self.pairing_id}

        self.data_source = BitkubAPIOrderBookDataSource(
            trading_pairs=[self.trading_pair],
            connector=self.connector,
            api_factory=self.connector._web_assistants_factory)
        self.data_source.logger().setLevel(1)
        self.data_source.logger().addHandler(self)

    def handle(self, record):
        self.log_records.append(record)

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: float = 1):
        return self.run_async_with_timeout(coroutine, timeout)

    @property
    def depth_url_regex(self):
        return re.compile(
            f"^{web_utils.public_rest_url(CONSTANTS.DEPTH_PATH_URL)}".replace(".", r"\.") + r"\?.*")

    @property
    def depth_changed_event(self):
        return {
            "event": CONSTANTS.DEPTH_CHANGED_EVENT,
            "pairing_id": self.pairing_id,
            "data": {
                "bids": [
                    {"price": 2466650.35, "base_volume": 0.0002027, "quote_volume": 500},
                    {"price": 2466000.00, "base_volume": 0.001, "quote_volume": 2466},
                ],
                "asks": [
                    {"price": 2467772.05, "base_volume": 0.003, "quote_volume": 7403.32},
                ],
            },
        }

    @property
    def trades_changed_event(self):
        return {
            "event": CONSTANTS.TRADES_CHANGED_EVENT,
            "pairing_id": self.pairing_id,
            # The payload holds three arrays: the latest trades, the buy orders and the sell orders
            "data": [
                [
                    [1734661894, 3367353.98, 0.00148484, "BUY", 0, 0, True, False, False],
                    [1734661893, 3367353.90, 0.00029622, "SELL", 0, 0, True, False, False],
                ],
                [[121.82, 3367000.00, 0.00108283, 0, False, False]],
                [[51247.13, 3368000.00, 0.45072632, 0, False, False]],
            ],
        }

    @aioresponses()
    async def test_order_book_snapshot_is_built_from_the_rest_depth_endpoint(self, mock_api):
        mock_api.get(self.depth_url_regex, body=json.dumps({
            "error": 0,
            "result": {
                "asks": [[3338932.98, 0.00619979], [3341006.36, 0.00134854]],
                "bids": [[3334907.27, 0.00471255], [3334907.26, 0.36895805]],
            },
        }))

        message = await self.data_source._order_book_snapshot(self.trading_pair)

        self.assertEqual(OrderBookMessageType.SNAPSHOT, message.type)
        self.assertEqual(self.trading_pair, message.trading_pair)
        self.assertEqual(2, len(message.bids))
        self.assertEqual(2, len(message.asks))
        self.assertEqual(3334907.27, message.bids[0].price)
        self.assertEqual(0.00471255, message.bids[0].amount)
        self.assertEqual(3338932.98, message.asks[0].price)

        depth_urls = [key[1].human_repr() for key in mock_api.requests
                      if CONSTANTS.DEPTH_PATH_URL in key[1].human_repr()]
        self.assertEqual(1, len(depth_urls))
        self.assertIn(f"sym={self.exchange_trading_pair}", depth_urls[0])
        self.assertIn(f"lmt={CONSTANTS.ORDER_BOOK_SNAPSHOT_DEPTH}", depth_urls[0])

    def test_depth_changed_is_routed_to_the_snapshot_queue(self):
        self.assertEqual(self.data_source._snapshot_messages_queue_key,
                         self.data_source._channel_originating_message(self.depth_changed_event))

    def test_trades_changed_is_routed_to_the_trade_queue(self):
        self.assertEqual(self.data_source._trade_messages_queue_key,
                         self.data_source._channel_originating_message(self.trades_changed_event))

    def test_one_sided_and_ticker_events_are_ignored(self):
        for event in ("bidschanged", "askschanged", "ticker", "global.ticker"):
            self.assertEqual("", self.data_source._channel_originating_message({"event": event}))

    async def test_depth_changed_event_is_parsed_as_a_full_snapshot(self):
        message = self.depth_changed_event
        message[TRADING_PAIR_KEY] = self.trading_pair
        queue = asyncio.Queue()

        await self.data_source._parse_order_book_snapshot_message(raw_message=message, message_queue=queue)

        snapshot: OrderBookMessage = queue.get_nowait()
        self.assertEqual(OrderBookMessageType.SNAPSHOT, snapshot.type)
        self.assertEqual(self.trading_pair, snapshot.trading_pair)
        self.assertEqual(2, len(snapshot.bids))
        self.assertEqual(1, len(snapshot.asks))
        self.assertEqual(2466650.35, snapshot.bids[0].price)
        self.assertEqual(0.0002027, snapshot.bids[0].amount)
        self.assertEqual(2467772.05, snapshot.asks[0].price)

    async def test_trades_changed_event_yields_one_message_per_trade(self):
        message = self.trades_changed_event
        message[TRADING_PAIR_KEY] = self.trading_pair
        queue = asyncio.Queue()

        await self.data_source._parse_trade_message(raw_message=message, message_queue=queue)

        first: OrderBookMessage = queue.get_nowait()
        second: OrderBookMessage = queue.get_nowait()
        self.assertTrue(queue.empty())

        self.assertEqual(OrderBookMessageType.TRADE, first.type)
        self.assertEqual(self.trading_pair, first.trading_pair)
        self.assertEqual(float(TradeType.BUY.value), first.content["trade_type"])
        self.assertEqual(3367353.98, first.content["price"])
        self.assertEqual(0.00148484, first.content["amount"])
        self.assertEqual(1734661894, first.timestamp)

        self.assertEqual(float(TradeType.SELL.value), second.content["trade_type"])

    async def test_listen_for_order_book_diffs_is_a_no_op(self):
        queue = asyncio.Queue()
        await self.data_source.listen_for_order_book_diffs(ev_loop=None, output=queue)
        self.assertTrue(queue.empty())

    @patch("hummingbot.core.web_assistant.web_assistants_factory.WebAssistantsFactory.get_ws_assistant")
    async def test_websocket_url_is_built_from_the_pairing_id(self, ws_assistant_mock):
        ws_assistant = AsyncMock()
        ws_assistant_mock.return_value = ws_assistant

        await self.data_source._connected_websocket_assistant_for_pair(self.trading_pair)

        ws_assistant.connect.assert_called_once()
        self.assertEqual(f"wss://api.bitkub.com/websocket-api/orderbook/{self.pairing_id}",
                         ws_assistant.connect.call_args.kwargs["ws_url"])

    async def test_listen_for_subscriptions_runs_one_task_per_trading_pair(self):
        self.data_source.add_trading_pair(f"{self.base_asset}2-{self.quote_asset}")
        started = []

        async def fake_listener(trading_pair):
            started.append(trading_pair)
            await asyncio.sleep(10)

        with patch.object(self.data_source, "_listen_for_trading_pair", side_effect=fake_listener):
            task = asyncio.create_task(self.data_source.listen_for_subscriptions())
            await asyncio.sleep(0.1)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        self.assertEqual(sorted(self.data_source._trading_pairs), sorted(started))

    @aioresponses()
    async def test_get_last_traded_prices(self, mock_api):
        mock_api.get(
            re.compile(f"^{web_utils.public_rest_url(CONSTANTS.TICKER_PATH_URL)}".replace(".", r"\.") + r"\?.*"),
            body=json.dumps([{"symbol": self.exchange_trading_pair, "last": "2466650.35"}]))

        prices = await self.data_source.get_last_traded_prices([self.trading_pair])

        self.assertEqual(1, len(prices))
        self.assertEqual(float(Decimal("2466650.35")), prices[self.trading_pair])
