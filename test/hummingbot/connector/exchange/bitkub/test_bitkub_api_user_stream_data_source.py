import asyncio
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from bidict import bidict

from hummingbot.connector.exchange.bitkub import bitkub_constants as CONSTANTS
from hummingbot.connector.exchange.bitkub.bitkub_api_user_stream_data_source import BitkubAPIUserStreamDataSource
from hummingbot.connector.exchange.bitkub.bitkub_auth import BitkubAuth
from hummingbot.connector.exchange.bitkub.bitkub_exchange import BitkubExchange
from hummingbot.core.web_assistant.connections.data_types import WSResponse


class BitkubAPIUserStreamDataSourceTests(IsolatedAsyncioWrapperTestCase):
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "BTC"
        cls.quote_asset = "THB"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.exchange_trading_pair = f"{cls.base_asset}_{cls.quote_asset}"

    def setUp(self) -> None:
        super().setUp()
        self.log_records = []

        time_provider = MagicMock()
        time_provider.time.return_value = 1640001112.223
        self.auth = BitkubAuth(api_key="testAPIKey", secret_key="testSecret", time_provider=time_provider)

        self.connector = BitkubExchange(
            bitkub_api_key="testAPIKey",
            bitkub_secret_key="testSecret",
            trading_pairs=[self.trading_pair])
        self.connector._set_trading_pair_symbol_map(bidict({self.exchange_trading_pair: self.trading_pair}))

        self.data_source = BitkubAPIUserStreamDataSource(
            auth=self.auth,
            connector=self.connector,
            api_factory=self.connector._web_assistants_factory)
        self.data_source.logger().setLevel(1)
        self.data_source.logger().addHandler(self)

    def handle(self, record):
        self.log_records.append(record)

    def is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message
                   for record in self.log_records)

    @patch("hummingbot.core.web_assistant.web_assistants_factory.WebAssistantsFactory.get_ws_assistant")
    async def test_connect_authenticates_and_sets_the_user_agent(self, ws_assistant_mock):
        ws_assistant = AsyncMock()
        ws_assistant.receive.return_value = WSResponse(data={"event": "auth", "code": "200", "message": "Success"})
        ws_assistant_mock.return_value = ws_assistant

        result = await self.data_source._connected_websocket_assistant()

        self.assertEqual(ws_assistant, result)
        connect_kwargs = ws_assistant.connect.call_args.kwargs
        self.assertEqual(CONSTANTS.WSS_PRIVATE_URL, connect_kwargs["ws_url"])
        self.assertEqual(CONSTANTS.WS_USER_AGENT, connect_kwargs["ws_headers"]["User-Agent"])

        auth_payload = ws_assistant.send.call_args.args[0].payload
        self.assertEqual("auth", auth_payload["event"])
        self.assertEqual("testAPIKey", auth_payload["data"]["X-BTK-APIKEY"])
        self.assertIn("X-BTK-SIGN", auth_payload["data"])
        self.assertIn("X-BTK-TIMESTAMP", auth_payload["data"])

    @patch("hummingbot.core.web_assistant.web_assistants_factory.WebAssistantsFactory.get_ws_assistant")
    async def test_connect_raises_when_authentication_is_rejected(self, ws_assistant_mock):
        ws_assistant = AsyncMock()
        ws_assistant.receive.return_value = WSResponse(
            data={"event": "auth", "code": "401", "message": "Unauthorized"})
        ws_assistant_mock.return_value = ws_assistant

        with self.assertRaises(IOError):
            await self.data_source._connected_websocket_assistant()

    @patch("hummingbot.core.web_assistant.web_assistants_factory.WebAssistantsFactory.get_ws_assistant")
    async def test_connect_reports_the_concurrent_connection_limit(self, ws_assistant_mock):
        ws_assistant = AsyncMock()
        ws_assistant.receive.return_value = WSResponse(
            data={"event": "auth", "code": "429", "message": "Too Many Connections"})
        ws_assistant_mock.return_value = ws_assistant

        with self.assertRaises(IOError):
            await self.data_source._connected_websocket_assistant()

        self.assertTrue(self.is_logged(
            "ERROR",
            "Bitkub allows at most 5 concurrent private websocket connections per API key."))

    async def test_subscribe_channels_sends_both_subscriptions(self):
        ws_assistant = AsyncMock()

        await self.data_source._subscribe_channels(ws_assistant)
        self.data_source._ping_task.cancel()

        payloads = [call.args[0].payload for call in ws_assistant.send.call_args_list]
        self.assertEqual([
            {"event": "subscribe", "channel": CONSTANTS.ORDER_UPDATE_CHANNEL},
            {"event": "subscribe", "channel": CONSTANTS.MATCH_UPDATE_CHANNEL},
        ], payloads)

    async def test_send_ping_uses_the_application_level_message(self):
        ws_assistant = AsyncMock()

        await self.data_source._send_ping(ws_assistant)

        self.assertEqual({"event": "ping"}, ws_assistant.send.call_args.args[0].payload)

    async def test_control_messages_are_not_forwarded(self):
        queue = asyncio.Queue()

        for message in (
            {"event": "auth", "code": "200"},
            {"event": "subscribe", "channel": "order_update", "code": "200"},
            {"event": "unsubscribe", "channel": "order_update", "code": "200"},
            {"event": "ping", "code": "200", "data": {"message": "pong"}},
            {},
        ):
            await self.data_source._process_event_message(event_message=message, queue=queue)

        self.assertTrue(queue.empty())

    async def test_account_messages_are_forwarded(self):
        queue = asyncio.Queue()
        order_update = {"event": CONSTANTS.ORDER_UPDATE_CHANNEL, "code": "200", "data": {"order_id": "1"}}
        match_update = {"event": CONSTANTS.MATCH_UPDATE_CHANNEL, "code": "200", "data": {"txn_id": "1"}}

        await self.data_source._process_event_message(event_message=order_update, queue=queue)
        await self.data_source._process_event_message(event_message=match_update, queue=queue)

        self.assertEqual(order_update, queue.get_nowait())
        self.assertEqual(match_update, queue.get_nowait())

    async def test_stream_interruption_cancels_the_ping_task(self):
        ws_assistant = AsyncMock()
        await self.data_source._subscribe_channels(ws_assistant)
        ping_task = self.data_source._ping_task

        await self.data_source._on_user_stream_interruption(websocket_assistant=ws_assistant)
        # Give the event loop a chance to process the cancellation
        await asyncio.sleep(0)

        self.assertTrue(ping_task.cancelled())
        self.assertIsNone(self.data_source._ping_task)
        ws_assistant.disconnect.assert_called_once()
