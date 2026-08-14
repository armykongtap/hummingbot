import asyncio
from typing import TYPE_CHECKING, Any, Dict, Optional

from hummingbot.connector.exchange.bitkub import bitkub_constants as CONSTANTS
from hummingbot.connector.exchange.bitkub.bitkub_auth import BitkubAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.exchange.bitkub.bitkub_exchange import BitkubExchange

# Events that acknowledge a client action rather than report account activity
CONTROL_EVENTS = {"auth", "subscribe", "unsubscribe", "ping", "pong"}


class BitkubAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    Listens to Bitkub's private websocket for order and trade execution updates.

    The stream is hosted on a different host from the public API, requires a User-Agent header, and
    authenticates with an ``auth`` message whose signature covers the timestamp alone. There is no
    balance channel, so balances are polled over REST instead.
    """

    def __init__(self,
                 auth: BitkubAuth,
                 connector: 'BitkubExchange',
                 api_factory: WebAssistantsFactory):
        super().__init__()
        self._auth = auth
        self._connector = connector
        self._api_factory = api_factory
        self._ping_task: Optional[asyncio.Task] = None

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_CONNECT):
            await ws.connect(
                ws_url=CONSTANTS.WSS_PRIVATE_URL,
                ping_timeout=CONSTANTS.WS_HEARTBEAT_TIMEOUT,
                ws_headers={"User-Agent": CONSTANTS.WS_USER_AGENT})

        await ws.send(WSJSONRequest(payload=self._auth.websocket_auth_payload()))
        response = await ws.receive()
        data = response.data if response is not None else {}
        if str(data.get("code")) != "200":
            if str(data.get("code")) == "429":
                self.logger().error("Bitkub allows at most 5 concurrent private websocket connections per API key.")
            raise IOError(f"Bitkub private websocket authentication failed: {data}")
        self.logger().info("Authenticated with the Bitkub private websocket.")
        return ws

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        try:
            for channel in (CONSTANTS.ORDER_UPDATE_CHANNEL, CONSTANTS.MATCH_UPDATE_CHANNEL):
                async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_SUBSCRIBE):
                    await websocket_assistant.send(
                        WSJSONRequest(payload={"event": "subscribe", "channel": channel}))
            self.logger().info("Subscribed to the Bitkub order and trade execution channels.")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error occurred subscribing to the Bitkub user stream.")
            raise

        self._ping_task = safe_ensure_future(self._ping_loop(websocket_assistant))

    async def _send_ping(self, websocket_assistant: WSAssistant):
        """
        Bitkub expects an application-level ping rather than a websocket protocol ping frame.
        """
        await websocket_assistant.send(WSJSONRequest(payload={"event": "ping"}))

    async def _ping_loop(self, websocket_assistant: WSAssistant):
        """
        Bitkub requires a ping at least every 5 minutes and closes every connection after 2 hours.
        The 2 hour close surfaces as a connection error, which the base class reconnects from.
        """
        while True:
            await self._sleep(CONSTANTS.WS_PING_INTERVAL)
            await self._send_ping(websocket_assistant=websocket_assistant)

    async def _process_event_message(self, event_message: Dict[str, Any], queue: asyncio.Queue):
        if not event_message or event_message.get("event") in CONTROL_EVENTS:
            return
        queue.put_nowait(event_message)

    async def _on_user_stream_interruption(self, websocket_assistant: Optional[WSAssistant]):
        if self._ping_task is not None:
            self._ping_task.cancel()
            self._ping_task = None
        await super()._on_user_stream_interruption(websocket_assistant=websocket_assistant)

    async def stop(self):
        if self._ping_task is not None:
            self._ping_task.cancel()
            self._ping_task = None
        await super().stop()
