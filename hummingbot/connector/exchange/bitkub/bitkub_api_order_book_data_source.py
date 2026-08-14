import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.bitkub import bitkub_constants as CONSTANTS, bitkub_web_utils as web_utils
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.exchange.bitkub.bitkub_exchange import BitkubExchange

# Key used to carry the trading pair alongside a raw websocket message. The order book stream does not
# include the symbol in its payloads, only the numeric pairing id, so the per-pair listener tags it.
TRADING_PAIR_KEY = "_hb_trading_pair"


class BitkubAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """
    Bitkub serves its live order book at ``/websocket-api/orderbook/<pairing_id>``, one symbol per
    connection, so this data source runs one websocket listener per trading pair.

    Every book event is a full replacement rather than an incremental update - there are no sequence
    numbers anywhere in the API - so messages are emitted as snapshots and the diff channel is unused.
    The public ``market.trade.*`` stream was permanently closed on 18 May 2026, which leaves the
    ``tradeschanged`` event on this socket as the only live public trade feed.
    """

    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'BitkubExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._ws_assistants: Dict[str, WSAssistant] = {}
        self._listening_tasks: Dict[str, asyncio.Task] = {}

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        return {trading_pair: await self._connector._get_last_traded_price(trading_pair)
                for trading_pair in trading_pairs}

    # === Subscription handling ===

    async def listen_for_subscriptions(self):
        """
        Runs one websocket listener per trading pair, since a connection carries a single symbol.
        """
        self._listening_tasks = {
            trading_pair: safe_ensure_future(self._listen_for_trading_pair(trading_pair))
            for trading_pair in self._trading_pairs
        }
        try:
            await safe_gather(*self._listening_tasks.values(), return_exceptions=True)
        finally:
            for task in self._listening_tasks.values():
                task.cancel()
            self._listening_tasks = {}

    async def _listen_for_trading_pair(self, trading_pair: str):
        while True:
            ws: Optional[WSAssistant] = None
            try:
                ws = await self._connected_websocket_assistant_for_pair(trading_pair)
                self._ws_assistants[trading_pair] = ws
                await self._process_websocket_messages_for_pair(websocket_assistant=ws, trading_pair=trading_pair)
            except asyncio.CancelledError:
                raise
            except ConnectionError as connection_exception:
                self.logger().warning(
                    f"The websocket connection for {trading_pair} was closed ({connection_exception})")
            except Exception:
                self.logger().exception(
                    f"Unexpected error while listening to the {trading_pair} order book stream. "
                    f"Retrying in 5 seconds...")
                await self._sleep(5.0)
            finally:
                self._ws_assistants.pop(trading_pair, None)
                await self._on_order_stream_interruption(websocket_assistant=ws)

    async def _connected_websocket_assistant_for_pair(self, trading_pair: str) -> WSAssistant:
        pairing_id = await self._connector.pairing_id_for_trading_pair(trading_pair)
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_CONNECT):
            await ws.connect(
                ws_url=CONSTANTS.WSS_PUBLIC_ORDERBOOK_URL.format(pairing_id),
                ping_timeout=CONSTANTS.WS_HEARTBEAT_TIMEOUT)
        return ws

    async def _connected_websocket_assistant(self) -> WSAssistant:
        raise NotImplementedError(
            "Bitkub order book streams are per trading pair; use _connected_websocket_assistant_for_pair.")

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        The websocket URL itself is the subscription, so there is no subscribe message to send.
        """
        pass

    async def _process_websocket_messages_for_pair(self, websocket_assistant: WSAssistant, trading_pair: str):
        async for ws_response in websocket_assistant.iter_messages():
            data = ws_response.data
            if data is None:
                continue
            data[TRADING_PAIR_KEY] = trading_pair
            channel = self._channel_originating_message(event_message=data)
            if channel in self._get_messages_queue_keys():
                self._message_queue[channel].put_nowait(data)

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        event = event_message.get("event")
        if event == CONSTANTS.DEPTH_CHANGED_EVENT:
            return self._snapshot_messages_queue_key
        if event == CONSTANTS.TRADES_CHANGED_EVENT:
            return self._trade_messages_queue_key
        # bidschanged / askschanged / ticker / global.ticker are ignored: the one-sided book events
        # would clobber the opposite side, and depthchanged already carries the full book.
        return ""

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        if trading_pair in self._listening_tasks:
            return True
        self.add_trading_pair(trading_pair)
        self._listening_tasks[trading_pair] = safe_ensure_future(self._listen_for_trading_pair(trading_pair))
        return True

    async def unsubscribe_from_trading_pair(self, trading_pair: str) -> bool:
        task = self._listening_tasks.pop(trading_pair, None)
        if task is not None:
            task.cancel()
        ws = self._ws_assistants.pop(trading_pair, None)
        if ws is not None:
            await ws.disconnect()
        self.remove_trading_pair(trading_pair)
        return True

    # === Message parsing ===

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        Bitkub never sends incremental order book updates, so there is nothing to listen for here.
        """
        pass

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        trading_pair = raw_message[TRADING_PAIR_KEY]
        data = raw_message["data"]
        timestamp = self._time()
        message_queue.put_nowait(OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            {
                "trading_pair": trading_pair,
                "update_id": int(timestamp * 1e3),
                "bids": [(entry["price"], entry["base_volume"]) for entry in data.get("bids") or []],
                "asks": [(entry["price"], entry["base_volume"]) for entry in data.get("asks") or []],
            },
            timestamp=timestamp))

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        trading_pair = raw_message[TRADING_PAIR_KEY]
        # The payload holds three arrays: the latest trades, the buy orders and the sell orders
        data = raw_message["data"]
        trades = data[0] if data else []
        for trade in trades:
            trade_timestamp = float(trade[0])
            message_queue.put_nowait(OrderBookMessage(
                OrderBookMessageType.TRADE,
                {
                    "trading_pair": trading_pair,
                    "trade_type": (float(TradeType.BUY.value)
                                   if str(trade[3]).upper() == "BUY"
                                   else float(TradeType.SELL.value)),
                    "trade_id": f"{trading_pair}-{trade[0]}-{trade[1]}-{trade[2]}",
                    "price": trade[1],
                    "amount": trade[2],
                },
                timestamp=trade_timestamp))

    # === REST snapshots ===

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot_response = await self._request_order_book_snapshot(trading_pair)
        timestamp = self._time()
        return OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            {
                "trading_pair": trading_pair,
                "update_id": int(timestamp * 1e3),
                "bids": snapshot_response.get("bids") or [],
                "asks": snapshot_response.get("asks") or [],
            },
            timestamp=timestamp)

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        rest_assistant = await self._api_factory.get_rest_assistant()
        response = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.DEPTH_PATH_URL, domain=self._domain),
            params={"sym": symbol, "lmt": CONSTANTS.ORDER_BOOK_SNAPSHOT_DEPTH},
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.DEPTH_PATH_URL,
        )
        return self._connector._unwrap_response(response)

    def _time(self) -> float:
        return time.time()
