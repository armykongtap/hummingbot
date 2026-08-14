import asyncio
import re
from decimal import ROUND_DOWN, Decimal
from typing import Any, Dict, List, Optional, Tuple, Union

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.bitkub import (
    bitkub_constants as CONSTANTS,
    bitkub_utils,
    bitkub_web_utils as web_utils,
)
from hummingbot.connector.exchange.bitkub.bitkub_api_order_book_data_source import BitkubAPIOrderBookDataSource
from hummingbot.connector.exchange.bitkub.bitkub_api_user_stream_data_source import BitkubAPIUserStreamDataSource
from hummingbot.connector.exchange.bitkub.bitkub_auth import BitkubAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair, split_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

ERROR_CODE_PATTERN = re.compile(r"Bitkub error ([\w\-]+)")


class BitkubExchange(ExchangePyBase):
    """
    BitkubExchange connects with the Bitkub exchange and provides order book pricing, user account tracking
    and trading functionality.

    Bitkub splits its API across two versions: market data and trading live on v3, while balances live on v4
    (the v3 wallet endpoints were removed on 2026-05-26). Both are reached through this single connector.
    """
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0

    web_utils = web_utils

    def __init__(self,
                 bitkub_api_key: str,
                 bitkub_secret_key: str,
                 balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
                 rate_limits_share_pct: Decimal = Decimal("100"),
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 ):
        """
        :param bitkub_api_key: the API key to connect to the private Bitkub APIs
        :param bitkub_secret_key: the API secret
        :param trading_pairs: the market trading pairs which to track order book data
        :param trading_required: whether actual trading is needed
        """
        self._api_key: str = bitkub_api_key
        self._secret_key: str = bitkub_secret_key
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        # Exchange symbol -> numeric pairing id, needed to build the order book websocket URL.
        # GET /api/v3/market/symbols is the only place the pairing id is published.
        self._pairing_ids: Dict[str, int] = {}
        # Trading pair -> quote amount increment, used to round the quote amount of a buy order
        self._quote_amount_increments: Dict[str, Decimal] = {}

        super().__init__(balance_asset_limit, rate_limits_share_pct)
        # Bitkub's private websocket has no balance channel, so balances have to be polled
        self.real_time_balance_update = False

    @property
    def authenticator(self) -> BitkubAuth:
        return BitkubAuth(
            api_key=self._api_key,
            secret_key=self._secret_key,
            time_provider=self._time_synchronizer)

    @property
    def name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self) -> str:
        return CONSTANTS.DEFAULT_DOMAIN

    @property
    def client_order_id_max_length(self) -> int:
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self) -> str:
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self) -> str:
        return CONSTANTS.SYMBOLS_PATH_URL

    @property
    def trading_pairs_request_path(self) -> str:
        return CONSTANTS.SYMBOLS_PATH_URL

    @property
    def check_network_request_path(self) -> str:
        # /api/status is the documented health endpoint but it currently answers 524 for every caller,
        # which would keep the connector permanently reported as disconnected. The server time endpoint
        # is public, cheap and reliable, so it is used as the reachability probe instead.
        return CONSTANTS.SERVER_TIME_PATH_URL

    @property
    def trading_pairs(self) -> List[str]:
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self) -> List[OrderType]:
        """
        Market orders are deliberately not offered. Bitkub takes the quote amount to spend on a buy
        rather than the base quantity Hummingbot works in, so a market buy would have to be sized
        from a reference price and could never deliver the requested base amount once the price moved
        or the fee was applied. ExchangeBase.get_taker_order_type falls back to LIMIT here.
        """
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER]

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self.domain,
            auth=self._auth)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return BitkubAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain)

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return BitkubAPIUserStreamDataSource(
            auth=self._auth,
            connector=self,
            api_factory=self._web_assistants_factory)

    # === Error handling ===
    #
    # Bitkub answers business errors with HTTP 200 and a non-zero code in the body, so the envelope is
    # inspected here and turned into an IOError. Using IOError specifically lets the retry loop in
    # ExchangePyBase._api_request pick up clock-skew failures and resynchronise the time offset.

    async def _api_request(self, *args, **kwargs) -> Union[Dict[str, Any], List[Any], Any]:
        response = await super()._api_request(*args, **kwargs)
        return self._unwrap_response(response)

    @staticmethod
    def _unwrap_response(response: Any) -> Any:
        """
        Validates the response envelope and returns its payload.

        v3 endpoints answer with ``{"error": 0, "result": ...}`` and v4 endpoints with
        ``{"code": "0", "message": ..., "data": ...}``. A few public endpoints (``/api/status``,
        ``/api/v3/servertime``, ``/api/v3/market/ticker``) answer with a bare array or scalar instead.
        """
        if not isinstance(response, dict):
            return response
        if "error" in response:
            if int(response["error"]) != 0:
                raise IOError(f"Bitkub error {response['error']} - response: {response}")
            return response.get("result", response)
        if "code" in response:
            # v4 reports "0" as a string for wallet endpoints and 0 as a number for fiat ones
            if str(response["code"]) not in ("0", "200"):
                raise IOError(f"Bitkub error {response['code']} - {response.get('message')}")
            return response.get("data", response)
        return response

    @staticmethod
    def _error_code(exception: Exception) -> Optional[str]:
        match = ERROR_CODE_PATTERN.search(str(exception))
        return match.group(1) if match is not None else None

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        code = self._error_code(request_exception)
        return code is not None and code.isdigit() and int(code) in CONSTANTS.TIME_SYNC_ERROR_CODES

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        code = self._error_code(status_update_exception)
        return code is not None and code.isdigit() and int(code) in CONSTANTS.ORDER_NOT_FOUND_ON_LOOKUP_ERROR_CODES

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        code = self._error_code(cancelation_exception)
        return code is not None and code.isdigit() and int(code) in CONSTANTS.ORDER_NOT_FOUND_ON_CANCEL_ERROR_CODES

    # === Trading ===

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> AddedToCostTradeFee:
        is_maker = is_maker if is_maker is not None else (order_type is OrderType.LIMIT_MAKER)
        return AddedToCostTradeFee(percent=self.estimate_fee_pct(is_maker))

    @staticmethod
    def _format_decimal(value: Decimal) -> str:
        """
        Renders an amount or a rate in the form Bitkub accepts.

        Bitkub rejects trailing zeros ("1000.00 is invalid, 1000 is ok"), so the value is normalized,
        and it is formatted fixed-point because scientific notation is not a documented input format.
        The result is a string here only so that json.dumps cannot turn a small amount back into
        exponent form through repr(); BitkubRESTPreProcessor unquotes it before the request is signed.
        """
        return format(value.normalize(), "f")

    async def _place_order(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           trade_type: TradeType,
                           order_type: OrderType,
                           price: Decimal,
                           **kwargs) -> Tuple[str, float]:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        if trade_type is TradeType.BUY:
            # place-bid takes the quote (THB) amount to spend
            path_url = CONSTANTS.PLACE_BID_PATH_URL
            order_amount = self._quantize_quote_amount(trading_pair, amount * price)
        else:
            # place-ask takes the base quantity to sell
            path_url = CONSTANTS.PLACE_ASK_PATH_URL
            order_amount = amount

        data = {
            "sym": symbol,
            "amt": self._format_decimal(order_amount),
            "rat": self._format_decimal(price),
            "typ": "limit",
            "client_id": order_id,
        }
        if order_type is OrderType.LIMIT_MAKER:
            data["post_only"] = True

        order_result = await self._api_post(
            path_url=path_url,
            data=data,
            is_auth_required=True)

        return str(order_result["id"]), self.current_timestamp

    def _quantize_quote_amount(self, trading_pair: str, amount: Decimal) -> Decimal:
        """
        Rounds a quote amount down to the increment the exchange accepts for that pair.

        The increments are tracked separately from the trading rules because TradingRule defaults
        min_quote_amount_increment to a placeholder value that would destroy precision here.
        """
        increment = self._quote_amount_increments.get(trading_pair)
        if increment is None or increment <= 0:
            return amount
        return (amount / increment).to_integral_value(rounding=ROUND_DOWN) * increment

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        # The documentation claims this endpoint still needs the reversed legacy symbol ("thb_btc"),
        # but a live cancellation with the canonical "BTC_THB" succeeds, so the note is stale and the
        # same symbol form is used here as everywhere else.
        exchange_order_id = await tracked_order.get_exchange_order_id()
        data = {
            "sym": await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair),
            "id": exchange_order_id,
            "sd": tracked_order.trade_type.name.lower(),
        }
        # A successful cancellation answers with {"error": 0} and no result body, so reaching this
        # point without an exception means the order was cancelled.
        await self._api_post(
            path_url=CONSTANTS.CANCEL_ORDER_PATH_URL,
            data=data,
            is_auth_required=True)
        return True

    # === Reference data ===

    async def _format_trading_rules(self, exchange_info_dict: List[Dict[str, Any]]) -> List[TradingRule]:
        """
        Converts the symbol details returned by GET /api/v3/market/symbols into trading rules.

        The "quantity_step" and "quantity_scale" fields are deliberately ignored: the exchange reports
        them as "1" and 0 for every symbol, including BTC_THB, which would forbid any order below one
        whole coin while the minimum notional is 10 THB. The real precision is in "base_quantity_scale"
        and "quote_quantity_scale", which are returned by the live API but absent from the documentation.
        """
        result = []
        for rule in filter(bitkub_utils.is_exchange_information_valid, exchange_info_dict):
            try:
                trading_pair = combine_to_hb_trading_pair(base=rule["base_asset"], quote=rule["quote_asset"])
                base_step = Decimal(1).scaleb(-int(rule.get("base_quantity_scale", rule["base_asset_scale"])))
                quote_step = Decimal(1).scaleb(-int(rule.get("quote_quantity_scale", rule["quote_asset_scale"])))
                price_step = Decimal(str(rule["price_step"]))
                self._quote_amount_increments[trading_pair] = quote_step
                result.append(TradingRule(
                    trading_pair=trading_pair,
                    min_order_size=base_step,
                    min_price_increment=price_step,
                    min_base_amount_increment=base_step,
                    min_quote_amount_increment=quote_step,
                    min_notional_size=Decimal(str(rule["min_quote_size"])),
                    supports_market_orders=False,
                ))
            except Exception:
                self.logger().exception(f"Error parsing the trading pair rule {rule}. Skipping.")
        return result

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: List[Dict[str, Any]]):
        mapping = bidict()
        pairing_ids: Dict[str, int] = {}
        for symbol_data in filter(bitkub_utils.is_exchange_information_valid, exchange_info):
            symbol = symbol_data["symbol"]
            mapping[symbol] = combine_to_hb_trading_pair(base=symbol_data["base_asset"],
                                                         quote=symbol_data["quote_asset"])
            pairing_ids[symbol] = int(symbol_data["pairing_id"])
        self._pairing_ids = pairing_ids
        self._set_trading_pair_symbol_map(mapping)

    async def pairing_id_for_trading_pair(self, trading_pair: str) -> int:
        """
        Returns the numeric symbol id used to build the order book websocket URL.
        """
        await self.trading_pair_symbol_map()
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        return self._pairing_ids[symbol]

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        tickers = await self._api_get(
            path_url=CONSTANTS.TICKER_PATH_URL,
            params={"sym": symbol})
        for ticker in tickers:
            if ticker.get("symbol") == symbol:
                return float(ticker["last"])
        raise ValueError(f"There is no ticker information for {trading_pair} ({symbol})")

    async def _update_trading_fees(self):
        """
        Bitkub does not expose per-account fee tiers through its API.
        """
        pass

    async def _update_balances(self):
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()

        balances = await self._api_get(
            path_url=CONSTANTS.BALANCES_PATH_URL,
            is_auth_required=True)

        for balance in balances:
            asset_name = balance["currency"]
            self._account_available_balances[asset_name] = Decimal(str(balance["available"]))
            self._account_balances[asset_name] = Decimal(str(balance["total"]))
            remote_asset_names.add(asset_name)

        for asset_name in local_asset_names.difference(remote_asset_names):
            del self._account_available_balances[asset_name]
            del self._account_balances[asset_name]

    # === Order status ===

    async def _request_order_info(self, tracked_order: InFlightOrder) -> Dict[str, Any]:
        exchange_order_id = await tracked_order.get_exchange_order_id()
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        return await self._api_get(
            path_url=CONSTANTS.ORDER_INFO_PATH_URL,
            params={
                "sym": symbol,
                "id": exchange_order_id,
                "sd": tracked_order.trade_type.name.lower(),
            },
            is_auth_required=True)

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        order_info = await self._request_order_info(tracked_order=tracked_order)
        return self._create_order_update(order=tracked_order, order_info=order_info)

    def _create_order_update(self, order: InFlightOrder, order_info: Dict[str, Any]) -> OrderUpdate:
        status = str(order_info["status"]).lower()
        if status == "unfilled":
            # The REST API only reports filled | unfilled | cancelled and keeps the partial fill
            # information in a separate boolean
            new_state = OrderState.PARTIALLY_FILLED if order_info.get("partial_filled") else OrderState.OPEN
        else:
            new_state = CONSTANTS.REST_ORDER_STATE[status]

        return OrderUpdate(
            client_order_id=order.client_order_id,
            exchange_order_id=str(order_info.get("id", order.exchange_order_id)),
            trading_pair=order.trading_pair,
            update_timestamp=self.current_timestamp,
            new_state=new_state,
        )

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        trade_updates = []
        if order.exchange_order_id is None:
            return trade_updates
        try:
            order_info = await self._request_order_info(tracked_order=order)
        except asyncio.CancelledError:
            raise
        except Exception as exception:
            if self._is_order_not_found_during_status_update_error(exception):
                return trade_updates
            raise
        return self._create_trade_updates(order=order, order_info=order_info)

    def _create_trade_updates(self, order: InFlightOrder, order_info: Dict[str, Any]) -> List[TradeUpdate]:
        trade_updates = []
        _, quote = split_hb_trading_pair(order.trading_pair)

        for fill in order_info.get("history") or []:
            rate = Decimal(str(fill["rate"]))
            amount = Decimal(str(fill["amount"]))
            # As everywhere else in the Bitkub API, "amount" is the quote amount on buys and the base
            # amount on sells
            if order.trade_type is TradeType.BUY:
                fill_quote_amount = amount
                fill_base_amount = amount / rate
            else:
                fill_base_amount = amount
                fill_quote_amount = amount * rate

            fee = TradeFeeBase.new_spot_fee(
                fee_schema=self.trade_fee_schema(),
                trade_type=order.trade_type,
                percent_token=quote,
                flat_fees=[TokenAmount(amount=Decimal(str(fill["fee"])), token=quote)],
            )
            trade_updates.append(TradeUpdate(
                trade_id=str(fill["txn_id"]),
                client_order_id=order.client_order_id,
                exchange_order_id=str(order_info.get("id", order.exchange_order_id)),
                trading_pair=order.trading_pair,
                fee=fee,
                fill_base_amount=fill_base_amount,
                fill_quote_amount=fill_quote_amount,
                fill_price=rate,
                fill_timestamp=int(fill["timestamp"]) * 1e-3,
            ))
        return trade_updates

    # === User stream ===

    async def _user_stream_event_listener(self):
        async for event_message in self._iter_user_event_queue():
            try:
                # Push messages name their channel in the "event" field
                event = event_message.get("event")
                data = event_message.get("data") or {}
                if event == CONSTANTS.ORDER_UPDATE_CHANNEL:
                    self._process_order_update_event(data)
                elif event == CONSTANTS.MATCH_UPDATE_CHANNEL:
                    self._process_match_update_event(data)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error in user stream listener loop.")

    def _tracked_order_from_event(self, data: Dict[str, Any], orders: Dict[str, InFlightOrder]) -> Optional[InFlightOrder]:
        client_order_id = data.get("client_id")
        order = orders.get(client_order_id) if client_order_id else None
        if order is None:
            exchange_order_id = str(data.get("order_id"))
            order = next((o for o in orders.values() if o.exchange_order_id == exchange_order_id), None)
        return order

    def _process_order_update_event(self, data: Dict[str, Any]):
        order = self._tracked_order_from_event(data, self._order_tracker.all_updatable_orders)
        if order is None:
            return
        status = str(data["status"]).lower()
        if status not in CONSTANTS.WS_ORDER_STATE:
            self.logger().warning(f"Unrecognized Bitkub order status '{status}' for {order.client_order_id}.")
            return
        self._order_tracker.process_order_update(OrderUpdate(
            client_order_id=order.client_order_id,
            exchange_order_id=str(data["order_id"]),
            trading_pair=order.trading_pair,
            # order_updated_at is nullable and reported in milliseconds
            update_timestamp=(int(data["order_updated_at"]) * 1e-3
                              if data.get("order_updated_at") is not None
                              else self.current_timestamp),
            new_state=CONSTANTS.WS_ORDER_STATE[status],
        ))

    def _process_match_update_event(self, data: Dict[str, Any]):
        order = self._tracked_order_from_event(data, self._order_tracker.all_fillable_orders)
        if order is None:
            return
        base, quote = split_hb_trading_pair(order.trading_pair)
        # executed_currency / received_currency identify which side of the pair each amount belongs to,
        # which avoids branching on the order side
        amounts = {
            data["executed_currency"]: Decimal(str(data["executed_amount"])),
            data["received_currency"]: Decimal(str(data["received_amount"])),
        }
        fee = TradeFeeBase.new_spot_fee(
            fee_schema=self.trade_fee_schema(),
            trade_type=order.trade_type,
            percent_token=quote,
            flat_fees=[TokenAmount(amount=Decimal(str(data["total_fee"])), token=quote)],
        )
        self._order_tracker.process_trade_update(TradeUpdate(
            trade_id=str(data["txn_id"]),
            client_order_id=order.client_order_id,
            exchange_order_id=str(data["order_id"]),
            trading_pair=order.trading_pair,
            fee=fee,
            fill_base_amount=amounts[base],
            fill_quote_amount=amounts[quote],
            fill_price=Decimal(str(data["price"])),
            # txn_ts is in seconds here, while the order_update timestamps are in milliseconds
            fill_timestamp=float(data["txn_ts"]),
        ))
