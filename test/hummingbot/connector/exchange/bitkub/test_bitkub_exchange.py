import asyncio
import json
import re
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from aioresponses import aioresponses
from aioresponses.core import RequestCall

from hummingbot.connector.exchange.bitkub import bitkub_constants as CONSTANTS, bitkub_web_utils as web_utils
from hummingbot.connector.exchange.bitkub.bitkub_exchange import BitkubExchange
from hummingbot.connector.test_support.exchange_connector_test import AbstractExchangeConnectorTests
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest


class BitkubExchangeTests(AbstractExchangeConnectorTests.ExchangeConnectorTests):

    @property
    def all_symbols_url(self):
        return web_utils.public_rest_url(CONSTANTS.SYMBOLS_PATH_URL)

    @property
    def latest_prices_url(self):
        return (f"{web_utils.public_rest_url(CONSTANTS.TICKER_PATH_URL)}"
                f"?sym={self.exchange_trading_pair}")

    @property
    def network_status_url(self):
        return web_utils.public_rest_url(CONSTANTS.SERVER_TIME_PATH_URL)

    @property
    def trading_rules_url(self):
        return self.all_symbols_url

    @property
    def order_creation_url(self):
        # Bitkub uses a different endpoint per side, so both have to be matched
        return re.compile(
            f"^{web_utils.public_rest_url('/api/v3/market/place-')}".replace(".", r"\.") + r"(bid|ask)")

    @property
    def balance_url(self):
        return web_utils.private_rest_url(CONSTANTS.BALANCES_PATH_URL)

    @property
    def order_info_url(self):
        return web_utils.private_rest_url(CONSTANTS.ORDER_INFO_PATH_URL)

    @property
    def order_info_url_regex(self):
        return re.compile(f"^{self.order_info_url}".replace(".", r"\.") + r"\?.*")

    @property
    def cancel_url_regex(self):
        return re.compile(
            f"^{web_utils.private_rest_url(CONSTANTS.CANCEL_ORDER_PATH_URL)}".replace(".", r"\."))

    @property
    def symbol_details(self) -> Dict[str, Any]:
        return {
            "base_asset": self.base_asset,
            "base_asset_scale": 8,
            "base_quantity_scale": 6,
            "buy_price_gap_as_percent": 20,
            "created_at": "2017-10-30T22:16:10+07:00",
            "description": "Test market",
            "freeze_buy": False,
            "freeze_cancel": False,
            "freeze_sell": False,
            "market_segment": "SPOT",
            "min_quote_size": 10,
            "modified_at": "2025-05-20T16:48:04.599+07:00",
            "name": "CoinAlpha",
            "pairing_id": 1,
            "price_scale": 4,
            "price_step": "0.0001",
            "quantity_scale": 6,
            "quantity_step": "0.000001",
            "quote_asset": self.quote_asset,
            "quote_asset_scale": 2,
            "quote_quantity_scale": 2,
            "sell_price_gap_as_percent": 20,
            "status": "active",
            "symbol": self.exchange_trading_pair,
            "source": "exchange",
        }

    @property
    def all_symbols_request_mock_response(self):
        return {"error": 0, "result": [self.symbol_details]}

    @property
    def latest_prices_request_mock_response(self):
        # The ticker endpoint answers with a bare array and no envelope
        return [
            {
                "symbol": self.exchange_trading_pair,
                "base_volume": "1875227.0489781",
                "high_24_hr": "12",
                "highest_bid": "9.99",
                "last": str(self.expected_latest_price),
                "low_24_hr": "9",
                "lowest_ask": "10.01",
                "percent_change": "2.69",
                "quote_volume": "69080877.73",
            }
        ]

    @property
    def all_symbols_including_invalid_pair_mock_response(self) -> Tuple[str, Any]:
        invalid_symbol_details = dict(self.symbol_details)
        invalid_symbol_details.update({
            "base_asset": "INVALID",
            "quote_asset": "PAIR",
            "symbol": "INVALID_PAIR",
            "pairing_id": 2,
            "status": "inactive",
        })
        return "INVALID-PAIR", {"error": 0, "result": [self.symbol_details, invalid_symbol_details]}

    @property
    def network_status_request_successful_mock_response(self):
        # The server time endpoint answers with a bare integer and no envelope
        return 1701251212273

    @property
    def trading_rules_request_mock_response(self):
        return self.all_symbols_request_mock_response

    @property
    def trading_rules_request_erroneous_mock_response(self):
        erroneous_rule = dict(self.symbol_details)
        del erroneous_rule["price_step"]
        return {"error": 0, "result": [erroneous_rule]}

    @property
    def order_creation_request_successful_mock_response(self):
        return {
            "error": 0,
            "result": {
                "id": self.expected_exchange_order_id,
                "typ": "limit",
                "amt": 1000000,
                "rat": 10000,
                "fee": 2500,
                "cre": 0,
                "rec": 100,
                "ts": "1640780000",
                "ci": "someClientId",
            },
        }

    @property
    def balance_request_mock_response_for_base_and_quote(self):
        return {
            "code": "0",
            "message": "success",
            "data": [
                {"currency": self.base_asset, "available": "10", "reserved": "5", "total": "15"},
                {"currency": self.quote_asset, "available": "2000", "reserved": "0", "total": "2000"},
            ],
        }

    @property
    def balance_request_mock_response_only_base(self):
        return {
            "code": "0",
            "message": "success",
            "data": [
                {"currency": self.base_asset, "available": "10", "reserved": "5", "total": "15"},
            ],
        }

    @property
    def balance_event_websocket_update(self):
        # Bitkub's private websocket has no balance channel, so balances are polled instead
        return {}

    @property
    def expected_latest_price(self):
        return 9.6

    @property
    def expected_supported_order_types(self):
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER]

    @property
    def expected_trading_rule(self):
        return TradingRule(
            trading_pair=self.trading_pair,
            min_order_size=Decimal("0.000001"),
            min_price_increment=Decimal(self.symbol_details["price_step"]),
            min_base_amount_increment=Decimal("0.000001"),
            min_quote_amount_increment=Decimal("0.01"),
            min_notional_size=Decimal(str(self.symbol_details["min_quote_size"])),
            supports_market_orders=False,
        )

    @property
    def expected_logged_error_for_erroneous_trading_rule(self):
        erroneous_rule = self.trading_rules_request_erroneous_mock_response["result"][0]
        return f"Error parsing the trading pair rule {erroneous_rule}. Skipping."

    @property
    def expected_exchange_order_id(self):
        return "EOID1"

    @property
    def is_order_fill_http_update_included_in_status_update(self) -> bool:
        # Fills ride along in the "history" array of the order-info response
        return True

    @property
    def is_order_fill_http_update_executed_during_websocket_order_event_processing(self) -> bool:
        # match_update carries everything needed, so no REST call is made while processing it
        return False

    @property
    def expected_partial_fill_price(self) -> Decimal:
        return Decimal("10500")

    @property
    def expected_partial_fill_amount(self) -> Decimal:
        return Decimal("0.5")

    @property
    def expected_fill_fee(self) -> TradeFeeBase:
        return AddedToCostTradeFee(
            percent_token=self.quote_asset,
            flat_fees=[TokenAmount(token=self.quote_asset, amount=Decimal("30"))])

    @property
    def expected_fill_trade_id(self) -> str:
        return "TXN1"

    def exchange_symbol_for_tokens(self, base_token: str, quote_token: str) -> str:
        return f"{base_token}_{quote_token}"

    def create_exchange_instance(self):
        return BitkubExchange(
            bitkub_api_key="testAPIKey",
            bitkub_secret_key="testSecret",
            trading_pairs=[self.trading_pair],
        )

    # === Request validation ===

    def validate_auth_credentials_present(self, request_call: RequestCall):
        request_headers = request_call.kwargs["headers"]
        self.assertEqual("testAPIKey", request_headers["X-BTK-APIKEY"])
        self.assertIn("X-BTK-TIMESTAMP", request_headers)
        self.assertIn("X-BTK-SIGN", request_headers)

    def validate_order_creation_request(self, order: InFlightOrder, request_call: RequestCall):
        request_data = json.loads(request_call.kwargs["data"])
        self.assertEqual(self.exchange_trading_pair, request_data["sym"])
        self.assertEqual("limit", request_data["typ"])
        self.assertEqual(float(order.price), float(request_data["rat"]))
        self.assertEqual(order.client_order_id, request_data["client_id"])
        if order.trade_type is TradeType.BUY:
            # place-bid takes the quote amount to spend
            self.assertEqual(float(order.amount * order.price), float(request_data["amt"]))
        else:
            # place-ask takes the base quantity to sell
            self.assertEqual(float(order.amount), float(request_data["amt"]))

    def validate_order_cancelation_request(self, order: InFlightOrder, request_call: RequestCall):
        request_data = json.loads(request_call.kwargs["data"])
        self.assertEqual(self.exchange_trading_pair, request_data["sym"])
        self.assertEqual(order.exchange_order_id, request_data["id"])
        self.assertEqual(order.trade_type.name.lower(), request_data["sd"])

    def validate_order_status_request(self, order: InFlightOrder, request_call: RequestCall):
        request_params = request_call.kwargs["params"]
        self.assertEqual(self.exchange_trading_pair, request_params["sym"])
        self.assertEqual(order.exchange_order_id, request_params["id"])
        self.assertEqual(order.trade_type.name.lower(), request_params["sd"])

    def validate_trades_request(self, order: InFlightOrder, request_call: RequestCall):
        # Fills come from the same order-info request as the status
        self.validate_order_status_request(order=order, request_call=request_call)

    # === Response configuration ===

    def _order_info_response(self,
                             order: InFlightOrder,
                             status: str,
                             partial_filled: bool = False,
                             fills: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        return {
            "error": 0,
            "result": {
                "id": order.exchange_order_id,
                "first": order.exchange_order_id,
                "parent": "0",
                "last": order.exchange_order_id,
                "client_id": order.client_order_id,
                "post_only": False,
                "amount": "10000",
                "rate": 10000,
                "fee": 30,
                "credit": 0,
                "filled": 10000 if status == "filled" else 0,
                "total": 10000,
                "status": status,
                "partial_filled": partial_filled,
                "remaining": 0,
                "history": fills or [],
            },
        }

    def _fill(self, price: Decimal, base_amount: Decimal) -> Dict[str, Any]:
        # As everywhere else in the Bitkub API, a buy order reports its amount in the quote asset
        return {
            "amount": float(base_amount * price),
            "credit": 0,
            "fee": 30,
            "id": self.expected_exchange_order_id,
            "rate": float(price),
            "timestamp": 1640780000000,
            "txn_id": self.expected_fill_trade_id,
        }

    def _register_order_info(self, mock_api: aioresponses, response: Dict[str, Any], callback: Optional[Callable]):
        mock_api.get(self.order_info_url_regex, body=json.dumps(response), callback=callback, repeat=True)
        return self.order_info_url

    def configure_successful_cancelation_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        mock_api.post(self.cancel_url_regex, body=json.dumps({"error": 0}), callback=callback)
        return web_utils.private_rest_url(CONSTANTS.CANCEL_ORDER_PATH_URL)

    def configure_erroneous_cancelation_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        # Error 23: failed to update order status
        mock_api.post(self.cancel_url_regex, body=json.dumps({"error": 23}), callback=callback)
        return web_utils.private_rest_url(CONSTANTS.CANCEL_ORDER_PATH_URL)

    def configure_order_not_found_error_cancelation_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        # Error 21: invalid order for cancellation
        mock_api.post(self.cancel_url_regex, body=json.dumps({"error": 21}), callback=callback)
        return web_utils.private_rest_url(CONSTANTS.CANCEL_ORDER_PATH_URL)

    def configure_one_successful_one_erroneous_cancel_all_response(
            self,
            successful_order: InFlightOrder,
            erroneous_order: InFlightOrder,
            mock_api: aioresponses) -> List[str]:
        url = web_utils.private_rest_url(CONSTANTS.CANCEL_ORDER_PATH_URL)
        mock_api.post(self.cancel_url_regex, body=json.dumps({"error": 0}))
        mock_api.post(self.cancel_url_regex, body=json.dumps({"error": 23}))
        return [url, url]

    def configure_completely_filled_order_status_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> List[str]:
        response = self._order_info_response(
            order=order,
            status="filled",
            fills=[self._fill(price=order.price, base_amount=order.amount)])
        return [self._register_order_info(mock_api, response, callback)]

    def configure_canceled_order_status_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> Union[str, List[str]]:
        response = self._order_info_response(order=order, status="cancelled")
        return self._register_order_info(mock_api, response, callback)

    def configure_open_order_status_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> List[str]:
        response = self._order_info_response(order=order, status="unfilled")
        return [self._register_order_info(mock_api, response, callback)]

    def configure_http_error_order_status_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        mock_api.get(self.order_info_url_regex, status=401, callback=callback, repeat=True)
        return self.order_info_url

    def configure_partially_filled_order_status_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        response = self._order_info_response(
            order=order,
            status="unfilled",
            partial_filled=True,
            fills=[self._fill(price=self.expected_partial_fill_price,
                              base_amount=self.expected_partial_fill_amount)])
        return self._register_order_info(mock_api, response, callback)

    def configure_order_not_found_error_order_status_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> List[str]:
        # Error 24: invalid order for lookup
        mock_api.get(self.order_info_url_regex,
                     body=json.dumps({"error": 24}), callback=callback, repeat=True)
        return [self.order_info_url]

    def configure_partial_fill_trade_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        # Fills and status share the order-info endpoint, so the status response below serves both
        return ""

    def configure_erroneous_http_fill_trade_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        # Registered without repeat so only the fills request fails; the status request that follows
        # falls through to the response configured after this one
        mock_api.get(self.order_info_url_regex, status=400, callback=callback)
        return self.order_info_url

    def configure_full_fill_trade_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = None) -> str:
        response = self._order_info_response(
            order=order,
            status="filled",
            fills=[self._fill(price=order.price, base_amount=order.amount)])
        return self._register_order_info(mock_api, response, callback)

    # === Websocket events ===

    def _order_update_event(self, order: InFlightOrder, status: str) -> Dict[str, Any]:
        return {
            "event": CONSTANTS.ORDER_UPDATE_CHANNEL,
            "code": "200",
            "message": "Success",
            "data": {
                "user_id": "1",
                "order_id": order.exchange_order_id,
                "client_id": order.client_order_id,
                "symbol": self.exchange_trading_pair,
                "side": order.trade_type.name.lower(),
                "type": "limit",
                "status": status,
                "price": str(order.price),
                "stop_price": None,
                "order_currency": self.quote_asset,
                "order_amount": str(order.amount * order.price),
                "executed_currency": self.quote_asset,
                "executed_amount": str(order.amount * order.price),
                "received_currency": self.base_asset,
                "received_amount": str(order.amount),
                "total_fee": "30",
                "credit_used": "0",
                "net_fee_paid": "30",
                "avg_filled_price": str(order.price),
                "post_only": False,
                "order_created_at": 1640780000000,
                "order_updated_at": 1640780000000,
            },
            "connection_id": "Y33pLftYyQ0CEpQ=",
        }

    def order_event_for_new_order_websocket_update(self, order: InFlightOrder):
        return self._order_update_event(order=order, status="new")

    def order_event_for_canceled_order_websocket_update(self, order: InFlightOrder):
        return self._order_update_event(order=order, status="canceled")

    def order_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        return self._order_update_event(order=order, status="filled")

    def trade_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        return {
            "event": CONSTANTS.MATCH_UPDATE_CHANNEL,
            "code": "200",
            "message": "Success",
            "data": {
                "order_id": order.exchange_order_id,
                "txn_id": self.expected_fill_trade_id,
                "client_id": order.client_order_id,
                "symbol": self.exchange_trading_pair,
                "type": "limit",
                "status": "filled",
                "side": order.trade_type.name.lower(),
                "is_maker": True,
                "price": str(order.price),
                "executed_currency": self.quote_asset,
                "executed_amount": str(order.amount * order.price),
                "received_currency": self.base_asset,
                "received_amount": str(order.amount),
                "fee_rate": "0.0025",
                "total_fee": "30",
                "credit_used": "0",
                "net_fee_paid": "30",
                "txn_ts": 1640780000,
            },
            "connection_id": "Y33pLftYyQ0CEpQ=",
        }

    # === Bitkub specific tests ===

    async def test_market_orders_are_rejected(self):
        # A market buy sizes the quote spend from a reference price, so it could never deliver the
        # requested base amount. ExchangePyBase._create_order refuses the unsupported type.
        self._simulate_trading_rules_initialized()
        self.exchange._set_current_timestamp(1640780000)

        self.assertNotIn(OrderType.MARKET, self.exchange.supported_order_types())

        with aioresponses() as mock_api:
            mock_api.post(self.order_creation_url,
                          body=json.dumps(self.order_creation_request_successful_mock_response),
                          repeat=True)
            order_id = self.place_buy_order(order_type=OrderType.MARKET)
            await asyncio.sleep(0.1)

            # Nothing reached the exchange and the order was failed locally
            self.assertEqual(0, len(mock_api.requests))

        self.assertNotIn(order_id, self.exchange.in_flight_orders)
        self.assertTrue(self.is_logged(
            "ERROR", f"{OrderType.MARKET} is not in the list of supported order types"))

    async def test_taker_order_type_falls_back_to_limit(self):
        self.assertEqual(OrderType.LIMIT, self.exchange.get_taker_order_type())
        self.assertEqual(OrderType.LIMIT_MAKER, self.exchange.get_maker_order_type())

    def test_fills_parse_amounts_returned_in_scientific_notation(self):
        # Bitkub will not accept exponent form on input but returns it on output: an order of
        # 0.00001044 BTC comes back from order-info as 1.044e-05.
        self.exchange.start_tracking_order(
            order_id="cid1", exchange_order_id="eoid1", trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT, trade_type=TradeType.SELL,
            price=Decimal("4218120.82"), amount=Decimal("0.00001044"))
        order = self.exchange.in_flight_orders["cid1"]

        trade_updates = self.exchange._create_trade_updates(order=order, order_info={
            "id": "eoid1",
            "history": [{
                "txn_id": "TXN9", "rate": 4218120.82, "amount": 1.044e-05,
                "fee": 0.11, "credit": 0, "timestamp": 1640780000000,
            }],
        })

        self.assertEqual(1, len(trade_updates))
        # A sell reports the base quantity, so the value is used as-is
        self.assertEqual(Decimal("0.00001044"), trade_updates[0].fill_base_amount)
        self.assertEqual(Decimal("4218120.82"), trade_updates[0].fill_price)

    def test_every_endpoint_has_a_rate_limit(self):
        # AsyncThrottler returns None for an unregistered limit_id, and the request then dies with a
        # bare "'NoneType' object has no attribute 'weight'" that says nothing about the real cause.
        # _api_request defaults throttler_limit_id to the path, so every path constant needs an entry.
        registered = {limit.limit_id for limit in CONSTANTS.RATE_LIMITS}
        endpoints = {value for name, value in vars(CONSTANTS).items()
                     if name.endswith("_PATH_URL") and isinstance(value, str)}

        self.assertEqual(set(), endpoints - registered)

    def test_unwrap_response_raises_on_v3_business_error(self):
        with self.assertRaises(IOError) as context:
            self.exchange._unwrap_response({"error": 18})
        self.assertIn("Bitkub error 18", str(context.exception))

    def test_unwrap_response_raises_on_v4_business_error(self):
        with self.assertRaises(IOError) as context:
            self.exchange._unwrap_response({"code": "V1007-CW", "message": "Symbol not found", "data": {}})
        self.assertIn("Bitkub error V1007-CW", str(context.exception))

    def test_unwrap_response_returns_bare_payloads_untouched(self):
        # /api/status, /api/v3/servertime and /api/v3/market/ticker have no envelope
        self.assertEqual([{"status": "ok"}], self.exchange._unwrap_response([{"status": "ok"}]))
        self.assertEqual(1701251212273, self.exchange._unwrap_response(1701251212273))

    def test_unwrap_response_returns_the_payload_of_each_envelope(self):
        self.assertEqual({"id": "1"}, self.exchange._unwrap_response({"error": 0, "result": {"id": "1"}}))
        self.assertEqual([{"currency": "THB"}],
                         self.exchange._unwrap_response({"code": "0", "data": [{"currency": "THB"}]}))
        # cancel-order answers with no result body at all
        self.assertEqual({"error": 0}, self.exchange._unwrap_response({"error": 0}))

    def test_error_classifiers_read_the_bitkub_error_code(self):
        self.assertTrue(self.exchange._is_request_exception_related_to_time_synchronizer(
            IOError("Bitkub error 8 - response: {'error': 8}")))
        self.assertTrue(self.exchange._is_order_not_found_during_cancelation_error(
            IOError("Bitkub error 21 - response: {'error': 21}")))
        self.assertTrue(self.exchange._is_order_not_found_during_status_update_error(
            IOError("Bitkub error 24 - response: {'error': 24}")))

        self.assertFalse(self.exchange._is_order_not_found_during_status_update_error(IOError("Timeout")))
        self.assertFalse(self.exchange._is_order_not_found_during_cancelation_error(
            IOError("Bitkub error 24 - response: {'error': 24}")))

    def test_format_decimal_drops_trailing_zeros_and_never_uses_exponent_form(self):
        self.assertEqual("1000", self.exchange._format_decimal(Decimal("1000.00")))
        self.assertEqual("0.1", self.exchange._format_decimal(Decimal("0.10000000")))
        self.assertEqual("0", self.exchange._format_decimal(Decimal("0")))
        # A 10 THB order on BTC-THB is roughly 4.76e-6 BTC; float(...) would serialize it as "4.76e-06"
        self.assertEqual("0.00000476", self.exchange._format_decimal(Decimal("0.00000476")))
        self.assertEqual("0.00000001", self.exchange._format_decimal(Decimal("1E-8")))
        self.assertEqual("2100000.01", self.exchange._format_decimal(Decimal("2100000.010")))

    async def test_small_order_amount_is_sent_as_a_fixed_point_json_number(self):
        # Guards the whole chain: fixed-point string -> json.dumps -> pre-processor unquoting
        request = RESTRequest(
            method=RESTMethod.POST,
            url=web_utils.private_rest_url(CONSTANTS.PLACE_ASK_PATH_URL),
            data=json.dumps({
                "sym": self.exchange_trading_pair,
                "amt": self.exchange._format_decimal(Decimal("0.00000476")),
                "rat": self.exchange._format_decimal(Decimal("2100000.00")),
                "typ": "limit",
                "client_id": "someClientId",
            }))

        processed = await web_utils.BitkubRESTPreProcessor().pre_process(request)

        self.assertIn('"amt": 0.00000476', processed.data)
        self.assertIn('"rat": 2100000', processed.data)
        self.assertNotIn("e-", processed.data)
        # The values are real JSON numbers, and the string fields are left alone
        body = json.loads(processed.data)
        self.assertEqual(Decimal("0.00000476"), Decimal(str(body["amt"])))
        self.assertEqual(self.exchange_trading_pair, body["sym"])
        self.assertEqual("someClientId", body["client_id"])
        self.assertEqual("limit", body["typ"])

    async def test_pre_processor_leaves_non_numeric_bodies_untouched(self):
        # cancel-order carries no numeric field, and "id" must stay a string
        original = json.dumps({"sym": "thb_btc", "id": "289", "sd": "buy"})
        request = RESTRequest(
            method=RESTMethod.POST,
            url=web_utils.private_rest_url(CONSTANTS.CANCEL_ORDER_PATH_URL),
            data=original)

        processed = await web_utils.BitkubRESTPreProcessor().pre_process(request)

        self.assertEqual(original, processed.data)
        self.assertEqual("289", json.loads(processed.data)["id"])

    async def test_trading_rules_ignore_the_unusable_quantity_step(self):
        # The live API reports quantity_step "1" and quantity_scale 0 for every symbol, including
        # BTC_THB. Only base_quantity_scale carries the real precision.
        live_shaped_rule = dict(self.symbol_details)
        live_shaped_rule.update({
            "quantity_step": "1",
            "quantity_scale": 0,
            "base_quantity_scale": 8,
            "quote_quantity_scale": 2,
            "price_step": "0.01",
        })

        rules = await self.exchange._format_trading_rules([live_shaped_rule])

        self.assertEqual(1, len(rules))
        self.assertEqual(Decimal("1E-8"), rules[0].min_base_amount_increment)
        self.assertEqual(Decimal("1E-8"), rules[0].min_order_size)
        self.assertEqual(Decimal("0.01"), rules[0].min_price_increment)
        self.assertEqual(Decimal("0.01"), self.exchange._quote_amount_increments[self.trading_pair])
        self.assertEqual(Decimal("10"), rules[0].min_notional_size)

    async def test_trading_rules_fall_back_to_the_asset_scales(self):
        rule_without_quantity_scales = dict(self.symbol_details)
        del rule_without_quantity_scales["base_quantity_scale"]
        del rule_without_quantity_scales["quote_quantity_scale"]

        rules = await self.exchange._format_trading_rules([rule_without_quantity_scales])

        self.assertEqual(Decimal("1E-8"), rules[0].min_base_amount_increment)
        self.assertEqual(Decimal("0.01"), rules[0].min_quote_amount_increment)

    async def test_cancel_uses_the_canonical_symbol(self):
        # The documentation says this endpoint needs the reversed "thb_btc" form, but a live
        # cancellation with the canonical form returns error 0, so the note is stale.
        self.exchange.start_tracking_order(
            order_id="cid1", exchange_order_id="eoid1", trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT, trade_type=TradeType.BUY,
            price=Decimal("10000"), amount=Decimal("1"))
        order = self.exchange.in_flight_orders["cid1"]

        with aioresponses() as mock_api:
            mock_api.post(self.cancel_url_regex, body=json.dumps({"error": 0}))
            self.assertTrue(await self.exchange._place_cancel("cid1", order))

        request_data = json.loads(
            self._all_executed_requests(
                mock_api, web_utils.private_rest_url(CONSTANTS.CANCEL_ORDER_PATH_URL))[0].kwargs["data"])
        self.assertEqual(self.exchange_trading_pair, request_data["sym"])
        self.assertEqual("eoid1", request_data["id"])
        self.assertEqual("buy", request_data["sd"])

    @aioresponses()
    async def test_pairing_ids_are_cached_from_the_symbols_response(self, mock_api):
        self.exchange._set_trading_pair_symbol_map(None)
        self.configure_all_symbols_response(mock_api=mock_api)

        pairing_id = await self.exchange.pairing_id_for_trading_pair(self.trading_pair)

        self.assertEqual(self.symbol_details["pairing_id"], pairing_id)

    @aioresponses()
    async def test_buy_and_sell_orders_use_the_side_specific_endpoint_and_amount(self, mock_api):
        self._simulate_trading_rules_initialized()
        self.exchange._set_current_timestamp(1640780000)
        request_sent_event = asyncio.Event()

        mock_api.post(self.order_creation_url,
                      body=json.dumps(self.order_creation_request_successful_mock_response),
                      callback=lambda *args, **kwargs: request_sent_event.set(),
                      repeat=True)

        self.place_buy_order(amount=Decimal("2"), price=Decimal("10000"))
        await request_sent_event.wait()
        request_sent_event.clear()
        self.place_sell_order(amount=Decimal("2"), price=Decimal("10000"))
        await request_sent_event.wait()
        await asyncio.sleep(0.1)

        # Each side has to reach its own endpoint
        requests_by_path = {key[1].path: calls for key, calls in mock_api.requests.items()}
        self.assertIn(CONSTANTS.PLACE_BID_PATH_URL, requests_by_path)
        self.assertIn(CONSTANTS.PLACE_ASK_PATH_URL, requests_by_path)

        buy_data = json.loads(requests_by_path[CONSTANTS.PLACE_BID_PATH_URL][0].kwargs["data"])
        sell_data = json.loads(requests_by_path[CONSTANTS.PLACE_ASK_PATH_URL][0].kwargs["data"])

        # The buy spends quote, the sell delivers base
        self.assertEqual(20000, buy_data["amt"])
        self.assertEqual(2, sell_data["amt"])
        self.assertEqual(10000, buy_data["rat"])
        self.assertEqual(10000, sell_data["rat"])
