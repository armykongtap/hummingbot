import asyncio
import hashlib
import hmac
import json
from typing import Awaitable
from unittest import TestCase
from unittest.mock import MagicMock

from hummingbot.connector.exchange.bitkub.bitkub_auth import BitkubAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest


class BitkubAuthTests(TestCase):

    def setUp(self) -> None:
        super().setUp()
        self.api_key = "testApiKey"
        self.secret_key = "testSecretKey"
        self.timestamp_ms = 1699381086593
        self.time_provider = MagicMock()
        # TimeSynchronizer.time() returns seconds
        self.time_provider.time.return_value = self.timestamp_ms * 1e-3
        self.auth = BitkubAuth(
            api_key=self.api_key,
            secret_key=self.secret_key,
            time_provider=self.time_provider)

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: float = 1):
        return asyncio.get_event_loop().run_until_complete(asyncio.wait_for(coroutine, timeout))

    def _expected_signature(self, payload: str) -> str:
        return hmac.new(self.secret_key.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()

    def test_signature_payload_for_get_request_includes_the_query_string(self):
        request = RESTRequest(
            method=RESTMethod.GET,
            url="https://api.bitkub.com/api/v3/market/my-order-history",
            params={"sym": "BTC_THB"},
            is_auth_required=True)

        payload = self.auth.signature_payload(request=request, timestamp=str(self.timestamp_ms))

        # Verbatim from the signature section of the official documentation
        self.assertEqual("1699381086593GET/api/v3/market/my-order-history?sym=BTC_THB", payload)

    def test_signature_payload_for_get_request_without_params_omits_the_query_string(self):
        request = RESTRequest(
            method=RESTMethod.GET,
            url="https://api.bitkub.com/api/v4/wallet/balances",
            is_auth_required=True)

        payload = self.auth.signature_payload(request=request, timestamp=str(self.timestamp_ms))

        self.assertEqual("1699381086593GET/api/v4/wallet/balances", payload)

    def test_signature_payload_for_post_request_uses_the_serialized_body_verbatim(self):
        # RESTAssistant serialises the body before the request reaches the auth layer
        body = json.dumps({"sym": "thb_btc", "amt": 1000, "rat": 10, "typ": "limit"})
        request = RESTRequest(
            method=RESTMethod.POST,
            url="https://api.bitkub.com/api/v3/market/place-bid",
            data=body,
            is_auth_required=True)

        payload = self.auth.signature_payload(request=request, timestamp="1699376552354")

        self.assertEqual(f"1699376552354POST/api/v3/market/place-bid{body}", payload)
        # The body must not be re-serialized: the transmitted bytes are what gets signed
        self.assertTrue(payload.endswith('{"sym": "thb_btc", "amt": 1000, "rat": 10, "typ": "limit"}'))

    def test_rest_authenticate_adds_the_expected_headers(self):
        request = RESTRequest(
            method=RESTMethod.GET,
            url="https://api.bitkub.com/api/v3/market/my-open-orders",
            params={"sym": "BTC_THB"},
            headers={"Accept": "application/json"},
            is_auth_required=True)

        authenticated = self.async_run_with_timeout(self.auth.rest_authenticate(request))

        expected_signature = self._expected_signature(
            f"{self.timestamp_ms}GET/api/v3/market/my-open-orders?sym=BTC_THB")

        self.assertEqual(self.api_key, authenticated.headers["X-BTK-APIKEY"])
        self.assertEqual(str(self.timestamp_ms), authenticated.headers["X-BTK-TIMESTAMP"])
        self.assertEqual(expected_signature, authenticated.headers["X-BTK-SIGN"])
        # Pre-existing headers are preserved
        self.assertEqual("application/json", authenticated.headers["Accept"])

    def test_websocket_auth_payload_signs_the_timestamp_alone(self):
        payload = self.auth.websocket_auth_payload()

        self.assertEqual("auth", payload["event"])
        self.assertEqual(self.api_key, payload["data"]["X-BTK-APIKEY"])
        self.assertEqual(str(self.timestamp_ms), payload["data"]["X-BTK-TIMESTAMP"])
        self.assertEqual(self._expected_signature(str(self.timestamp_ms)), payload["data"]["X-BTK-SIGN"])

    def test_ws_authenticate_is_a_pass_through(self):
        request = MagicMock()
        self.assertEqual(request, self.async_run_with_timeout(self.auth.ws_authenticate(request)))
