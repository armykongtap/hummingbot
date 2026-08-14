import hashlib
import hmac
from typing import Any, Dict
from urllib.parse import urlencode, urlparse

from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class BitkubAuth(AuthBase):
    """
    Auth class required by the Bitkub API.

    The signature is an HMAC-SHA256 hex digest over the plain concatenation of

        timestamp + METHOD + path + queryString + jsonBody

    with no separators, for example::

        1699381086593GET/api/v3/market/my-order-history?sym=BTC_THB
        1699376552354POST/api/v3/market/place-bid{"sym": "thb_btc", "amt": 1000}

    The same scheme applies to both the v3 and the v4 endpoints.
    Learn more at https://github.com/bitkub/bitkub-official-api-docs/blob/master/restful-api.md#signature
    """

    def __init__(self, api_key: str, secret_key: str, time_provider: TimeSynchronizer):
        self.api_key = api_key
        self.secret_key = secret_key
        self.time_provider: TimeSynchronizer = time_provider

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        Adds the API key, timestamp and signature headers required for authenticated interactions.

        :param request: the request to be configured for authenticated interaction

        :return: the RESTRequest with auth information included
        """
        headers = {}
        if request.headers is not None:
            headers.update(request.headers)
        headers.update(self.authentication_headers(request=request))
        request.headers = headers
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        """
        Bitkub authenticates the private websocket with an ``auth`` message rather than a header, so
        this is a pass-through. See :meth:`websocket_auth_payload`.
        """
        return request

    def _timestamp(self) -> str:
        # TimeSynchronizer.time() returns seconds; Bitkub requires milliseconds
        return str(int(self.time_provider.time() * 1e3))

    def _sign(self, payload: str) -> str:
        return hmac.new(
            self.secret_key.encode("utf-8"),
            payload.encode("utf-8"),
            hashlib.sha256).hexdigest()

    @staticmethod
    def _query_string(request: RESTRequest) -> str:
        if not request.params:
            return ""
        # The leading "?" is part of the signed payload
        return f"?{urlencode(request.params)}"

    @staticmethod
    def _body(request: RESTRequest) -> str:
        # RESTAssistant serialises the body to JSON before the request reaches the auth layer, so
        # request.data already holds the exact string aiohttp will transmit. It must be signed
        # verbatim - re-serialising it would change the separators and break the signature.
        if request.data is None:
            return ""
        return request.data if isinstance(request.data, str) else str(request.data)

    def signature_payload(self, request: RESTRequest, timestamp: str) -> str:
        path = urlparse(request.url).path
        return f"{timestamp}{request.method.value}{path}{self._query_string(request)}{self._body(request)}"

    def authentication_headers(self, request: RESTRequest) -> Dict[str, Any]:
        timestamp = self._timestamp()
        return {
            "X-BTK-APIKEY": self.api_key,
            "X-BTK-TIMESTAMP": timestamp,
            "X-BTK-SIGN": self._sign(self.signature_payload(request=request, timestamp=timestamp)),
        }

    def websocket_auth_payload(self) -> Dict[str, Any]:
        """
        The private websocket signs the timestamp on its own, unlike the REST endpoints.
        """
        timestamp = self._timestamp()
        return {
            "event": "auth",
            "data": {
                "X-BTK-APIKEY": self.api_key,
                "X-BTK-SIGN": self._sign(timestamp),
                "X-BTK-TIMESTAMP": timestamp,
            },
        }
