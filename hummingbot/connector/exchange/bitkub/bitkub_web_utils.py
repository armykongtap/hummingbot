import re
from typing import Callable, Optional

import hummingbot.connector.exchange.bitkub.bitkub_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.utils import TimeSynchronizerRESTPreProcessor
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest
from hummingbot.core.web_assistant.rest_pre_processors import RESTPreProcessorBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

# Body fields Bitkub expects as JSON numbers. They are built as fixed-point strings by
# BitkubExchange._format_decimal and unquoted here, because json.dumps always renders a float
# through repr(): an order of 0.00000476 BTC would otherwise be sent as 4.76e-06.
NUMERIC_BODY_FIELDS = ("amt", "rat")

# Only matches values that are already plain fixed-point decimals, so it cannot touch a symbol,
# a client id, or any other string field that happens to share a name.
QUOTED_NUMBER_PATTERN = re.compile(
    r'"(' + "|".join(NUMERIC_BODY_FIELDS) + r')":(\s*)"(-?\d+(?:\.\d+)?)"')


class BitkubRESTPreProcessor(RESTPreProcessorBase):
    """
    Applies the two body/header adjustments Bitkub needs on every request.

    Bitkub documents ``Accept: application/json`` and ``Content-type: application/json`` on every
    request, while RESTAssistant defaults GET requests to ``application/x-www-form-urlencoded``.

    Numeric body fields are also converted from their fixed-point string form to bare JSON numbers.
    This runs before the request is signed (``RESTAssistant.call`` pre-processes, then authenticates),
    so the signature always covers the bytes that are actually transmitted.
    """

    async def pre_process(self, request: RESTRequest) -> RESTRequest:
        headers = dict(request.headers or {})
        headers["Accept"] = "application/json"
        headers["Content-Type"] = "application/json"
        request.headers = headers

        if isinstance(request.data, str):
            request.data = QUOTED_NUMBER_PATTERN.sub(r'"\1":\2\3', request.data)

        return request


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Creates a full URL for a provided REST endpoint

    :param path_url: a REST endpoint, starting with a slash
    :param domain: unused, Bitkub exposes a single domain

    :return: the full URL to the endpoint
    """
    return f"{CONSTANTS.REST_URL}{path_url}"


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Bitkub serves public and private endpoints from the same host.
    """
    return public_rest_url(path_url=path_url, domain=domain)


def build_api_factory(
        throttler: Optional[AsyncThrottler] = None,
        time_synchronizer: Optional[TimeSynchronizer] = None,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
        time_provider: Optional[Callable] = None,
        auth: Optional[AuthBase] = None) -> WebAssistantsFactory:
    throttler = throttler or create_throttler()
    time_synchronizer = time_synchronizer or TimeSynchronizer()
    time_provider = time_provider or (lambda: get_current_server_time(throttler=throttler, domain=domain))
    api_factory = WebAssistantsFactory(
        throttler=throttler,
        auth=auth,
        rest_pre_processors=[
            TimeSynchronizerRESTPreProcessor(synchronizer=time_synchronizer, time_provider=time_provider),
            BitkubRESTPreProcessor(),
        ])
    return api_factory


def build_api_factory_without_time_synchronizer_pre_processor(throttler: AsyncThrottler) -> WebAssistantsFactory:
    return WebAssistantsFactory(throttler=throttler, rest_pre_processors=[BitkubRESTPreProcessor()])


def create_throttler() -> AsyncThrottler:
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


async def get_current_server_time(
        throttler: Optional[AsyncThrottler] = None,
        domain: str = CONSTANTS.DEFAULT_DOMAIN) -> float:
    """
    Returns the exchange server time in milliseconds, which is the unit TimeSynchronizer expects.

    ``/api/v3/servertime`` answers with a bare integer and no ``{"error": ..., "result": ...}`` envelope.
    The unversioned ``/api/servertime`` endpoint is deliberately not used: it reports seconds and the
    documentation states it cannot be used to sign v3 secure endpoints.
    """
    throttler = throttler or create_throttler()
    api_factory = build_api_factory_without_time_synchronizer_pre_processor(throttler=throttler)
    rest_assistant = await api_factory.get_rest_assistant()
    response = await rest_assistant.execute_request(
        url=public_rest_url(path_url=CONSTANTS.SERVER_TIME_PATH_URL, domain=domain),
        method=RESTMethod.GET,
        throttler_limit_id=CONSTANTS.SERVER_TIME_PATH_URL,
    )
    return float(response)
