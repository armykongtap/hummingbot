# A single source of truth for constant variables related to the exchange

from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

EXCHANGE_NAME = "bitkub"
DEFAULT_DOMAIN = ""

REST_URL = "https://api.bitkub.com"
# The public order book stream carries a single symbol per connection, keyed by the numeric pairing id
WSS_PUBLIC_ORDERBOOK_URL = "wss://api.bitkub.com/websocket-api/orderbook/{}"
WSS_PRIVATE_URL = "wss://stream.bitkub.com/v3/private"

# Bitkub requires a User-Agent header when connecting to the private stream from a server-to-server context
WS_USER_AGENT = "python-websocket-client/2.3.1"
# Bitkub asks for a ping at least every 5 minutes; the docs recommend 4 minutes
WS_PING_INTERVAL = 240.0
WS_HEARTBEAT_TIMEOUT = 30.0

MAX_ORDER_ID_LEN = 32
HBOT_ORDER_ID_PREFIX = ""

# REST API ENDPOINTS
SERVER_TIME_PATH_URL = "/api/v3/servertime"
SYMBOLS_PATH_URL = "/api/v3/market/symbols"
TICKER_PATH_URL = "/api/v3/market/ticker"
DEPTH_PATH_URL = "/api/v3/market/depth"
PLACE_BID_PATH_URL = "/api/v3/market/place-bid"
PLACE_ASK_PATH_URL = "/api/v3/market/place-ask"
CANCEL_ORDER_PATH_URL = "/api/v3/market/cancel-order"
ORDER_INFO_PATH_URL = "/api/v3/market/order-info"
# Balances only exist on v4; the v3 wallet/balances endpoints were removed on 2026-05-26
BALANCES_PATH_URL = "/api/v4/wallet/balances"

# Default depth requested when building an order book snapshot over REST
ORDER_BOOK_SNAPSHOT_DEPTH = 100

# WS THROTTLER IDS
WS_CONNECT = "WSConnect"
WS_SUBSCRIBE = "WSSubscribe"

# PRIVATE WS CHANNELS
ORDER_UPDATE_CHANNEL = "order_update"
MATCH_UPDATE_CHANNEL = "match_update"

# PUBLIC ORDER BOOK WS EVENTS
DEPTH_CHANGED_EVENT = "depthchanged"
TRADES_CHANGED_EVENT = "tradeschanged"

ONE_SECOND = 1
TEN_SECONDS = 10

# Bitkub applies its rate limits per endpoint, regardless of the API version.
# A breach blocks the caller for 30 seconds and returns HTTP 429.
RATE_LIMITS = [
    RateLimit(limit_id=SERVER_TIME_PATH_URL, limit=2000, time_interval=TEN_SECONDS),
    RateLimit(limit_id=SYMBOLS_PATH_URL, limit=100, time_interval=ONE_SECOND),
    RateLimit(limit_id=TICKER_PATH_URL, limit=100, time_interval=ONE_SECOND),
    # The tightest limit in the whole API
    RateLimit(limit_id=DEPTH_PATH_URL, limit=10, time_interval=ONE_SECOND),
    RateLimit(limit_id=PLACE_BID_PATH_URL, limit=150, time_interval=ONE_SECOND),
    RateLimit(limit_id=PLACE_ASK_PATH_URL, limit=150, time_interval=ONE_SECOND),
    RateLimit(limit_id=CANCEL_ORDER_PATH_URL, limit=200, time_interval=ONE_SECOND),
    RateLimit(limit_id=ORDER_INFO_PATH_URL, limit=100, time_interval=ONE_SECOND),
    RateLimit(limit_id=BALANCES_PATH_URL, limit=150, time_interval=ONE_SECOND),
    RateLimit(limit_id=WS_CONNECT, limit=30, time_interval=60),
    RateLimit(limit_id=WS_SUBSCRIBE, limit=100, time_interval=TEN_SECONDS),
]

# GET /api/v3/market/order-info only reports filled | unfilled | cancelled, and carries the partial fill
# information in a separate "partial_filled" boolean. See BitkubExchange._create_order_update.
REST_ORDER_STATE = {
    "filled": OrderState.FILLED,
    "unfilled": OrderState.OPEN,
    "cancelled": OrderState.CANCELED,
    "canceled": OrderState.CANCELED,
}

# The private websocket reports a richer set of states than the REST API does
WS_ORDER_STATE = {
    "new": OrderState.OPEN,
    "open": OrderState.OPEN,
    "untriggered": OrderState.PENDING_CREATE,
    "partial_filled": OrderState.PARTIALLY_FILLED,
    "filled": OrderState.FILLED,
    "rejected": OrderState.FAILED,
    "canceled": OrderState.CANCELED,
    "cancelled": OrderState.CANCELED,
    "partial_filled_canceled": OrderState.CANCELED,
}

# Error codes used to classify failures (see the error table in restful-api.md)
TIME_SYNC_ERROR_CODES = {8}  # Invalid timestamp
ORDER_NOT_FOUND_ON_CANCEL_ERROR_CODES = {21}  # Invalid order for cancellation
ORDER_NOT_FOUND_ON_LOOKUP_ERROR_CODES = {24}  # Invalid order for lookup
