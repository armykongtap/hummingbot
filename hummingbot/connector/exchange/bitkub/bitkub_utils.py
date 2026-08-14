from decimal import Decimal
from typing import Any, Dict

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True

EXAMPLE_PAIR = "BTC-THB"

# Bitkub does not publish a fee schedule in its API docs. 0.25% is the rate used throughout the
# documented examples. Users on a different tier should set an override in conf_fee_overrides.
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.0025"),
    taker_percent_fee_decimal=Decimal("0.0025"),
)


def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    """
    Verifies if a trading pair is enabled to operate with based on its exchange information
    :param exchange_info: the exchange information for a trading pair
    :return: True if the trading pair is enabled, False otherwise
    """
    return (exchange_info.get("status") == "active"
            and not exchange_info.get("freeze_buy", False)
            and not exchange_info.get("freeze_sell", False))


class BitkubConfigMap(BaseConnectorConfigMap):
    connector: str = "bitkub"
    bitkub_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Bitkub API key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    bitkub_secret_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Bitkub API secret",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    model_config = ConfigDict(title="bitkub")


KEYS = BitkubConfigMap.model_construct()
