#!/usr/bin/env python
"""
Standalone probe for the Bitkub public order book websocket.

Runs the connector's own order book data source end to end - real connection, real parsing -
without a password, a strategy or any API key. Prints the REST snapshot first, then whatever the
websocket pushes.

    python scripts/bitkub_ws_probe.py [TRADING_PAIR] [SECONDS]
"""
import asyncio
import sys
from decimal import Decimal

from hummingbot.connector.exchange.bitkub.bitkub_exchange import BitkubExchange
from hummingbot.core.data_type.order_book_message import OrderBookMessage


async def drain(name: str, queue: asyncio.Queue, counters: dict):
    while True:
        message: OrderBookMessage = await queue.get()
        counters[name] += 1
        if name == "snapshot":
            best_bid = message.bids[0] if message.bids else None
            best_ask = message.asks[0] if message.asks else None
            print(f"[snapshot #{counters[name]:>3}] bids={len(message.bids):>3} asks={len(message.asks):>3} "
                  f"best_bid={best_bid.price if best_bid else '-'} best_ask={best_ask.price if best_ask else '-'}",
                  flush=True)
        else:
            print(f"[trade    #{counters[name]:>3}] {message.content['trade_type']:>3} "
                  f"price={message.content['price']} amount={message.content['amount']} "
                  f"id={message.content['trade_id']}", flush=True)


async def main(trading_pair: str, seconds: float):
    connector = BitkubExchange(
        bitkub_api_key="",
        bitkub_secret_key="",
        trading_pairs=[trading_pair],
        trading_required=False,
    )
    data_source = connector._create_order_book_data_source()

    pairing_id = await connector.pairing_id_for_trading_pair(trading_pair)
    symbol = await connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
    print(f"pair={trading_pair} symbol={symbol} pairing_id={pairing_id}")
    print(f"ws url=wss://api.bitkub.com/websocket-api/orderbook/{pairing_id}\n", flush=True)

    snapshot = await data_source._order_book_snapshot(trading_pair)
    print(f"REST snapshot: bids={len(snapshot.bids)} asks={len(snapshot.asks)} "
          f"best_bid={snapshot.bids[0].price} best_ask={snapshot.asks[0].price} "
          f"spread={Decimal(str(snapshot.asks[0].price)) - Decimal(str(snapshot.bids[0].price))}\n", flush=True)

    snapshot_queue: asyncio.Queue = asyncio.Queue()
    trade_queue: asyncio.Queue = asyncio.Queue()
    counters = {"snapshot": 0, "trade": 0}

    tasks = [
        asyncio.ensure_future(data_source.listen_for_subscriptions()),
        asyncio.ensure_future(data_source.listen_for_order_book_snapshots(asyncio.get_event_loop(), snapshot_queue)),
        asyncio.ensure_future(data_source.listen_for_trades(asyncio.get_event_loop(), trade_queue)),
        asyncio.ensure_future(drain("snapshot", snapshot_queue, counters)),
        asyncio.ensure_future(drain("trade", trade_queue, counters)),
    ]
    print(f"listening for {seconds:.0f}s ...\n", flush=True)
    await asyncio.sleep(seconds)
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await connector.stop_network()

    print(f"\nRESULT snapshots={counters['snapshot']} trades={counters['trade']}")
    if counters["snapshot"] == 0:
        print("FAIL: no websocket order book messages arrived")
        return 1
    print("PASS: websocket order book is streaming")
    return 0


if __name__ == "__main__":
    pair = sys.argv[1] if len(sys.argv) > 1 else "USDC-THB"
    duration = float(sys.argv[2]) if len(sys.argv) > 2 else 30.0
    sys.exit(asyncio.get_event_loop().run_until_complete(main(pair, duration)))
