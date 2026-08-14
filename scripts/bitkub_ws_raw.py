#!/usr/bin/env python
"""Dump raw event names from the Bitkub public order book websocket.

    python scripts/bitkub_ws_raw.py [PAIRING_ID] [SECONDS]
"""
import asyncio
import json
import sys
from collections import Counter

import aiohttp


async def main(pairing_id: int, seconds: float):
    url = f"wss://api.bitkub.com/websocket-api/orderbook/{pairing_id}"
    print(f"connecting {url}", flush=True)
    counts = Counter()
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(url, heartbeat=30) as ws:
            print("connected\n", flush=True)
            deadline = asyncio.get_event_loop().time() + seconds
            while asyncio.get_event_loop().time() < deadline:
                remaining = deadline - asyncio.get_event_loop().time()
                try:
                    msg = await asyncio.wait_for(ws.receive(), timeout=max(remaining, 0.1))
                except asyncio.TimeoutError:
                    break
                if msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING,
                                aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    print(f"socket closed by peer: {msg.type!r}", flush=True)
                    break
                if msg.type is not aiohttp.WSMsgType.TEXT:
                    print(f"ignoring frame: {msg.type!r}", flush=True)
                    continue
                payload = json.loads(msg.data)
                event = payload.get("event", "<no event key>")
                counts[event] += 1
                if counts[event] <= 2:
                    print(f"--- {event} (keys={list(payload)}) ---", flush=True)
                    print(json.dumps(payload, indent=2)[:900], flush=True)
    print("\nevent counts:")
    for event, count in counts.most_common():
        print(f"  {event:<20} {count}")


if __name__ == "__main__":
    pid = int(sys.argv[1]) if len(sys.argv) > 1 else 46
    duration = float(sys.argv[2]) if len(sys.argv) > 2 else 30.0
    asyncio.run(main(pid, duration))
