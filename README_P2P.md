# P2P Overlay Milestone: Chord-style DHT for Restaurant Discovery

## Overlay summary
- **Topology:** ring of peers, identifiers = SHA-1("127.0.0.1:port") mod 256 (M = 8 bits).
- **Peer state:** predecessor, successor list (2), finger table (8), local resource store.
- **Resource:** restaurant records (`name -> host/port/menu`) stored on the peer that owns hash(name), i.e. its successor on the ring. Each record is replicated on the next 2 successors.
- **Lookup:** recursive finger-table routing, O(log N) hops; every hop is logged.
- **Join:** FIND_SUCCESSOR(own id) via any peer -> pull owned keys from successor -> NOTIFY; stabilization completes the links.
- **Leave:** graceful = hand keys to successor, relink predecessor/successor. Crash = detected by stabilize/check_predecessor; replicas keep data available.

## Required software
Python 3.8+ (standard library only, Linux/macOS; Windows works for peers but `demo_p2p.py` is plain subprocess so it also works).

## Install dependencies
None. `pip install` is not needed.

## Files
`peer.py` (peer node + CLI), `demo_p2p.py` (automated demo), `sample_demo_log.txt` (captured run).
The original `order_service.py`, `restaurant.py`, `driver.py`, `customer.py` are unchanged.

## Start each service
```
python peer.py start --port 7001                # first peer
python peer.py start --port 7002 --join 7001    # more peers (any live peer can be the bootstrap)
python peer.py start --port 7003 --join 7001
python peer.py start --port 7004 --join 7001
# add --verbose to print marshalling before/after
```

## Run the demo
Automated (recording-friendly, press Enter between steps):
```
python demo_p2p.py --pause
```
Or manually, in separate terminals:
```
python peer.py publish --via 7001 "DiDi Express Burger" '{"host":"127.0.0.1","port":6000,"menu":["Bacon Cheese Burger"]}'
python peer.py lookup  --via 7002 "DiDi Express Burger"     # routed lookup
python peer.py start --port 7005 --join 7002                # JOIN
python peer.py leave --port 7003                            # graceful LEAVE
# CRASH: Ctrl+C / kill the peer that owns the key, then repeat lookup
python peer.py status --port 7001                           # show pred/successors/fingers/keys
```

## Reproduce the test cases
| Test | Command / step | Expected |
|---|---|---|
| 4+ peers start | demo step 1 | ring forms, successors logged |
| Resource discovery | demo steps 2-3 | `FOUND ...` + route like `7002 -> 7004 -> 7001 -> 7002` |
| Join | demo steps 4-5 | "Successor changed", new peer receives keys, lookup via new peer works |
| Graceful leave | demo steps 6-7 | keys handed off, lookups still succeed |
| Crash | demo steps 8-10 | "unreachable; trying next replica", `FOUND ... (replica)`, then ring repairs |

## Optional integration with the order system
`customer.py` can call `python peer.py lookup --via 7001 "DiDi Express Burger"` logic (the `LOOKUP` op over TCP to any peer) to discover the restaurant's order-service address before placing an order.
