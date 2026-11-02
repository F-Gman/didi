"""P2P overlay peer: a small Chord-style DHT ring for restaurant discovery.

Usage:
  python peer.py start  --port 7001                 # first peer (creates ring)
  python peer.py start  --port 7002 --join 7001     # join through any peer
  python peer.py publish --via 7001 "DiDi Express Burger" '{"host":"127.0.0.1","port":6000}'
  python peer.py lookup  --via 7002 "DiDi Express Burger"
  python peer.py status  --port 7001
  python peer.py leave   --port 7002                # graceful leave
  (kill the process to simulate a crash)
"""
import argparse
import hashlib
import json
import os
import socket
import sys
import threading
import time
from datetime import datetime

HOST = '127.0.0.1'
M = 8                      # identifier bits -> ring of 256 ids
RING = 1 << M
SUCC_LIST_SIZE = 2         # successors tracked; also the number of replicas
TIMEOUT = 2.0
ROUTE_TIMEOUT = 4.0        # recursive routing nests RPCs, so allow more time
MAINTENANCE_PERIOD = 1.0
MAX_MESSAGE_BYTES = 65536
VERBOSE = False            # --verbose shows marshalling before/after


def log(me, message):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [PEER {me['port']} id={me['id']}] {message}",
          end="\n\n", flush=True)


def hash_id(text):
    return int(hashlib.sha1(text.encode("utf-8")).hexdigest(), 16) % RING


def between(x, a, b):
    """x in (a, b] on the ring. If a == b the interval is the whole ring."""
    if a < b:
        return a < x <= b
    return x > a or x <= b


def between_open(x, a, b):
    """x in (a, b) on the ring."""
    if a < b:
        return a < x < b
    if a > b:
        return x > a or x < b
    return x != a


def marshal(data, me=None):
    wire_bytes = (json.dumps(data) + "\n").encode("utf-8")
    if VERBOSE and me:
        log(me, f"Before marshalling: {data}")
        log(me, f"After marshalling: {wire_bytes!r}")
    return wire_bytes


def rpc(addr, message, timeout=TIMEOUT, me=None):
    """One request, one reply, newline-delimited JSON (same framing as the order service)."""
    with socket.create_connection((addr["host"], addr["port"]), timeout=timeout) as sock:
        sock.sendall(marshal(message, me))
        with sock.makefile("rb") as stream:
            line = stream.readline(MAX_MESSAGE_BYTES + 1)
    if not line:
        raise ConnectionError("peer closed connection without reply")
    reply = json.loads(line.decode("utf-8"))
    if VERBOSE and me:
        log(me, f"After unmarshalling: {reply}")
    return reply


class Peer:
    def __init__(self, port):
        self.me = {"host": HOST, "port": port, "id": hash_id(f"{HOST}:{port}")}
        self.lock = threading.RLock()
        self.pred = None
        self.succ_list = [self.me]
        self.fingers = [None] * M
        self.store = {}          # key id -> {"key", "name", "value"}
        self.running = True

    def say(self, message):
        log(self.me, message)

    # ------------------------------------------------------------ routing
    def forget(self, node):
        """Drop a peer we found unreachable from every routing structure."""
        with self.lock:
            self.fingers = [None if f and f["id"] == node["id"] else f for f in self.fingers]
            self.succ_list = [s for s in self.succ_list if s["id"] != node["id"]] or [self.me]
            if self.pred and self.pred["id"] == node["id"]:
                self.pred = None
        self.say(f"Peer {node['port']} unreachable; removed from routing table")

    def find_successor(self, target, path, quiet=False):
        """Recursive Chord routing. Returns the successor list of the last hop,
        whose first entry owns `target`."""
        path = path + [{"port": self.me["port"], "id": self.me["id"]}]
        if len(path) > 3 * M:
            return {"ok": False, "error": "routing loop"}
        with self.lock:
            succs = list(self.succ_list)
            fingers = list(self.fingers)
        succ = succs[0]
        if between(target, self.me["id"], succ["id"]):
            if not quiet:
                self.say(f"Key {target} in ({self.me['id']}, {succ['id']}] -> "
                         f"owner is peer {succ['port']}")
            return {"ok": True, "cands": succs, "path": path}
        nxt = succ
        for finger in reversed(fingers):
            if finger and between_open(finger["id"], self.me["id"], target):
                nxt = finger
                break
        if not quiet:
            self.say(f"Key {target}: forwarding lookup to peer {nxt['port']} (id {nxt['id']})")
        hops = [nxt] if nxt["id"] == succ["id"] else [nxt, succ]
        for hop in hops:
            try:
                return rpc(hop, {"op": "FIND_SUCCESSOR", "target": target, "path": path,
                                 "quiet": quiet}, timeout=ROUTE_TIMEOUT, me=self.me if not quiet else None)
            except (OSError, ValueError):
                self.forget(hop)
        return {"ok": False, "error": "no live route"}

    # ----------------------------------------------------------- resources
    def put_local(self, key, name, value):
        with self.lock:
            self.store[key] = {"key": key, "name": name, "value": value}

    def push_to_replicas(self, items):
        with self.lock:
            targets = [s for s in self.succ_list if s["id"] != self.me["id"]]
        for target in targets:
            try:
                rpc(target, {"op": "BULK_PUT", "items": items})
            except (OSError, ValueError):
                pass  # stabilization will find and drop a dead successor

    def publish(self, name, value):
        key = hash_id(name.lower())
        self.say(f"PUBLISH '{name}' -> key {key}")
        found = self.find_successor(key, [])
        if not found.get("ok"):
            return {"ok": False, "error": found.get("error")}
        owner = found["cands"][0]
        rpc(owner, {"op": "PUT", "key": key, "name": name, "value": value, "primary": True},
            me=self.me)
        return {"ok": True, "key": key, "owner": owner["port"], "path": found["path"]}

    def lookup(self, name):
        key = hash_id(name.lower())
        self.say(f"LOOKUP '{name}' -> key {key}")
        found = self.find_successor(key, [])
        if not found.get("ok"):
            return {"ok": False, "error": found.get("error")}
        for index, candidate in enumerate(found["cands"]):
            try:
                reply = rpc(candidate, {"op": "GET", "key": key}, me=self.me)
            except (OSError, ValueError):
                self.say(f"Owner/replica {candidate['port']} unreachable; trying next replica")
                continue
            if reply.get("found"):
                role = "replica" if index > 0 else "owner"
                self.say(f"Resource '{name}' found on peer {candidate['port']} ({role})")
                return {"ok": True, "found": True, "value": reply["value"],
                        "path": found["path"], "served_by": candidate["port"],
                        "from_replica": index > 0}
        self.say(f"Resource '{name}' not found")
        return {"ok": True, "found": False, "path": found["path"]}

    # --------------------------------------------------------- join / leave
    def join(self, bootstrap_port):
        bootstrap = {"host": HOST, "port": bootstrap_port}
        self.say(f"Joining ring through bootstrap peer {bootstrap_port}")
        reply = rpc(bootstrap, {"op": "FIND_SUCCESSOR", "target": self.me["id"], "path": []},
                    timeout=ROUTE_TIMEOUT, me=self.me)
        if not reply.get("ok"):
            raise ConnectionError(reply.get("error", "join failed"))
        succ = reply["cands"][0]
        with self.lock:
            self.succ_list = [succ]
            self.pred = None
        self.say(f"Successor is peer {succ['port']} (id {succ['id']})")
        # Pull the keys this peer now owns: (succ.pred, my id].
        state = rpc(succ, {"op": "GET_STATE"})
        low = state["pred"]["id"] if state.get("pred") else self.me["id"]
        moved = rpc(succ, {"op": "TRANSFER_KEYS", "low": low, "high": self.me["id"]}, me=self.me)
        for item in moved["items"]:
            self.put_local(item["key"], item["name"], item["value"])
        self.say(f"Received {len(moved['items'])} resource(s) from peer {succ['port']}")
        rpc(succ, {"op": "NOTIFY", "node": self.me})

    def leave(self):
        self.say("Graceful LEAVE: handing resources to successor and unlinking")
        with self.lock:
            succ = next((s for s in self.succ_list if s["id"] != self.me["id"]), None)
            pred = self.pred
            items = list(self.store.values())
        if succ:
            try:
                rpc(succ, {"op": "BULK_PUT", "items": items}, me=self.me)
                rpc(succ, {"op": "SET_PRED", "node": pred}, me=self.me)
                self.say(f"Transferred {len(items)} resource(s) to peer {succ['port']}")
                if pred:
                    rpc(pred, {"op": "SET_SUCC", "node": succ}, me=self.me)
                    self.say(f"Told predecessor {pred['port']} its new successor is {succ['port']}")
            except (OSError, ValueError) as error:
                self.say(f"Leave handoff incomplete ({error}); neighbours will repair via stabilize")
        self.running = False

    # ---------------------------------------------------------- maintenance
    def stabilize(self):
        with self.lock:
            succs = list(self.succ_list)
        succ, state = None, None
        for candidate in succs:
            if candidate["id"] == self.me["id"]:
                succ = candidate
                break
            try:
                state = rpc(candidate, {"op": "GET_STATE"})
                succ = candidate
                break
            except (OSError, ValueError):
                self.forget(candidate)
        if succ is None:
            succ = self.me
        x = state["pred"] if state else self.pred
        if x and x["id"] != self.me["id"] and between_open(x["id"], self.me["id"], succ["id"]):
            try:
                state = rpc(x, {"op": "GET_STATE"})
                succ = x
            except (OSError, ValueError):
                pass
        with self.lock:
            old = self.succ_list[0]
            if succ["id"] == self.me["id"]:
                self.succ_list = [self.me]
            else:
                extra = [n for n in (state or {}).get("succ_list", [])
                         if n["id"] not in (self.me["id"], succ["id"])]
                self.succ_list = [succ] + extra[:SUCC_LIST_SIZE - 1]
        if old["id"] != succ["id"]:
            self.say(f"Successor changed: {old['port']} -> {succ['port']}")
        if succ["id"] != self.me["id"]:
            try:
                rpc(succ, {"op": "NOTIFY", "node": self.me})
            except (OSError, ValueError):
                pass

    def check_predecessor(self):
        with self.lock:
            pred = self.pred
        if pred:
            try:
                rpc(pred, {"op": "PING"})
            except (OSError, ValueError):
                self.say(f"Predecessor {pred['port']} failed; I now own its key range "
                         f"(served from replicas)")
                with self.lock:
                    if self.pred and self.pred["id"] == pred["id"]:
                        self.pred = None

    def fix_fingers(self):
        for i in range(M):
            start = (self.me["id"] + (1 << i)) % RING
            result = self.find_successor(start, [], quiet=True)
            if result.get("ok"):
                with self.lock:
                    self.fingers[i] = result["cands"][0]

    def replicate_primary(self):
        with self.lock:
            pred = self.pred
            items = [it for it in self.store.values()
                     if pred is None or between(it["key"], pred["id"], self.me["id"])]
        if items:
            self.push_to_replicas(items)

    def maintenance_loop(self):
        while self.running:
            try:
                self.stabilize()
                self.check_predecessor()
                self.fix_fingers()
                self.replicate_primary()
            except Exception as error:  # keep the peer alive on unexpected errors
                self.say(f"Maintenance error: {error}")
            time.sleep(MAINTENANCE_PERIOD)

    # -------------------------------------------------------------- server
    def handle(self, msg):
        op = msg.get("op")
        quiet = msg.get("quiet", False)
        if op == "PING":
            return {"ok": True}
        if op == "GET_STATE":
            with self.lock:
                return {"ok": True, "pred": self.pred, "succ_list": list(self.succ_list)}
        if op == "NOTIFY":
            node = msg["node"]
            with self.lock:
                if (self.pred is None or between_open(node["id"], self.pred["id"], self.me["id"])) \
                        and node["id"] != self.me["id"]:
                    if not self.pred or self.pred["id"] != node["id"]:
                        self.say(f"Predecessor is now peer {node['port']}")
                    self.pred = node
            return {"ok": True}
        if op == "FIND_SUCCESSOR":
            if not quiet:
                last = msg["path"][-1]["port"] if msg.get("path") else "client"
                self.say(f"Received FIND_SUCCESSOR(key {msg['target']}) from {last}")
            return self.find_successor(msg["target"], msg.get("path", []), quiet)
        if op == "PUT":
            self.put_local(msg["key"], msg["name"], msg["value"])
            self.say(f"Stored '{msg['name']}' (key {msg['key']})")
            if msg.get("primary"):
                self.push_to_replicas([self.store[msg["key"]]])
            return {"ok": True}
        if op == "GET":
            with self.lock:
                item = self.store.get(msg["key"])
            return {"ok": True, "found": item is not None, "value": item["value"] if item else None}
        if op == "BULK_PUT":
            for item in msg["items"]:
                self.put_local(item["key"], item["name"], item["value"])
            return {"ok": True}
        if op == "TRANSFER_KEYS":
            with self.lock:
                items = [it for it in self.store.values()
                         if between(it["key"], msg["low"], msg["high"])]
            self.say(f"Transferring {len(items)} resource(s) to a joining peer")
            return {"ok": True, "items": items}
        if op == "SET_PRED":
            with self.lock:
                node = msg["node"]
                self.pred = None if (node is None or node["id"] == self.me["id"]) else node
            self.say(f"Predecessor set to {self.pred['port'] if self.pred else None} (neighbour left)")
            return {"ok": True}
        if op == "SET_SUCC":
            with self.lock:
                node = msg["node"]
                self.succ_list = [self.me] if node["id"] == self.me["id"] else [node]
            self.say(f"Successor set to {node['port']} (neighbour left)")
            return {"ok": True}
        if op == "LOOKUP":
            return self.lookup(msg["name"])
        if op == "PUBLISH":
            return self.publish(msg["name"], msg["value"])
        if op == "STATUS":
            with self.lock:
                return {"ok": True, "me": self.me, "pred": self.pred,
                        "succ_list": list(self.succ_list),
                        "fingers": [f["port"] if f else None for f in self.fingers],
                        "keys": [{"key": k, "name": v["name"]} for k, v in self.store.items()]}
        if op == "LEAVE":
            self.leave()
            threading.Timer(0.5, lambda: os._exit(0)).start()
            return {"ok": True, "message": "peer leaving"}
        return {"ok": False, "error": "unknown op"}

    def serve_connection(self, conn):
        conn.settimeout(ROUTE_TIMEOUT + 2)
        try:
            with conn.makefile("rb") as stream:
                line = stream.readline(MAX_MESSAGE_BYTES + 1)
            if not line:
                return
            msg = json.loads(line.decode("utf-8"))
            reply = self.handle(msg)
            conn.sendall((json.dumps(reply) + "\n").encode("utf-8"))
        except (OSError, ValueError, KeyError, UnicodeError) as error:
            try:
                conn.sendall((json.dumps({"ok": False, "error": str(error)}) + "\n").encode())
            except OSError:
                pass
        finally:
            conn.close()

    def run(self, join_port=None):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((HOST, self.me["port"]))
            server.listen(50)
            server.settimeout(0.5)
            self.say(f"Peer started on {HOST}:{self.me['port']}")
            threading.Thread(target=self.maintenance_loop, daemon=True).start()
            if join_port:
                threading.Thread(target=self._join_later, args=(join_port,), daemon=True).start()
            else:
                self.say("First peer: created a new ring")
            while self.running:
                try:
                    conn, _ = server.accept()
                except socket.timeout:
                    continue
                threading.Thread(target=self.serve_connection, args=(conn,), daemon=True).start()

    def _join_later(self, join_port):
        try:
            self.join(join_port)
        except (OSError, ValueError, ConnectionError, KeyError) as error:
            self.say(f"Join failed: {error}")
            os._exit(1)


# ------------------------------------------------------------------ CLI
def client_call(port, message):
    return rpc({"host": HOST, "port": port}, message, timeout=ROUTE_TIMEOUT + 4)


def format_path(path):
    return " -> ".join(str(hop["port"]) for hop in path)


def main():
    global VERBOSE
    parser = argparse.ArgumentParser(description="Chord-style P2P overlay peer")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("start"); p.add_argument("--port", type=int, required=True)
    p.add_argument("--join", type=int); p.add_argument("--verbose", action="store_true")
    p = sub.add_parser("publish"); p.add_argument("--via", type=int, required=True)
    p.add_argument("name"); p.add_argument("value_json")
    p = sub.add_parser("lookup"); p.add_argument("--via", type=int, required=True); p.add_argument("name")
    p = sub.add_parser("status"); p.add_argument("--port", type=int, required=True)
    p = sub.add_parser("leave"); p.add_argument("--port", type=int, required=True)
    args = parser.parse_args()

    try:
        if args.cmd == "start":
            VERBOSE = args.verbose
            Peer(args.port).run(args.join)
        elif args.cmd == "publish":
            r = client_call(args.via, {"op": "PUBLISH", "name": args.name,
                                       "value": json.loads(args.value_json)})
            print(f"Published: {r}" if r.get("ok") else f"Failed: {r}")
        elif args.cmd == "lookup":
            r = client_call(args.via, {"op": "LOOKUP", "name": args.name})
            if r.get("ok") and r.get("found"):
                print(f"FOUND '{args.name}' on peer {r['served_by']}"
                      f"{' (replica)' if r['from_replica'] else ''}")
                print(f"Route through overlay: {format_path(r['path'])} -> {r['served_by']}")
                print(f"Value: {r['value']}")
            else:
                print(f"NOT FOUND / error: {r}")
        elif args.cmd == "status":
            r = client_call(args.port, {"op": "STATUS"})
            print(json.dumps(r, indent=2))
        elif args.cmd == "leave":
            print(client_call(args.port, {"op": "LEAVE"}))
    except KeyboardInterrupt:
        return 0
    except (OSError, ValueError) as error:
        print(f"Error: {error}")
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
