"""Automated P2P demo: start 4 peers, join a 5th, lookup, leave, crash, lookup again.

Run:  python demo_p2p.py            (timed steps)
      python demo_p2p.py --pause    (wait for Enter between steps; good for recording)
"""
import json
import subprocess
import sys
import time

PEER = [sys.executable, "peer.py"]
PAUSE = "--pause" in sys.argv
procs = {}

RESTAURANTS = {
    "DiDi Express Burger": {"host": "127.0.0.1", "port": 6000,
                            "menu": ["Bacon Cheese Burger", "Vanilla Milkshake", "American Fries"]},
    "Sakura Sushi": {"host": "127.0.0.1", "port": 6000, "menu": ["Salmon Roll", "Miso Soup"]},
    "Taco Loco": {"host": "127.0.0.1", "port": 6000, "menu": ["Carne Asada Taco", "Horchata"]},
    "Green Bowl Salads": {"host": "127.0.0.1", "port": 6000, "menu": ["Caesar Bowl"]},
}


def step(title):
    print(f"\n{'=' * 70}\n  {title}\n{'=' * 70}", flush=True)
    if PAUSE:
        input("  (press Enter to run this step) ")


def start_peer(port, join=None):
    cmd = PEER + ["start", "--port", str(port)] + (["--join", str(join)] if join else [])
    procs[port] = subprocess.Popen(cmd)
    time.sleep(1.0)


def cli(*args):
    subprocess.run(PEER + [str(a) for a in args])


def lookup(via, name):
    out = subprocess.run(PEER + ["lookup", "--via", str(via), name],
                         capture_output=True, text=True).stdout
    print(out, flush=True)
    for line in out.splitlines():
        if line.startswith("FOUND") and "on peer" in line:
            return int(line.split("on peer")[1].split()[0])
    return None


def main():
    try:
        step("1. Starting 4 peers (7001 creates the ring; 7002-7004 join it)")
        start_peer(7001)
        for port in (7002, 7003, 7004):
            start_peer(port, join=7001)
        time.sleep(6)  # let stabilization and finger tables converge

        step("2. Publishing restaurants into the DHT (via peer 7001)")
        for name, value in RESTAURANTS.items():
            cli("publish", "--via", 7001, name, json.dumps(value))
        time.sleep(2)

        step("3. Lookup routed through the overlay (entered at peer 7002)")
        owner = lookup(7002, "DiDi Express Burger")

        step("4. Peer 7005 JOINS the running ring")
        start_peer(7005, join=7002)
        time.sleep(6)
        cli("status", "--port", 7005)

        step("5. Lookup after the join (entered at the new peer 7005)")
        owner = lookup(7005, "Sakura Sushi") or owner

        step("6. Peer 7003 LEAVES gracefully")
        cli("leave", "--port", 7003)
        procs.pop(7003, None)
        time.sleep(5)

        step("7. Lookup after the graceful leave")
        owner = lookup(7001, "Taco Loco") or owner

        step("8. CRASH: kill the peer that owns 'DiDi Express Burger'")
        owner = lookup(7001, "DiDi Express Burger")
        victim = owner if owner in procs and owner != 7001 else 7004
        if victim == 7001:
            victim = 7002
        print(f"Killing peer {victim} without warning...", flush=True)
        procs[victim].kill()
        procs.pop(victim)
        entry = next(iter(procs))

        step(f"9. Lookup immediately after the crash (entered at peer {entry}) -> served by a replica")
        lookup(entry, "DiDi Express Burger")
        time.sleep(5)

        step("10. Lookup again after the ring has repaired itself")
        lookup(entry, "DiDi Express Burger")
        print("Demo complete.", flush=True)
    except KeyboardInterrupt:
        pass
    finally:
        for proc in procs.values():
            proc.kill()


if __name__ == '__main__':
    main()
