"""Receive multicast order offers, claim an order, and complete delivery."""
import socket
import json
import threading
import select
import sys
from datetime import datetime

APP_NAME = "DRIVER"

SERVER_HOST = '127.0.0.1'
SERVER_PORT = 6000
DEMO_ORDER_ID = 8801
SHOW_DETAILS = True  # Set True when recording the marshalling demonstration.
SOCKET_TIMEOUT = 3.0
MAX_MESSAGE_BYTES = 16389
subscription_socket = None

MULTICAST_GROUP = '224.1.1.1'
MULTICAST_PORT = 5007
MULTICAST_INTERFACE = '127.0.0.1'
subscription_ready = threading.Event()
driver_done_event = threading.Event()
connection_failed = threading.Event()
food_ready_event = threading.Event()

def get_timestamp():
    return datetime.now().isoformat(timespec="milliseconds")


def log(message):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [Driver {APP_NAME}] {message}", end="\n\n", flush=True)


def marshal(data):
    # TCP messages end with a newline so the receiver can find their boundaries.
    wire_bytes = (json.dumps(data) + "\n").encode("utf-8")
    if SHOW_DETAILS:
        log(f"Before marshalling: {data}")
        log(f"After marshalling: {wire_bytes!r}")
    return wire_bytes


def unmarshal(raw_bytes):
    data = json.loads(raw_bytes.decode("utf-8"))
    if not isinstance(data, dict):
        raise ValueError("Message must be a JSON object")
    if SHOW_DETAILS:
        log(f"After unmarshalling: {data}")
    return data


def receive_message(stream):
    # Keep the SAME stream for a persistent subscription; it buffers extra bytes.
    raw_bytes = stream.readline(MAX_MESSAGE_BYTES + 1)
    if not raw_bytes:
        raise ConnectionError("Order service disconnected")
    if len(raw_bytes) > MAX_MESSAGE_BYTES or not raw_bytes.endswith(b"\n"):
        raise ValueError("Message is too large or incomplete")
    return unmarshal(raw_bytes)

def send_request(message):
    with socket.create_connection((SERVER_HOST, SERVER_PORT), timeout=SOCKET_TIMEOUT) as sock:
        sock.sendall(marshal(message))
        with sock.makefile("rb") as stream:
            return receive_message(stream)


def close_subscription():
    if subscription_socket is not None:
        try:
            subscription_socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass  # A socket that is already disconnected needs no shutdown.
        subscription_socket.close()

def wait_for_enter(stop_event):
    # Unlike input(), this can stop when the background listener detects a failure.
    while not stop_event.is_set():
        readable, _, _ = select.select([sys.stdin], [], [], 0.2)
        if readable:
            if sys.stdin.readline() == "":
                log("Input closed; stopping this demo.")
                return False
            return not stop_event.is_set()
    return False

def listen_for_events(driver_name):
    global subscription_socket
    try:
        subscription_socket = socket.create_connection((SERVER_HOST, SERVER_PORT), timeout=SOCKET_TIMEOUT)
        with subscription_socket.makefile("rb") as stream:
            subscription_socket.sendall(marshal({"action": "SUBSCRIBE_UPDATES", "role": driver_name}))
            if receive_message(stream).get("status") != "SUBSCRIBED":
                raise ValueError("Subscription was rejected")
            subscription_socket.settimeout(None)
            log("Subscribed to order updates")
            subscription_ready.set()
            while not driver_done_event.is_set():
                event = receive_message(stream)
                if event.get("order_id") != DEMO_ORDER_ID:
                    continue
                payload = event.get("payload", {})
                if event.get("event_type") == "ORDER_TAKEN":
                    assigned_driver = payload.get("assigned_driver")
                    if assigned_driver != driver_name:
                        log(f"Order taken by {assigned_driver}; this demo driver is stopping")
                        driver_done_event.set()
                    elif payload.get("status") == "READY_FOR_PICKUP":
                        food_ready_event.set()
                elif event.get("event_type") == "ORDER_READY_FOR_PICKUP":
                    if payload.get("assigned_driver") == driver_name:
                        log("Pub-sub received: food is ready for pickup")
                        food_ready_event.set()
    except (OSError, ValueError, UnicodeError, KeyError) as error:
        if not driver_done_event.is_set():
            log(f"Connection lost: {error}. Delivery is not confirmed.")
            connection_failed.set()
            driver_done_event.set()
    finally:
        subscription_ready.set()


def create_multicast_socket():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind(('', MULTICAST_PORT))
        # Two IPv4 addresses form the 8-byte group-membership request.
        membership = socket.inet_aton(MULTICAST_GROUP) + socket.inet_aton(MULTICAST_INTERFACE)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership)
        sock.settimeout(0.2)  # Periodically check whether the app needs to stop.
        return sock
    except OSError:
        sock.close()
        raise


def main():
    global APP_NAME
    driver_name = sys.argv[1] if len(sys.argv) > 1 else "Driver_1"
    APP_NAME = driver_name
    multicast_sock = None
    try:
        multicast_sock = create_multicast_socket()
        threading.Thread(target=listen_for_events, args=(driver_name,), daemon=True).start()
        if not subscription_ready.wait(SOCKET_TIMEOUT + 1) or connection_failed.is_set():
            return 1
        log("Joined driver group; waiting for orders")
        while not driver_done_event.is_set():
            try:
                data, _ = multicast_sock.recvfrom(MAX_MESSAGE_BYTES + 1)
            except socket.timeout:
                continue
            if len(data) > MAX_MESSAGE_BYTES:
                log("Ignored an oversized group message")
                continue
            try:
                offer = unmarshal(data)
            except (ValueError, UnicodeError) as error:
                log(f"Ignored invalid group message: {error}")
                continue
            if offer.get("order_id") != DEMO_ORDER_ID:
                continue
            log(f"Group message received: order #{DEMO_ORDER_ID} at {offer.get('restaurant')}")
            log("Press Enter to claim this order")
            if not wait_for_enter(driver_done_event):
                break
            reply = send_request({"action": "ACCEPT_ORDER", "order_id": DEMO_ORDER_ID,
                                  "driver_name": driver_name, "timestamp": get_timestamp()})
            log("Reply: " + reply.get("message", "No message"))
            if reply.get("status") != "ASSIGNMENT_SUCCESS":
                return 1
            if reply.get("order_status") == "READY_FOR_PICKUP":
                food_ready_event.set()
            log("Waiting for the kitchen to finish cooking")
            while not food_ready_event.wait(0.2):
                if driver_done_event.is_set():
                    return 1 if connection_failed.is_set() else 0
            if driver_done_event.is_set():
                break
            log("Food is ready. Pick it up, then press Enter after delivery")
            if not wait_for_enter(driver_done_event):
                break
            reply = send_request({"action": "COMPLETE_DELIVERY", "order_id": DEMO_ORDER_ID,
                                  "driver_name": driver_name, "timestamp": get_timestamp()})
            log("Reply: " + reply.get("message", "No message"))
            return 0 if reply.get("status") == "SUCCESS" else 1
        return 1 if connection_failed.is_set() else 0
    except KeyboardInterrupt:
        log("Driver app stopped by the user")
        return 0
    except (OSError, ValueError, UnicodeError) as error:
        log(f"Communication failed: {error}. No automatic retry.")
        return 1
    finally:
        driver_done_event.set()
        close_subscription()
        if multicast_sock is not None:
            multicast_sock.close()
        log("Driver app closed")


if __name__ == '__main__':
    raise SystemExit(main())
