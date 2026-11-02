"""Report that food is ready and receive pub-sub order updates."""
import socket
import json
import threading
import select
import sys
from datetime import datetime

APP_NAME = "RESTAURANT"

SERVER_HOST = '127.0.0.1'
SERVER_PORT = 6000
DEMO_ORDER_ID = 8801
SHOW_DETAILS = True  # Set True when recording the marshalling demonstration.
SOCKET_TIMEOUT = 3.0
MAX_MESSAGE_BYTES = 16389
subscription_socket = None

subscription_ready = threading.Event()
order_received_event = threading.Event()
order_delivered_event = threading.Event()
connection_failed = threading.Event()

def get_timestamp():
    return datetime.now().isoformat(timespec="milliseconds")


def log(message):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [{APP_NAME}] {message}", end="\n\n", flush=True)


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

def listen_for_events():
    global subscription_socket
    try:
        subscription_socket = socket.create_connection((SERVER_HOST, SERVER_PORT), timeout=SOCKET_TIMEOUT)
        with subscription_socket.makefile("rb") as stream:
            subscription_socket.sendall(marshal({"action": "SUBSCRIBE_UPDATES", "role": "Restaurant"}))
            if receive_message(stream).get("status") != "SUBSCRIBED":
                raise ValueError("Subscription was rejected")
            subscription_socket.settimeout(None)
            log("Subscribed to order updates")
            log("Waiting for new orders...")
            subscription_ready.set()
            while not order_delivered_event.is_set():
                event = receive_message(stream)
                if event.get("order_id") != DEMO_ORDER_ID:
                    continue
                if event.get("event_type") == "ORDER_CREATED":
                    log(f"New order #{DEMO_ORDER_ID} received")
                    log("Items: " + ", ".join(event["payload"]["items"]))
                    order_received_event.set()
                elif event.get("event_type") == "ORDER_TAKEN":
                    log(f"Driver assigned: {event['payload']['assigned_driver']}")
                elif event.get("event_type") == "ORDER_READY_FOR_PICKUP":
                    log(f"Pub-sub received: order #{DEMO_ORDER_ID} ready")
                elif event.get("event_type") == "ORDER_DELIVERED":
                    log(f"Order #{DEMO_ORDER_ID} delivered")
                    order_delivered_event.set()
    except (OSError, ValueError, UnicodeError, KeyError) as error:
        if not order_delivered_event.is_set():
            log(f"Connection lost: {error}. All Orders and Deliveries were cancelled.")
            connection_failed.set()
            order_delivered_event.set()
    finally:
        subscription_ready.set()


def notify_order_ready():
    message = {"action": "UPDATE_ORDER_STATUS", "order_id": DEMO_ORDER_ID,
               "status": "READY_FOR_PICKUP", "timestamp": get_timestamp()}
    log("Sending status: READY_FOR_PICKUP")
    reply = send_request(message)
    log("Reply: " + reply.get("message", "No message"))
    return reply.get("status") == "SUCCESS"


def main():
    try:
        threading.Thread(target=listen_for_events, daemon=True).start()
        if not subscription_ready.wait(SOCKET_TIMEOUT + 1) or connection_failed.is_set():
            return 1
        while not order_received_event.wait(0.2):
            if connection_failed.is_set():
                return 1
        if connection_failed.is_set():
            return 1
        log("Press Enter when cooking is finished")
        if not wait_for_enter(connection_failed):
            return 1
        if not notify_order_ready():
            return 1
        log("Waiting for delivery confirmation")
        while not order_delivered_event.wait(0.2):
            continue
        if connection_failed.is_set():
            return 1
        log("Restaurant app closed")
        return 0
    except KeyboardInterrupt:
        log("Restaurant app stopped by the user")
        return 0
    except (OSError, ValueError, UnicodeError) as error:
        log(f"Request failed: {error}. Update outcome may be unknown; no automatic retry.")
        return 1
    finally:
        order_delivered_event.set()
        close_subscription()


if __name__ == '__main__':
    raise SystemExit(main())
