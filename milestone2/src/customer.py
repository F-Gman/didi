"""Place a food order and receive pub-sub status updates."""
import socket
import json
import threading
from datetime import datetime

APP_NAME = "CUSTOMER"

SERVER_HOST = '127.0.0.1'
SERVER_PORT = 6000
DEMO_ORDER_ID = 8801
SHOW_DETAILS = True  # Set True when recording the marshalling demonstration.
SOCKET_TIMEOUT = 3.0
MAX_MESSAGE_BYTES = 16389
subscription_socket = None

subscription_ready = threading.Event()
order_completed = threading.Event()
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

def listen_for_events():
    global subscription_socket
    try:
        subscription_socket = socket.create_connection((SERVER_HOST, SERVER_PORT), timeout=SOCKET_TIMEOUT)
        with subscription_socket.makefile("rb") as stream:
            subscription_socket.sendall(marshal({"action": "SUBSCRIBE_UPDATES", "role": "Customer"}))
            if receive_message(stream).get("status") != "SUBSCRIBED":
                raise ValueError("Subscription was rejected")
            subscription_socket.settimeout(None)  # Waiting for events is normally idle.
            log("Subscribed to order updates")
            subscription_ready.set()
            while not order_completed.is_set():
                event = receive_message(stream)
                if event.get("order_id") != DEMO_ORDER_ID:
                    continue
                event_type = event.get("event_type")
                if event_type == "ORDER_TAKEN":
                    log(f"Driver assigned: {event['payload']['assigned_driver']}")
                elif event_type == "ORDER_READY_FOR_PICKUP":
                    log("Your food is ready for pickup")
                elif event_type == "ORDER_DELIVERED":
                    log("Your food has arrived")
                    order_completed.set()
    except (OSError, ValueError, UnicodeError, KeyError) as error:
        if not order_completed.is_set():
            log(f"Connection lost: {error}. Order and Delivery was not confirmed.")
            connection_failed.set()
            order_completed.set()
    finally:
        subscription_ready.set()  # Also wake the main thread if subscription failed.


def place_order():
    order = {"action": "PLACE_ORDER", "order_id": DEMO_ORDER_ID,
             "customer_name": "Jordan Smith", "restaurant": "DiDi Express Burger",
             "items": ["Bacon Cheese Burger", "Vanilla Milkshake, American Fries"],
             "timestamp": get_timestamp()}
    log(f"Sending order #{DEMO_ORDER_ID}")
    log("Items: " + ", ".join(order["items"]))
    reply = send_request(order)
    log("Reply: " + reply.get("message", "No message"))
    return reply.get("status") == "ORDER_CONFIRMED"


def main():
    try:
        threading.Thread(target=listen_for_events, daemon=True).start()
        if not subscription_ready.wait(SOCKET_TIMEOUT + 1) or connection_failed.is_set():
            log("Subscription unavailable; order was not sent")
            return 1
        if not place_order():
            return 1
        while not order_completed.wait(0.2):
            continue
        if connection_failed.is_set():
            return 1
        log("Order complete. Customer app closed.")
        return 0
    except KeyboardInterrupt:
        log("Customer app stopped by the user")
        return 0
    except (OSError, ValueError, UnicodeError) as error:
        log(f"Request failed: {error}. Order outcome may be unknown; no automatic retry.")
        return 1
    finally:
        order_completed.set()
        close_subscription()


if __name__ == '__main__':
    raise SystemExit(main())
