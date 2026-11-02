"""Central order service: TCP request-reply, pub-sub, and UDP multicast."""
import socket
import threading
import json
from datetime import datetime

APP_NAME = "SERVICE"
TCP_HOST = '127.0.0.1'
TCP_PORT = 6000
MULTICAST_GROUP = '224.1.1.1'
MULTICAST_PORT = 5007
MULTICAST_INTERFACE = '127.0.0.1'  # Local-machine demo.
SHOW_DETAILS = True
SOCKET_TIMEOUT = 3.0
MAX_MESSAGE_BYTES = 16389

subscribers = []
subscribers_lock = threading.Lock()
orders = {}
orders_lock = threading.Lock()

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

def publish_event(event_type, order_id, payload):
    event = {"event_type": event_type, "order_id": order_id,
             "payload": payload, "published_at": get_timestamp()}
    wire_bytes = marshal(event)
    with subscribers_lock:
        log(f"Pub-sub: {event_type} -> {len(subscribers)} subscribers")
        for conn in subscribers[:]:
            try:
                conn.sendall(wire_bytes)
            except OSError as error:
                log(f"Subscriber disconnected: {error}")
                subscribers.remove(conn)
                try:
                    conn.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                conn.close()


def send_group_multicast(order):
    message = {"communication_type": "GROUP_MULTICAST", "target_group": "DRIVERS_ZONE_A",
               "order_id": order["order_id"], "restaurant": order["restaurant"],
               "destination": "1250 Bellflower Blvd, Long Beach, CA",
               "timestamp": get_timestamp()}
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP) as sock:
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF,
                        socket.inet_aton(MULTICAST_INTERFACE))
        sock.sendto(marshal(message), (MULTICAST_GROUP, MULTICAST_PORT))
    log(f"Group message: order #{order['order_id']} -> drivers")


def error_reply(message):
    return {"status": "ERROR", "message": message, "timestamp": get_timestamp()}


def process_order_action(msg):
    """Called while orders_lock is held. Return a reply and an optional event."""
    action = msg.get("action")
    order_id = msg.get("order_id")
    if type(order_id) is not int or order_id <= 0:
        return error_reply("A positive integer order_id is required"), None

    if action == "PLACE_ORDER":
        if order_id in orders:
            return error_reply(f"Order #{order_id} already exists"), None
        for field in ("customer_name", "restaurant"):
            if not isinstance(msg.get(field), str) or not msg[field].strip():
                return error_reply(f"{field} must be a non-empty string"), None
        items = msg.get("items")
        if not isinstance(items, list) or not items or any(
                not isinstance(item, str) or not item.strip() for item in items):
            return error_reply("items must be a non-empty list of food names"), None
        orders[order_id] = {"order_id": order_id, "customer_name": msg["customer_name"],
                            "restaurant": msg["restaurant"], "items": items,
                            "status": "ACCEPTED", "assigned_driver": None}
        log(f"Order #{order_id} received from {msg['customer_name']}")
        event = ("ORDER_CREATED", order_id,
                 {"customer_name": msg["customer_name"], "restaurant": msg["restaurant"],
                  "items": items[:], "status": "ACCEPTED"})
        return {"status": "ORDER_CONFIRMED", "order_id": order_id,
                "message": f"Order #{order_id} accepted", "timestamp": get_timestamp()}, event

    order = orders.get(order_id)
    if order is None:
        return error_reply(f"Order #{order_id} does not exist"), None
    driver_name = msg.get("driver_name")

    if action == "ACCEPT_ORDER":
        if not isinstance(driver_name, str) or not driver_name.strip():
            return error_reply("driver_name is required"), None
        if order["assigned_driver"] is not None or order["status"] == "DELIVERED":
            return {"status": "ASSIGNMENT_REJECTED", "order_id": order_id,
                    "message": f"Order #{order_id} was already taken"}, None
        order["assigned_driver"] = driver_name
        log(f"Order #{order_id} assigned to {driver_name}")
        reply = {"status": "ASSIGNMENT_SUCCESS", "order_id": order_id,
                 "order_status": order["status"], "message": f"Order #{order_id} assigned to you"}
        event = ("ORDER_TAKEN", order_id,
                 {"assigned_driver": driver_name, "status": order["status"]})
        return reply, event

    if action == "UPDATE_ORDER_STATUS":
        if msg.get("status") != "READY_FOR_PICKUP" or order["status"] != "ACCEPTED":
            return error_reply("Only an ACCEPTED order can become READY_FOR_PICKUP"), None
        order["status"] = "READY_FOR_PICKUP"
        log(f"Order #{order_id} ready for pickup")
        event = ("ORDER_READY_FOR_PICKUP", order_id,
                 {"status": order["status"], "restaurant": order["restaurant"],
                  "assigned_driver": order["assigned_driver"]})
        return {"status": "SUCCESS", "message": f"Order #{order_id} ready for pickup"}, event

    if action == "COMPLETE_DELIVERY":
        if not driver_name or driver_name != order["assigned_driver"]:
            return error_reply("Only the assigned driver can complete this order"), None
        if order["status"] != "READY_FOR_PICKUP":
            return error_reply("Food must be READY_FOR_PICKUP before delivery"), None
        order["status"] = "DELIVERED"
        log(f"Order #{order_id} delivered by {driver_name}")
        event = ("ORDER_DELIVERED", order_id,
                 {"status": "DELIVERED", "delivered_by": driver_name})
        return {"status": "SUCCESS", "message": f"Order #{order_id} delivered"}, event

    return error_reply("Unknown action"), None


def handle_client_connection(conn, addr):
    conn.settimeout(SOCKET_TIMEOUT)
    try:
        with conn.makefile("rb") as stream:
            msg = receive_message(stream)
        if msg.get("action") == "SUBSCRIBE_UPDATES":
            role = msg.get("role", "Anonymous")
            with subscribers_lock:
                # Send the ACK before events can be sent on this connection.
                conn.sendall(marshal({"status": "SUBSCRIBED", "message": "Subscription ready"}))
                subscribers.append(conn)
            log(f"Subscriber joined: {role}")
            while True:
                try:
                    if not conn.recv(1024):
                        break
                except socket.timeout:
                    continue  # An idle subscriber is allowed to stay connected.
            log(f"Subscriber left: {role}")
        else:
            # Serialize state transitions and their events for this small demo.
            with orders_lock:
                reply, event = process_order_action(msg)
                if event is not None:
                    publish_event(*event)
                conn.sendall(marshal(reply))
                if msg.get("action") == "PLACE_ORDER" and reply["status"] == "ORDER_CONFIRMED":
                    send_group_multicast(orders[msg["order_id"]])
    except ConnectionError:
        pass  # Client closed its socket before submitting a complete request.
    except (ValueError, UnicodeError) as error:
        log(f"Invalid message: {error}")
        try:
            conn.sendall(marshal(error_reply(str(error))))
        except OSError:
            log("Could not send the error reply; client disconnected.")
    except OSError as error:
        log(f"Connection error: {error}")
    finally:
        with subscribers_lock:
            if conn in subscribers:
                subscribers.remove(conn)
        conn.close()


def main():
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((TCP_HOST, TCP_PORT))
            server.listen(20)
            log(f"Listening on {TCP_HOST}:{TCP_PORT}; Ctrl+C to stop")
            while True:
                conn, addr = server.accept()
                threading.Thread(target=handle_client_connection, args=(conn, addr), daemon=True).start()
    except KeyboardInterrupt:
        log("Service stopped. In-memory orders are not saved.")
        return 0
    except OSError as error:
        log(f"Service could not start: {error}")
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
