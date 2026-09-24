import socket                   # Standard Python networking library used for TCP socket communication
import threading                # Threading library to support concurrent request handling
import json                     # Library for encoding and decoding structured JSON payloads
import random                   # Library for random operations (used to simulate driver selection)
from datetime import datetime   # Library to retrieve precise timestamps for event logging

HOST = '127.0.0.1'  # Loopback interface address for local inter-process execution
PORT = 5003         # Listening port dedicated to the Matching Service

# Mock pool of available couriers currently online in the system
AVAILABLE_DRIVERS = ["Courier_Alex (ID: D-101)", "Courier_Bailey (ID: D-204)", "Courier_Chris (ID: D-309)"]


def log(message):   # Function for printing to terminal's log.
    # Capture current local timestamp formatted down to millisecond precision
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    # Print formatted event trace prefixed with the component identifier
    print(f"\n[{timestamp}] [MATCHING_SERVICE] {message}\n")


def handle_dispatch_request(conn, addr):
    try:
        # Receive incoming byte stream (up to 4096 bytes) from the calling service
        data = conn.recv(4096).decode('utf-8')
        if not data:  # If no data is received (connection closed prematurely), terminate handler
            return

        # Deserialize received JSON string into a native Python dictionary
        payload = json.loads(data)
        order_id = payload.get("order_id")  # Extract the unique order identifier
        restaurant = payload.get("restaurant")  # Extract the merchant name/location
        log(f"\nReceived dispatch request for Order #{order_id} at '{restaurant}' from {addr}")

        # Simulate spatial matching logic by selecting an available driver at random
        assigned_driver = random.choice(AVAILABLE_DRIVERS)
        # Simulate estimated time of arrival (ETA) for pickup between 5 and 12 minutes
        estimated_pickup = random.randint(5, 12)
        log(f"\nAssigned {assigned_driver} to Order #{order_id}. ETA: {estimated_pickup} mins")

        # Construct the structured response payload containing dispatch results
        response = {
            "status": "MATCHED",
            "order_id": order_id,
            "assigned_driver": assigned_driver,
            "estimated_pickup_minutes": estimated_pickup
        }

        # Serialize response to JSON format and transmit back over the established TCP socket
        conn.sendall(json.dumps(response).encode('utf-8'))
        log(f"\nDispatched assignment response to Order Service")
    except Exception as e:
        # Catch and log any runtime exception encountered during request execution
        log(f"\nError handling request: {e}")
    finally:
        # Explicitly release socket resources by closing client connection descriptor
        conn.close()


def main():
    # Instantiate an IPv4 (AF_INET) stream-oriented TCP socket (SOCK_STREAM)
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Enable SO_REUSEADDR option to immediately reclaim the port upon process restart
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # Bind socket to the specified host IP and port number
    server.bind((HOST, PORT))
    # Enter listening mode with a maximum pending connection backlog of 10
    server.listen(10)
    log(f"\nMatching Service initialized and listening on {HOST}:{PORT}")

    while True:
        # Block until an incoming client connection request is accepted
        conn, addr = server.accept()
        # Spawn a non-blocking daemon thread to execute the request handler concurrently
        threading.Thread(target=handle_dispatch_request, args=(conn, addr), daemon=True).start()


if __name__ == '__main__':
    main()  # Entry point when executed directly via Python CLI