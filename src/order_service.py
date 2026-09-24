import socket                   # Standard socket module for building network communication endpoints
import threading                # Multi-threading support to serve multiple clients simultaneously
import json                     # Utility library to handle data serialization into JSON
from datetime import datetime   # Module to generate high-resolution log timestamps

HOST = '127.0.0.1'  # Local host binding interface
PORT = 5001  # Inbound listening port for Customer Client connections
MATCHING_SERVICE_PORT = 5003  # Target port of the downstream Matching Service


def log(message):   # Function for printing to terminal's log.
    # Format current local timestamp with millisecond precision
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    # Output standardized log entry labeled with [ORDER_SERVICE]
    print(f"\n[{timestamp}] [ORDER_SERVICE] {message}\n")


def query_matching_service(order_payload):
    try:
        # Create a client-side TCP socket to invoke downstream RPC with Matching Service
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Establish a three-way TCP handshake with Matching Service
        s.connect((HOST, MATCHING_SERVICE_PORT))
        log(f"Forwarding Order #{order_payload['order_id']} to Matching Service on port {MATCHING_SERVICE_PORT}...")

        # Serialize and deliver order details across the inter-service socket channel
        s.sendall(json.dumps(order_payload).encode('utf-8'))

        # Block and wait for the response payload from the Matching Service
        raw_response = s.recv(4096).decode('utf-8')
        s.close()  # Close the connection upon successful transaction completion
        return json.loads(raw_response)  # Parse and return response as dictionary
    except ConnectionRefusedError:
        # Gracefully handle downstream failure when Matching Service is offline or unreachable
        log("\nERROR: Matching Service is unavailable.")
        return {"status": "FAILED", "reason": "Matching Service unreachable"}


def handle_client(conn, addr):
    # Log incoming connection from a client endpoint
    log(f"Accepted connection from Customer Client: {addr}")
    try:
        # Read incoming stream of bytes representing client order payload
        data = conn.recv(4096).decode('utf-8')
        if not data:
            return

        # Parse JSON string from client into Python dictionary representation
        order_payload = json.loads(data)
        log(f"\nReceived Order: #{order_payload.get('order_id')} for item(s): {order_payload.get('items')}")

        # Execute inter-process communication (IPC) to resolve driver matching
        matching_response = query_matching_service(order_payload)

        # Construct aggregate response object containing order status and courier info
        final_response = {
            "order_id": order_payload.get("order_id"),
            # Update state based on whether driver matching succeeded or failed
            "order_status": "PLACED_AND_DISPATCHED" if matching_response.get("status") == "MATCHED" else "PENDING_DISPATCH",
            "dispatch_info": matching_response,
            "processed_at": datetime.now().isoformat()
        }

        log(f"\nSending confirmation response back to Client: Order #{order_payload.get('order_id')}")
        # Transmit aggregated response back to the Customer Client over the open socket
        conn.sendall(json.dumps(final_response).encode('utf-8'))
    except Exception as e:
        # Capture and log unexpected processing errors
        log(f"\nError processing client request: {e}")
    finally:
        # Ensure the client connection socket is closed reliably
        conn.close()


def main():
    # Initialize an IPv4 TCP stream socket for the Order Service
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Configure socket to reuse local address and port to prevent binding conflicts
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # Bind server to assigned address and port
    server.bind((HOST, PORT))
    # Start listening with connection queue limit set to 10
    server.listen(10)
    log(f"\nOrder Service initialized and listening on {HOST}:{PORT}")

    while True:
        # Accept incoming client connections
        conn, addr = server.accept()
        # Delegate client processing to a separate thread to maintain server responsiveness
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()


if __name__ == '__main__':
    main()