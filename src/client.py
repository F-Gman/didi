import socket                   # Standard socket module for initiating client network connections
import json                     # Module to format order objects into JSON wire format
from datetime import datetime   # Module to attach timestamps to execution logs

ORDER_SERVICE_HOST = '127.0.0.1'  # Target IP address of the Order Service
ORDER_SERVICE_PORT = 5001  # Target port of the Order Service


def log(message):
    # Capture local timestamp formatted to millisecond resolution
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    # Print formatted client message trace
    print(f"\n [{timestamp}] [CUSTOMER_CLIENT] {message}\n")


def send_order():
    # Construct mock customer order request payload
    order_request = {
        "order_id": 33169,
        "customer_name": "Jordan Smith",
        "restaurant": "DiDi Bento House - Long Beach",
        "items": ["Teriyaki Chicken Bowl", "Boba Milk Tea"],
        "total_amount": 18.75
    }

    try:
        # Instantiate a TCP client socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        log(f"\nConnecting to Order Service at {ORDER_SERVICE_HOST}:{ORDER_SERVICE_PORT}...")
        # Initiate connection to Order Service
        s.connect((ORDER_SERVICE_HOST, ORDER_SERVICE_PORT))

        log(f"\n Sending Order Placement Request for Order #{order_request['order_id']}")
        # Encode JSON string to UTF-8 bytes and transmit across the network
        s.sendall(json.dumps(order_request).encode('utf-8'))

        # Await and read incoming confirmation stream from the Order Service
        raw_response = s.recv(4096).decode('utf-8')
        # Deserialize JSON response string into native Python structure
        response_json = json.loads(raw_response)

        log("\n Received Confirmation Response from Order Service:")
        # Pretty-print formatted JSON response to standard output
        print(json.dumps(response_json, indent=4))

        # Terminate socket connection upon completing client transaction
        s.close()
    except ConnectionRefusedError:
        # Handle connection failure if the remote server process is down
        log("ERROR: Could not connect to Order Service. Make sure it is running.")
    except Exception as e:
        # Handle and display any other unexpected runtime exceptions
        log(f"Unexpected error: {e}")


if __name__ == '__main__':
    send_order()  # Trigger client order transaction on script execution