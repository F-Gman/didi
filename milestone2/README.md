# DiDi Food Delivery — Milestone 2

This project demonstrates communication between a customer, a restaurant,
two drivers, and an order service. It uses Python sockets to show TCP
request-reply, JSON marshalling, publish-subscribe updates, and UDP multicast.
All five processes run on the same computer. Cooking and delivery are simulated
by pressing Enter in the terminal.

## Requirements

- Python 3.9 or newer.
- macOS or Linux terminals. The keyboard prompts use `select`, so this version
  does not support the Windows console.
- Five terminal windows opened in the project folder.

Only Python's standard library is used. No extra packages need to be installed.

## Files

| File | Purpose |
| --- | --- |
| `order_service.py` | Handles requests, stores orders in memory, publishes updates, and sends multicast offers. |
| `customer.py` | Places an order and receives updates until delivery is confirmed. |
| `restaurant.py` | Receives the order and reports when the food is ready. |
| `driver.py` | Receives an order offer, claims the order, and reports delivery. Run this file twice with different driver names. |

## How to run

Run each command in a separate terminal, in this order:

```bash
# Terminal 1
python3 order_service.py

# Terminal 2
python3 restaurant.py

# Terminal 3
python3 driver.py An

# Terminal 4
python3 driver.py Alice
```

Wait until both drivers show `Joined driver group; waiting for orders`.
Then start the customer in the fifth terminal:

```bash
# Terminal 5
python3 customer.py
```

If your Python command is `python`, use it instead of `python3`.

## Demo steps

1. The customer places order 8801. The service confirms the order, publishes
   `ORDER_CREATED`, and sends a multicast offer to both drivers.
2. The restaurant displays the order and food items. Both drivers receive
   the offer.
3. Press Enter in An's terminal to claim the order. The service assigns An
   and publishes `ORDER_TAKEN`. Alice receives the update and exits.
4. Press Enter in the restaurant terminal when cooking is finished. The
   service publishes `ORDER_READY_FOR_PICKUP`.
5. An receives the food-ready update. After the simulated delivery, press
   Enter in An's terminal again.
6. The service confirms delivery and publishes `ORDER_DELIVERED`. The customer,
   restaurant, and An finish. The service stays running.

The order ID is fixed at 8801. To repeat the demo, stop the service with Ctrl+C,
restart it, and restart the other processes in the same order. Restarting the
service clears its stored orders. Submitting order 8801 again without restarting
is rejected as a duplicate.

## Communication

### TCP request-reply

The service listens on `127.0.0.1:6000`. A client opens a connection, sends one
command, receives a reply, and closes the connection. Commands include
`PLACE_ORDER`, `ACCEPT_ORDER`, `UPDATE_ORDER_STATUS`, and `COMPLETE_DELIVERY`.

For example, the customer sends the following request. The timestamp is an
example; the running program creates a new timestamp.

```json
{
  "action": "PLACE_ORDER",
  "order_id": 8801,
  "customer_name": "Jordan Smith",
  "restaurant": "DiDi Express Burger",
  "items": ["Bacon Cheese Burger", "Vanilla Milkshake, American Fries"],
  "timestamp": "2026-10-12T00:00:00.000"
}
```

The service replies with `ORDER_CONFIRMED` when the order is accepted.

### Marshalling

`marshal()` converts a Python dictionary to JSON using `json.dumps()`, adds a
newline, and encodes it as UTF-8 bytes. The receiver reads a complete message,
decodes the bytes, and uses `json.loads()` to restore the dictionary.

TCP is a byte stream, so the final newline marks the end of each message.
A buffered reader handles messages that arrive in pieces or together.
Messages are limited to 16,384 bytes.

The current settings are `SHOW_DETAILS = True` in the customer, driver, and
service files, and `False` in the restaurant file. Set it to `True` in a file
to show its marshalling and unmarshalling details. Set it to `False` for shorter
logs, then restart that program. A displayed `b'...'` indicates Python bytes.

### Publish-subscribe

The service also acts as the pub-sub broker. Each subscriber sends
`SUBSCRIBE_UPDATES` and receives a `SUBSCRIBED` acknowledgment. It keeps this
separate TCP connection open to receive events.

Events include `ORDER_CREATED`, `ORDER_TAKEN`, `ORDER_READY_FOR_PICKUP`, and
`ORDER_DELIVERED`. The service sends events on one shared channel, and clients
handle them according to the order ID and event type. The restaurant waits for
`ORDER_CREATED` before showing the cooking prompt.

### Group communication

The service sends one UDP order offer to multicast group `224.1.1.1:5007`.
Both drivers join the group with `IP_ADD_MEMBERSHIP`. The interface is set to
`127.0.0.1` for this local demo.

Both drivers must join before the customer places the order because offers
are not saved or replayed. Drivers claim the order through TCP. The service
uses a local thread lock to prevent the same order from being assigned twice.

## Failure demo

### Service unavailable

With the service stopped, run `customer.py`. It should report a connection
failure and exit instead of continuing the order.

### Service stops during an order

Start the normal demo and let An claim the order. Leave the restaurant waiting
to finish cooking, then press Ctrl+C in the service terminal.

The customer, restaurant, and An should detect the closed TCP connection and
exit without reporting successful delivery. Alice has already exited after
An claimed the order. The keyboard waits can end without another Enter press.

This demonstrates connection-loss detection. It does not demonstrate automatic
recovery or confirm that the order was cancelled.

### One subscriber stops

Start the normal demo. After both drivers receive the offer, stop Alice with
Ctrl+C before either driver claims it. Continue with An, the restaurant, and
the customer. They should still receive updates and finish the order.

## Limitations

- Orders are stored in memory and are lost when the service restarts.
- The service has no replica, automatic failover, or automatic reconnection.
- Events and multicast offers are not replayed for late subscribers.
- UDP multicast does not guarantee delivery and has no acknowledgment or retry.
- Request-reply operations use a three-second socket timeout. Idle subscriptions
  have no heartbeat, so a silent network failure may leave them waiting.
- Driver-name checks are not authentication. The assignment lock only protects
  threads inside the central service.
- If a reply is lost, the client may not know whether the service accepted its
  request. Automatic request retries are not implemented.
