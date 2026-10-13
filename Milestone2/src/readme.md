# DiDi - Milestone 2 Communication Demo

Four Python programs demonstrate a food-order workflow. The order service
also acts as the pub-sub broker and multicast sender; no separate broker file
is required. All programs use Python's standard library.

## Requirements

- Python 3.9 or newer.
- macOS or Linux terminals. The interruptible keyboard prompt uses `select`
  on standard input; this version does not support the Windows console.
- Run all processes on the same computer. TCP uses localhost port 6000;
  UDP multicast uses group 224.1.1.1, port 5007, on the loopback interface.
- Allow local socket connections when the operating system requests access.

There are no extra Python packages to install.

## Files and roles

| File | Role |
| --- | --- |
| `order_service.py` | Stores demo orders in memory, handles requests, publishes events, and multicasts order offers |
| `customer.py` | Places order 8801 and subscribes to status updates |
| `restaurant.py` | Subscribes to updates and reports when cooking is finished |
| `driver.py` | Joins the multicast group, subscribes to events, claims the order, and reports delivery |

`DEMO_ORDER_ID = 8801` is intentionally fixed for this single-order exercise.
The service rejects a duplicate order instead of overwriting its state.

## Start the demo

Open five terminal windows in this folder. If your computer uses `python`
instead of `python3`, replace the command name below.

1. Terminal 1: `python3 order_service.py`
2. Terminal 2: `python3 restaurant.py`
3. Terminal 3: `python3 driver.py Driver_A`
4. Terminal 4: `python3 driver.py Driver_B`
5. Wait until both drivers print `Joined driver group; waiting for orders`.
6. Terminal 5: `python3 customer.py`

Then complete the workflow:

1. The restaurant first displays `Waiting for new orders...`. When the customer
   places order 8801, the service publishes `ORDER_CREATED`. The restaurant
   displays the order and its food items, followed by `Press Enter when cooking
   is finished`. Both drivers also receive the multicast order offer.
2. Press Enter in Driver_A's terminal to claim the order.
3. Driver_B receives `ORDER_TAKEN` and exits this single-order demo.
4. Driver_A waits for the kitchen. It cannot report delivery yet.
5. Press Enter in the restaurant terminal when cooking is finished.
6. The restaurant sends a request; the service replies and publishes
   `ORDER_READY_FOR_PICKUP` to the connected subscribers.
7. Driver_A displays the food-ready prompt. After the simulated delivery,
   press Enter in Driver_A's terminal again.
8. The service verifies the driver and order state, replies, and publishes
   `ORDER_DELIVERED`. Customer, restaurant, and Driver_A finish.

The service remains running. To repeat a fresh demo, stop the service with
Ctrl+C, restart it, and restart the other four processes in the order above.
This clears the in-memory order 8801.

## Clean logs and detailed serialization

The default `SHOW_DETAILS = False` prints short action logs with local-time
timestamps. Full dates and timestamps also appear in message fields where
provided. For the serialization portion of your video, change the setting
to `True` in `order_service.py`, then restart the demo. You can also enable
it in a client file to show that client's marshalling.

Detailed output shows the Python dictionary before marshalling, UTF-8 JSON
bytes after marshalling, and the restored dictionary after unmarshalling.
Switch it back to `False` for the normal workflow demonstration.

## Message examples

### TCP request-reply

The customer sends a structured request:

```json
{"action":"PLACE_ORDER","order_id":8801,"customer_name":"Jordan Smith","restaurant":"DiDi Express Burger","items":["Bacon Cheese Burger","Vanilla Milkshake"],"timestamp":"2026-10-11T22:00:00.000"}
```

The service replies:

```json
{"status":"ORDER_CONFIRMED","order_id":8801,"message":"Order #8801 accepted","timestamp":"2026-10-11T22:00:00.001"}
```

### Pub-sub

Subscribers first send `SUBSCRIBE_UPDATES` and receive a `SUBSCRIBED` ACK.
They then keep that TCP connection open for pushed events. The first event is
`ORDER_CREATED`, with the customer name, restaurant, food items, and ACCEPTED
status. The restaurant waits for this event before offering the cooking prompt.
When a driver claims the order, the service publishes:

```json
{"event_type":"ORDER_TAKEN","order_id":8801,"payload":{"assigned_driver":"Driver_A","status":"ACCEPTED"},"published_at":"2026-10-11T22:00:01.000"}
```

All subscribers share one order-update channel in this small demo. Client
code filters events by the demo order ID. There is no topic-filtering system.

### Group communication

The service sends one UDP datagram to the multicast group:

```json
{"communication_type":"GROUP_MULTICAST","target_group":"DRIVERS_ZONE_A","order_id":8801,"restaurant":"DiDi Express Burger","destination":"1250 Bellflower Blvd, Long Beach, CA","timestamp":"2026-10-11T22:00:00.002"}
```

The two driver processes join the same group and receive the offer. Start
both drivers before placing the order because multicast offers are not saved.

## Serialization and TCP message boundaries

`marshal()` uses `json.dumps()` and UTF-8 encoding. `unmarshal()` uses UTF-8
decoding and `json.loads()`. Each TCP message ends with a newline. A persistent
buffered reader reads one complete line at a time, so partial packets and
multiple messages in one TCP read do not break the JSON protocol.

Messages are limited to 16384 bytes. JSON string newlines are escaped by
`json.dumps()`, so they do not accidentally create additional message boundaries.

## Reproduce failure checks

### Service unavailable at startup

Stop the service and launch `customer.py`. It should show a clear connection
failure and exit with an error instead of printing a traceback.

### Service disconnects during the demo

Start the normal demo and leave Driver_A waiting for the kitchen. Stop the
service with Ctrl+C. Customer, restaurant, and drivers should detect the closed
TCP subscription and exit with a connection-loss message. They must not claim
that the order was delivered. Restaurant and driver keyboard waits can stop
without the user having to press Enter.

This tests an observable TCP disconnect. It does not test a network blackhole
where the TCP connection stays open but packets never arrive.

### One subscriber disconnects

Start the demo with two drivers. Stop Driver_B before anyone claims the order.
Continue with Driver_A, restaurant, and customer. Remaining subscribers should
still receive the order events and complete their workflow.

### Restaurant reports an unknown order

Start only the service. In another terminal, simulate an invalid kitchen update:

```bash
python3 -c 'import restaurant; print(restaurant.send_request({"action":"UPDATE_ORDER_STATUS","order_id":8801,"status":"READY_FOR_PICKUP"}))'
```

The service should return an error saying the order does not exist. The normal
restaurant UI waits for a new-order event and does not offer this action early.

### Duplicate order

Start a normal demo. While the first customer waits, run `customer.py` again
in another terminal. The second submission should be rejected because order
8801 already exists. The original assignment and state must remain intact.

### Invalid delivery request

With order 8801 present, run this in a separate terminal to simulate an
unassigned driver trying to complete it:

```bash
python3 -c 'import driver; print(driver.send_request({"action":"COMPLETE_DELIVERY","order_id":8801,"driver_name":"NotAssigned"}))'
```

The response should contain `"status": "ERROR"`. A completion request from
the assigned driver before the food is ready is also rejected by the service.
Driver_A's normal UI already waits for the food-ready event before offering
the delivery prompt.

## What these mechanisms prove

- Three-second socket timeouts bound connection and request-reply waits.
- A disconnected event listener signals the main thread to stop waiting.
- Subscription ACKs confirm registration before the app proceeds.
- A lock prevents two local service threads from assigning the same order.
- Validation rejects unknown orders, duplicates, invalid transitions, and
  delivery reports whose driver name does not match the assignment.

Driver-name validation is a demo consistency check, not authentication.
The lock is local to one central service, not distributed mutual exclusion.

## Limits and remaining submission work

- The central service is a single point of failure. It has no replica or failover.
- Orders live in memory and are lost when the service restarts.
- There is no automatic reconnect, event replay, durable queue, or recovery of
  interrupted orders. Restarting the processes starts a fresh demo.
- There are no automatic request retries. If a reply is lost, a request may
  already have changed the order; its outcome can be unknown to the sender.
- Persistent subscriptions have no heartbeat. A silent network failure can
  leave a subscriber waiting; only an observable socket failure stops it.
- UDP multicast has no ACK, retransmission, or delivery guarantee.
- The kitchen-ready action and physical delivery are simulated by Enter presses.
- State changes and event sending are serialized for this low-volume exercise.
  A slow subscriber can delay other work up to its send timeout.
- Run input prompts in macOS/Linux terminals, not an IDE console without a
  selectable standard-input stream.
- The business scope still needs clarification: the syllabus title says food
  delivery but describes ride matching. This code follows the current food workflow.

The final submission still needs your PDF report, video, source repository,
screenshots/logs, and contribution table. Use actual runs as evidence and
distinguish disconnect handling from automatic recovery in the report.
