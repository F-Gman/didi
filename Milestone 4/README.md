# DiDi Food Delivery — Milestone 4

This project continues our restaurant P2P ring from Milestone 3. Milestone 4 adds Lamport clocks, restaurant coordinator election, one shared Featured Restaurant slot, and order recovery after restarting the service.

## Requirements and files

Use **Windows with Python 3.10 or newer**. The project was tested with Python 3.12. All imports use the Python standard library: no pip packages or separate SQLite installation are needed.

This version runs on one computer. TCP uses localhost; driver offers use UDP multicast `224.1.1.1:5007` on the loopback interface with TTL 0. It is not configured for separate computers.

Keep these files together:

```text
DiDi_Milestone_4/
  order_service.py
  restaurant.py
  customer.py
  driver.py
  common.py
  coordination.py
  storage.py
  data/                  Restaurant JSON files
  README.md
  DiDi_Milestone_4_Report.docx
```

`runtime/` is created automatically for the SQLite database and process lock. `tests/` and `evidence/` are optional for running the app, but useful for checking the project and explaining results.

## Start the demo

Extract the ZIP first. Open PowerShell **inside the extracted folder**. Run each line below in a separate terminal, in this order. Wait for each restaurant to print `Restaurant peer ready` before opening the next one.

```powershell
python order_service.py
python restaurant.py Burger
python restaurant.py Pizza
python restaurant.py Sushi
python restaurant.py Chicken
python driver.py An
python driver.py Alice
python customer.py
```

Wait for the drivers to join before placing an order. Optional fifth restaurant:

```powershell
python restaurant.py Noodle
```

| Name | Peer ID | TCP port |
| --- | --- | --- |
| Burger | r1 | 7001 |
| Pizza | r2 | 7002 |
| Sushi | r3 | 7003 |
| Chicken | r4 | 7004 |
| Noodle | r5 | 7005 |

Names are case-insensitive. Burger is the bootstrap peer, so start it first. Each restaurant reads its own menu from `data/`; changing a display name alone does not generate a new menu.

## Customer menu and a complete order

The customer menu has three options:

1. **Place an order:** use the selected restaurant, or choose one if none is selected.
2. **Find a restaurant:** look up a menu through the ring and save that selection for this customer session.
3. **Exit.**

After option 2, option 1 uses the same restaurant and refreshes its live menu before ordering. Choose option 2 again to change restaurants. Restarting the customer clears the selection. Browsing alone does not create an order or a subscriber.

Enter items as `menu-number:quantity`, separated by commas, for example `1:2,3:1`.

Follow these six Enter presses, including claiming the order:

| Step | Terminal | What Enter confirms |
| --- | --- | --- |
| 1 | One driver | Claim the offered order |
| 2 | Selected restaurant | Cooking is finished |
| 3 | Assigned driver | I picked up the food |
| 4 | Selected restaurant | I handed the food to that driver |
| 5 | Assigned driver | I handed the food to the customer |
| 6 | Customer | I received the food |

The service requires both pickup confirmations before delivery. Delivery is successful only after the customer's confirmation. Then the customer closes; drivers and restaurants remain available for more orders.

```text
ACCEPTED -> READY_FOR_PICKUP -> WAITING_PICKUP_CONFIRMATION
         -> PICKED_UP -> WAITING_DELIVERY_CONFIRMATION -> DELIVERED
```

There is **no handoff confirmation timer**. If a participant has not confirmed, the order waits.

### If something goes wrong

The customer or assigned driver can type `issue` for their unfinished order, including while waiting. A restaurant can type `issue` when it has one unfinished order, or `issue ORDER_ID` to identify one.

The service saves `CANCELLED` and notifies the related participants to contact DiDi service staff. The customer exits, the driver waits for other orders, and the restaurant removes the order from actionable work. No extra Enter is needed to receive this cancellation. New confirmations cannot revive a cancelled order. Delivered orders are also final.

The staff message is a manual contact instruction; this project has no support chat or refund system.

## Restaurant commands

| Command | Meaning |
| --- | --- |
| `topology` | Show the restaurant ring |
| `leader` | Show the observed coordinator and feature permission |
| `feature` | Request the shared Featured Restaurant slot |
| `release` | Release the slot or cancel a pending request |
| `ready ORDER_ID` | Mark a selected order cooked |
| `pickup ORDER_ID` | Confirm restaurant pickup handoff |
| `issue [ORDER_ID]` | Report a problem with an unfinished order |
| Enter | Confirm cooking or pickup for the oldest actionable order shown |
| `leave` | Leave gracefully and close |
| `crash` | Stop immediately, without graceful cleanup |

## Demos to explain to the TA

### 1. P2P menu lookup

Start a customer, choose 2, and select Chicken. Observe the lookup path through the restaurant ring and the returned menu. Each restaurant is a peer that stores its own menu and forwards a lookup to its successor when needed. Replies return through the request chain. The ring is a simple educational design, not Chord or a distributed file system.

### 2. Lamport clocks

Look at `SEND ... ts=`, `RECEIVE ... remote_ts= ... local_ts=`, and matching request IDs. Each send increments the clock. A receive sets it to `max(local, received) + 1`. Local coordination records also increment it.

Background polling and heartbeats advance clocks even when their logs are hidden. Therefore a large clock value does not mean that many customers joined. The explicit event fields are more precise than the `[L=...]` prefix when threads interleave. Logical clocks express causal ordering, not elapsed seconds or a total order of all events.

### 3. Featured Restaurant mutual exclusion

1. Wait for election; Chicken/r4 normally wins with four healthy peers.
2. Type `feature` at Burger. Wait for its grant token.
3. Type `feature` at Sushi. It queues while Burger holds the slot.
4. Start a customer and choose Find a restaurant to see Burger's Featured card.
5. Type `release` at Burger. Sushi receives a new grant token.
6. Open Find a restaurant again to read the current Featured card.

There is one global slot, independent of cooking and order prices. Requests go through the elected coordinator. The service stores the queue and atomically validates grants. A queued restaurant polls again, so the leader and service can keep printing messages. This polling is intentional.

### 4. Coordinator crash

With Chicken/r4 as leader, type `crash` in its terminal. The remaining peers detect the failure and elect Sushi/r3. Compare `COORDINATOR elected`, the service's coordinator term, and the other peers' announcements.

A healthy replacement should settle instead of repeatedly starting elections. Delayed election messages from lower peers do not force a healthy leader to restart election. An unexpired Featured holder can remain holder through a leader change. Stale coordinator terms and holder tokens are rejected by the service.

### 5. Service crash and recovery

Place an order, then type `crash` in the service terminal. Keep the clients open. Run `python order_service.py` again in that terminal, using the same project folder and database.

Clients reconnect and reload committed order state. Finish the remaining handoff confirmations. This demonstrates **manual restart recovery**, not automatic service failover. A restarted service changes its boot generation and clears old coordinator/Featured permissions; live peers register and request again. Orders and saved request results remain in SQLite.

## How the parts fit together

- `common.py`: JSON message framing, TCP helpers, Lamport clocks and shared client state handling.
- `restaurant.py`: local menus, ring lookup, kitchen input and restaurant confirmations.
- `coordination.py`: election and Featured coordination between restaurants and service.
- `order_service.py`: order validation, subscriptions, driver offers and authoritative workflow changes.
- `storage.py`: SQLite transactions, durable state and duplicate-request handling.
- `driver.py` / `customer.py`: participant input, notifications and reconnect behavior.

This is a **hybrid system**. Menus use a restaurant P2P ring, while the service remains the durable authority for orders and Featured grants. Bully-style election chooses the restaurant coordinator; it does not replace the service database. FIFO means arrival order accepted by the service, not Lamport timestamp order.

## Checks and limitations

The saved verification log reports **49 tests passed** (`Ran 49 tests in 137.652s`, `OK`). It covers communication, clocks, election, mutual exclusion, recovery, handoff confirmation and cancellation. Some test fixtures use r0-based peer IDs; the normal demo uses r1 through r5.

If the `tests/` folder is included, run the suite with the demo stopped:

```powershell
python tests/run_tests.py
```

Tests use temporary state; the saved evidence is in `evidence/verification.txt` and per-test logs. The Word report discusses the design and selected recorded results.

Limits: this is a Windows single-computer demo; the service needs manual restart; there is no replicated database or automatic replacement of crashed customer/driver processes. Menus are local files. Participant fields enforce workflow checks but are not secure authentication. There is no payment processing, refund handling or proof that physical handoffs occurred.

## Share the project

Stop the apps before packaging. Send the seven Python files, the full `data/` folder, this README and the Word report together in one ZIP. Include `tests/` and `evidence/` if your friend or TA needs verification material. Leave out `__pycache__/` and `runtime/` for a fresh demo. Keep your original `runtime/` if you need its existing orders.

Your friend should extract the ZIP, install Python if needed, open PowerShell in the extracted folder, and follow the start commands above.
