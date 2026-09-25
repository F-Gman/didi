# didi
The system must support distributed ride-request processing, driver discovery, passenger-driver matching, trip coordination, and location updates. It must demonstrate communication among distributed components, peer integration, coordination mechanisms, replication, transactions, and failure recovery.

## 1.Required Software
- Python 3.6 or newer.
- Three seperate terminal windows open at once

## 2.Installing Dependencies
- No installation needed. Currently the project only uses Python Standard Library

## 3.Starting Each Service
- Start the **Matching Service** and the **Order Service** *before* running `client.py`. If either service is not running, the order cannot be fully processed.
- Start the services in this order, each in its own terminal.

## 4. Running the Demo

**Terminal 3: Customer Client**
```bash
python3 client.py
```

The client sends a sample order (Order #33169) and prints the confirmation it receives. The driver and ETA are chosen at random, so your values may differ:

```json
{
    "order_id": 33169,
    "order_status": "PLACED_AND_DISPATCHED",
    "dispatch_info": {
        "status": "MATCHED",
        "order_id": 33169,
        "assigned_driver": "Courier_Bailey (ID: D-204)",
        "estimated_pickup_minutes": 8
    },
    "processed_at": "<ISO timestamp>"
}
```

The log output in Terminals 1 and 2 shows each message as it moves through the system.

## 5. Reproducing the Test Cases

### Test 1: Successful order (normal operation)
1. Start `matching_service.py`, then `order_service.py`.
2. Run `python3 client.py`.

**Expected results:**
- The client prints `"order_status": "PLACED_AND_DISPATCHED"` with `"status": "MATCHED"`.
- The Order Service logs show it received the order, forwarded it, and sent a confirmation.
- The Matching Service logs show it received the request, assigned a driver, and sent a response.

### Test 2: Matching Service unavailable
1. Stop `matching_service.py` (press `Ctrl+C`), or do not start it.
2. Make sure `order_service.py` is running.
3. Run `python3 client.py`.

**Expected results:**
- The Order Service logs `ERROR: Matching Service is unavailable.`
- The client still receives a response:
  ```json
  "order_status": "PENDING_DISPATCH",
  "dispatch_info": {
      "status": "FAILED",
      "reason": "Matching Service unreachable"
  }
  ```

### Test 3: Order Service unavailable
1. Stop `order_service.py`, or do not start it.
2. Run `python3 client.py`.

**Expected result:** The client logs:
```
ERROR: Could not connect to Order Service. Make sure it is running.
```

### Test 4: Multiple clients 
1. Start both services.
2. Run `python3 client.py` in several terminals at about the same time
   ```bash
   for i in 1 2 3; do python3 client.py & done; wait
   ```

**Expected result:** Every client receives a confirmation. The service logs show several connections being handled, one thread per connection. All clients send the same sample Order #33169.
