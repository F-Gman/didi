"""Receive actual UDP multicast offers, claim by TCP and listen to TCP events."""
import argparse
import socket
import threading
import time

from common import (MAX_MESSAGE_BYTES, MULTICAST_GROUP, MULTICAST_INTERFACE, MULTICAST_PORT,
                    Console, Subscription, configure, endpoint, log, rpc, reliable_rpc, fetch_orders, unmarshal,
                    OrderTracker, TERMINAL_ORDER_STATES, SUPPORT_MESSAGE)


def create_multicast_socket(port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, 'SO_REUSEPORT'):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind(('', port))

        membership = socket.inet_aton(MULTICAST_GROUP) + socket.inet_aton(MULTICAST_INTERFACE)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership)

        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
        sock.settimeout(0.2)
        return sock
    except OSError:
        sock.close()
        raise


def main():
    parser = argparse.ArgumentParser(description='DiDi driver')
    parser.add_argument('name', nargs='?', default='An')
    parser.add_argument('--service', default='127.0.0.1:6000')
    parser.add_argument('--multicast-port', type=int, default=MULTICAST_PORT)
    parser.add_argument('--auto', action='store_true', help='Confirm this driver actions automatically for tests')
    parser.add_argument('--once', action='store_true', help='Exit after one successful bilateral delivery')
    parser.add_argument('--details', action='store_true')
    args = parser.parse_args()
    configure('DRIVER ' + args.name, args.details)
    stopped = threading.Event()
    tracker = OrderTracker()
    claim_stops = {}
    subscription = multicast = None
    console = Console()

    def claim_stop(oid):
        return claim_stops.setdefault(oid, threading.Event())

    def observe(order):
        if not tracker.update(order):
            return
        oid = order['order_id']
        if order['status'] in TERMINAL_ORDER_STATES or (order.get('assigned_driver') and order['assigned_driver'] != args.name):
            claim_stop(oid).set()
        if order.get('assigned_driver') not in (None, args.name):
            log(f'Order {oid} taken by {order["assigned_driver"]}; waiting for other offers')
        elif order.get('assigned_driver') == args.name:
            log(f'Order {oid}: {order["status"]}')

    def on_event(event):
        observe(event.get('payload', {}))

    def disconnected(error):
        log(f'Service disconnected: {error}. Waiting to reconnect; delivery is not confirmed.')

    try:
        address = endpoint(args.service)
        multicast = create_multicast_socket(args.multicast_port)
        def synchronize():
            for order in fetch_orders(address, driver_name=args.name):
                observe(order)
            log('Driver state resynchronized from durable orders')
        subscription = Subscription(address, args.name, on_event, disconnected, on_connect=synchronize)
        subscription.start()
        log(f'Joined driver group {MULTICAST_GROUP}:{args.multicast_port}; waiting for orders')
        next_pull = time.monotonic() + 2
        while not stopped.is_set():
            try:
                wire, _ = multicast.recvfrom(MAX_MESSAGE_BYTES + 1)
                offer = unmarshal(wire)
                source = 'GROUP_MULTICAST'
            except socket.timeout:
                if time.monotonic() < next_pull:
                    continue
                next_pull = time.monotonic() + 2
                try:
                    available = rpc(address, {'action': 'GET_ORDERS', 'available_only': True}, timeout=2).get('orders', [])
                except OSError:
                    continue
                if not available:
                    continue
                offer = {'communication_type': 'GROUP_MULTICAST', 'target_group': 'DRIVERS_ZONE_A',
                         'order_id': available[0]['order_id'], 'restaurant': available[0]['restaurant']}
                source = 'TCP fallback'
            except (ValueError, UnicodeError) as error:
                log(f'Ignored invalid multicast offer: {error}')
                continue
            oid = offer.get('order_id')
            if (offer.get('communication_type') != 'GROUP_MULTICAST' or
                    offer.get('target_group') != 'DRIVERS_ZONE_A' or not isinstance(oid, str)):
                continue
            if claim_stop(oid).is_set():
                continue
            log(f'{source} offer {oid} from {offer.get("restaurant")}')
            if not args.auto:
                try:
                    line = console.read('Press Enter to claim this order', claim_stop(oid))
                except ConnectionError:
                    continue
                if line.strip():
                    log('Press Enter to claim; issue is available after assignment.')
                    continue
            reply = reliable_rpc(address, {'action': 'ACCEPT_ORDER', 'order_id': oid, 'driver_name': args.name}, stop=stopped)
            if reply.get('status') != 'ASSIGNMENT_SUCCESS':
                log(reply.get('message', 'Assignment rejected'))
                continue
            observe(reply.get('order'))
            log(f'Assigned order {oid}; waiting for kitchen')
            next_snapshot, shown = 0, None
            while not stopped.is_set():
                if time.monotonic() >= next_snapshot:
                    next_snapshot = time.monotonic() + 1
                    try:
                        observe(rpc(address, {'action': 'GET_ORDER', 'order_id': oid}, timeout=2).get('order'))
                    except OSError:
                        pass
                order = tracker.get(oid)
                if order is None:
                    continue
                status = order['status']
                if status == 'CANCELLED':
                    log(SUPPORT_MESSAGE)
                    log('Waiting for other orders')
                    break
                if status == 'DELIVERED':
                    log(f'Delivery confirmed for {oid}; customer receipt confirmed')
                    claim_stop(oid).set()
                    if args.once:
                        return 0
                    log('Waiting for other orders')
                    break
                prompts = {
                    'ACCEPTED': 'Waiting for kitchen. Type issue to cancel and contact service staff.',
                    'READY_FOR_PICKUP': 'Press Enter to confirm pickup, or type issue to cancel.',
                    'WAITING_PICKUP_CONFIRMATION': 'Waiting for restaurant confirmation. Type issue to cancel.',
                    'PICKED_UP': 'Press Enter after handing food to the customer, or type issue to cancel.',
                    'WAITING_DELIVERY_CONFIRMATION': 'Waiting for customer confirmation. Type issue to cancel.'}
                if status != shown:
                    log(prompts.get(status, status))
                    shown = status
                actionable = status in ('READY_FOR_PICKUP', 'PICKED_UP')
                line = '' if args.auto and actionable else console.poll()
                if line is None:
                    continue
                command = line.strip().lower()
                if command == 'issue':
                    message = {'action': 'REPORT_ISSUE', 'role': 'driver'}
                elif command == '' and status == 'READY_FOR_PICKUP':
                    message = {'action': 'CONFIRM_PICKUP', 'role': 'driver'}
                elif command == '' and status == 'PICKED_UP':
                    message = {'action': 'COMPLETE_DELIVERY'}
                else:
                    log('Waiting for the other participant. Type issue if there is a problem.')
                    continue
                reply = reliable_rpc(address, {**message, 'order_id': oid, 'driver_name': args.name}, stop=stopped)
                log(reply.get('message', reply.get('status')))
                observe(reply.get('order'))
                next_snapshot = 0
        return 0
    except KeyboardInterrupt:
        log('Driver stopped; this does not cancel or reassign its order.')
        return 0
    except (OSError, ValueError, UnicodeError, KeyError, TypeError, EOFError) as error:
        log(f'Driver failed: {error}')
        return 1
    finally:
        stopped.set()
        if subscription:
            subscription.close()
        if multicast:
            multicast.close()


if __name__ == '__main__':
    raise SystemExit(main())
