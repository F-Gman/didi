"""Central order state, authoritative menu pricing, TCP pub-sub and UDP multicast."""
import argparse
import socket
import threading
import uuid
import json
import os
import time
from pathlib import Path
from storage import Store
from coordination import FEATURE_ACTIONS, feature_command

from common import (HOST, MULTICAST_GROUP, MULTICAST_INTERFACE, MULTICAST_PORT,
                    configure, envelope, log, marshal, peer_address, response, rpc,
                    send_message, start_server, Console, crash_process)


class OrderService:
    def __init__(self, multicast_port=MULTICAST_PORT, db_path=None, lease_seconds=8, leader_seconds=4):
        self.store = Store(db_path or Path(__file__).resolve().parent / 'runtime' / 'didi.sqlite')
        self.orders = {}
        self.orders_lock = self.store.lock
        self.lease_seconds, self.leader_seconds = lease_seconds, leader_seconds
        self.subscribers = []
        self.subscribers_lock = threading.Lock()
        self.multicast_port = multicast_port

    def publish(self, event_type, order, request):
        event = envelope({'event_type': event_type, 'order_id': order['order_id'],
                          'request_id': request.get('request_id'), 'payload': dict(order)})
        wire = marshal(event)
        with self.subscribers_lock:
            log(f'PUB-SUB {event_type} order={order["order_id"]} -> {len(self.subscribers)} subscribers')
            for conn in self.subscribers[:]:
                try:
                    conn.sendall(wire)
                except OSError:
                    self.subscribers.remove(conn)
                    try:
                        conn.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    conn.close()

    def multicast(self, order, request):
        message = {'communication_type': 'GROUP_MULTICAST', 'target_group': 'DRIVERS_ZONE_A',
                   'request_id': request.get('request_id'), 'order_id': order['order_id'],
                   'restaurant_id': order['restaurant_id'], 'restaurant': order['restaurant'],
                   'destination': order['destination']}
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP) as sock:

            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 0)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(MULTICAST_INTERFACE))
            sock.sendto(marshal(message), (MULTICAST_GROUP, self.multicast_port))
        log(f'MULTICAST order={order["order_id"]} -> {MULTICAST_GROUP}:{self.multicast_port}')

    def prepare_order(self, request):
        name = request.get('customer_name')
        rid = request.get('restaurant_id')
        destination = request.get('destination', '1250 Bellflower Blvd, Long Beach, CA')
        if not isinstance(name, str) or not name.strip() or len(name) > 100:
            raise ValueError('customer_name must contain 1 to 100 characters')
        customer_id = request.get('customer_id')
        if customer_id is not None and (not isinstance(customer_id, str) or not 1 <= len(customer_id) <= 100):
            raise ValueError('customer_id must contain 1 to 100 characters')
        if not isinstance(rid, str) or not rid.strip():
            raise ValueError('restaurant_id is required')
        if not isinstance(destination, str) or not destination.strip() or len(destination) > 300:
            raise ValueError('destination must contain 1 to 300 characters')
        requested = request.get('items')
        if not isinstance(requested, list) or not 1 <= len(requested) <= 30:
            raise ValueError('Order needs 1 to 30 item entries')

        catalog_reply = rpc(peer_address(request.get('peer')), {'action': 'LOOKUP', 'restaurant_id': rid}, timeout=12)
        if catalog_reply.get('status') != 'FOUND':
            raise ValueError('Restaurant is unavailable through the peer overlay')
        catalog = catalog_reply['resource']
        if catalog.get('restaurant_id') != rid:
            raise ValueError('Peer returned the wrong restaurant')
        menu = {item['item_id']: item for item in catalog['menu']}
        items, used = [], set()
        total = 0
        for entry in requested:
            if not isinstance(entry, dict):
                raise ValueError('Each order item must be an object')
            item_id, quantity = entry.get('item_id'), entry.get('quantity')
            if not isinstance(item_id, str) or item_id not in menu or item_id in used:
                raise ValueError('Unknown or duplicate menu item')
            if type(quantity) is not int or not 1 <= quantity <= 99:
                raise ValueError('Quantity must be an integer from 1 to 99')
            item = menu[item_id]
            price = item['price_cents']
            if item.get('available') is not True:
                raise ValueError(f'{item_id} is sold out')
            if type(price) is not int or not 0 < price <= 100000:
                raise ValueError('Peer returned an invalid price')
            used.add(item_id)
            total += price * quantity
            items.append({'item_id': item_id, 'name': item['name'], 'quantity': quantity,
                          'unit_price_cents': price, 'line_total_cents': price * quantity})
        return {'order_id': uuid.uuid4().hex[:12], 'restaurant_id': rid, 'restaurant': catalog['name'],
                'customer_name': name.strip(), 'destination': destination.strip(), 'items': items,
                'total_cents': total, 'status': 'ACCEPTED', 'assigned_driver': None,
                'customer_id': request.get('customer_id') or uuid.uuid4().hex, 'revision': 1,
                'pickup_driver_confirmed': False, 'pickup_restaurant_confirmed': False,
                'delivery_driver_confirmed': False, 'delivery_customer_confirmed': False}

    def process(self, request, prepared=None):
        action = request.get('action')
        if action == 'PLACE_ORDER':
            order = prepared

            while order['order_id'] in self.orders:
                order['order_id'] = uuid.uuid4().hex[:12]
            self.orders[order['order_id']] = order
            return response(request, 'ORDER_CONFIRMED', **order_reply(order)), 'ORDER_CREATED', order
        oid = request.get('order_id')
        if not isinstance(oid, str) or oid not in self.orders:
            raise ValueError('Unknown order_id')
        order = self.orders[oid]
        if order['status'] in ('DELIVERED', 'CANCELLED'):
            raise ValueError(f'Order is already {order["status"]}; no further changes are allowed')
        driver = request.get('driver_name')
        if action == 'ACCEPT_ORDER':
            if not isinstance(driver, str) or not driver.strip() or len(driver) > 100:
                raise ValueError('driver_name must contain 1 to 100 characters')
            if order['assigned_driver'] is not None or order['status'] == 'DELIVERED':
                return response(request, 'ASSIGNMENT_REJECTED', message='Order was already taken'), None, order
            order['assigned_driver'] = driver
            return response(request, 'ASSIGNMENT_SUCCESS', **order_reply(order)), 'ORDER_TAKEN', order
        if action == 'UPDATE_ORDER_STATUS':
            if request.get('restaurant_id') != order['restaurant_id']:
                raise ValueError('Only the matching restaurant may mark this order ready')
            if request.get('status') != 'READY_FOR_PICKUP' or order['status'] != 'ACCEPTED':
                raise ValueError('Only an ACCEPTED order can become READY_FOR_PICKUP')
            order['status'] = 'READY_FOR_PICKUP'
            return response(request, 'SUCCESS', **order_reply(order)), 'ORDER_READY_FOR_PICKUP', order
        if action == 'CONFIRM_PICKUP':
            role = request.get('role')
            if role == 'driver':
                if not driver or driver != order['assigned_driver']:
                    raise ValueError('Only the assigned driver can confirm pickup')
                if order['status'] != 'READY_FOR_PICKUP':
                    raise ValueError('Food must be READY_FOR_PICKUP before driver pickup confirmation')
                order['pickup_driver_confirmed'] = True
                order['status'] = 'WAITING_PICKUP_CONFIRMATION'
                return response(request, 'SUCCESS', **order_reply(order)), 'ORDER_PICKUP_PENDING', order
            if role == 'restaurant':
                if request.get('restaurant_id') != order['restaurant_id']:
                    raise ValueError('Only the matching restaurant can confirm handoff')
                if order['status'] != 'WAITING_PICKUP_CONFIRMATION' or not order['pickup_driver_confirmed']:
                    raise ValueError('Wait for the driver to confirm pickup first')
                order['pickup_restaurant_confirmed'] = True
                order['status'] = 'PICKED_UP'
                return response(request, 'SUCCESS', **order_reply(order)), 'ORDER_PICKED_UP', order
            raise ValueError('Pickup role must be driver or restaurant')
        if action == 'COMPLETE_DELIVERY':
            if not driver or driver != order['assigned_driver']:
                raise ValueError('Only the assigned driver can report delivery')
            if order['status'] != 'PICKED_UP':
                raise ValueError('Both driver and restaurant must confirm pickup before delivery')
            order['delivery_driver_confirmed'] = True
            order['status'] = 'WAITING_DELIVERY_CONFIRMATION'
            return response(request, 'SUCCESS', **order_reply(order)), 'ORDER_DELIVERY_PENDING', order
        if action == 'CONFIRM_DELIVERY':
            if not request.get('customer_id') or request['customer_id'] != order['customer_id']:
                raise ValueError('Only the customer of this order can confirm receipt')
            if order['status'] != 'WAITING_DELIVERY_CONFIRMATION' or not order['delivery_driver_confirmed']:
                raise ValueError('Wait for the driver to report delivery first')
            order['delivery_customer_confirmed'] = True
            order['status'] = 'DELIVERED'
            return response(request, 'SUCCESS', **order_reply(order)), 'ORDER_DELIVERED', order
        if action == 'REPORT_ISSUE':
            role = request.get('role')
            allowed = ((role == 'customer' and request.get('customer_id') == order['customer_id']) or
                       (role == 'restaurant' and request.get('restaurant_id') == order['restaurant_id']) or
                       (role == 'driver' and driver and driver == order['assigned_driver']))
            if not allowed:
                raise ValueError('Only a participant of this order can report an issue')
            order['status'] = 'CANCELLED'
            order['cancelled_by'] = role
            order['cancellation_reason'] = 'A handoff issue was reported. Please contact DiDi service staff for assistance.'
            reply = order_reply(order)
            reply['message'] = f'Order {oid} cancelled. Please contact DiDi service staff for assistance.'
            return response(request, 'SUCCESS', **reply), 'ORDER_CANCELLED', order
        raise ValueError('Unknown order action')

    def close(self):
        self.store.close()

    def execute(self, request):
        """Atomic state + reply cache. Reads use snapshots, not pub-sub replay."""
        action = request.get('action')
        event_type = order = None
        with self.store.lock:
            state = self.store.read()
            self.orders = state['orders']
            for order in self.orders.values():
                order.setdefault('revision', 0)
                order.setdefault('customer_id', 'legacy-' + order['order_id'])
                for flag in ('pickup_driver_confirmed', 'pickup_restaurant_confirmed',
                             'delivery_driver_confirmed', 'delivery_customer_confirmed'):
                    order.setdefault(flag, False)
            if action in FEATURE_ACTIONS:
                try:
                    reply = feature_command(state, request, self.lease_seconds, self.leader_seconds)
                except (ValueError, KeyError, TypeError) as error:
                    reply = {'status': 'ERROR', 'message': str(error)}
                self.store.save(state)
                return response(request, **reply), None, None
            if action == 'GET_ORDER':
                order = self.orders.get(request.get('order_id'))
                return response(request, 'OK' if order else 'ERROR', order=order), None, None
            if action == 'GET_ORDERS':
                orders = list(self.orders.values())
                if request.get('restaurant_id'):
                    orders = [o for o in orders if o['restaurant_id'] == request['restaurant_id']]
                if request.get('driver_name'):
                    orders = [o for o in orders if o['assigned_driver'] == request['driver_name']]
                if request.get('available_only'):
                    orders = [o for o in orders if o['assigned_driver'] is None and o['status'] not in ('DELIVERED', 'CANCELLED')]
                if request.get('active_only'):
                    orders = [o for o in orders if o['status'] not in ('DELIVERED', 'CANCELLED')]
                after = request.get('after', '')
                if not isinstance(after, str):
                    raise ValueError('after must be an order ID string')
                orders = sorted((o for o in orders if o['order_id'] > after), key=lambda o: o['order_id'])
                page, size = [], 0
                for item in orders:
                    item_size = len(json.dumps(item, ensure_ascii=True).encode('utf-8'))
                    if page and (len(page) >= 20 or size + item_size > 40000):
                        break
                    page.append(item)
                    size += item_size
                cursor = page[-1]['order_id'] if page and len(page) < len(orders) else None
                return response(request, 'OK', orders=page, next_cursor=cursor), None, None
            key = request.get('request_id')
            if not isinstance(key, str) or not 1 <= len(key) <= 100:
                return response(request, 'ERROR', message='A stable request_id is required'), None, None
            body = {k: v for k, v in request.items() if k not in
                    ('request_id', 'message_id', 'sender_id', 'timestamp', 'lamport')}
            fingerprint = json.dumps(body, sort_keys=True, allow_nan=False)
            cached = state['requests'].get(key)
            if cached:
                if cached['fingerprint'] != fingerprint:
                    return response(request, 'ERROR', message='request_id was already used with a different command'), None, None
                log(f'REPLAY request={key}: returning committed result, no second state change')
                return response(request, **cached['reply']), None, None
            try:
                prepared = self.prepare_order(request) if action == 'PLACE_ORDER' else None
                reply, event_type, order = self.process(request, prepared)
                if event_type and event_type != 'ORDER_CREATED':
                    order['revision'] += 1
                    reply['order'] = dict(order)
            except (ValueError, KeyError, TypeError, OSError) as error:
                # Unavailable menu is retryable; do not cache a transient lookup failure.
                return response(request, 'ERROR', message=str(error)), None, None
            plain = {k: v for k, v in reply.items() if k not in
                     ('message_id', 'request_id', 'sender_id', 'timestamp', 'lamport')}
            state['requests'][key] = {'fingerprint': fingerprint, 'reply': plain}

            if order:
                if len(json.dumps(order).encode('utf-8')) > 25000:
                    raise ValueError('Order is too large')
            self.store.save(state)
            return reply, event_type, order

    def handle_connection(self, conn, stream, request):
        action = request.get('action')
        if action == 'PING':
            send_message(conn, response(request, 'OK'))
            return
        if action == 'SUBSCRIBE_UPDATES':
            conn.settimeout(2)
            with self.subscribers_lock:
                send_message(conn, response(request, 'SUBSCRIBED'))
                self.subscribers.append(conn)
            log(f'Subscriber joined: {request.get("role", "anonymous")}')
            try:
                while True:
                    try:
                        if not conn.recv(1024):
                            break
                    except socket.timeout:
                        continue
            finally:
                with self.subscribers_lock:
                    if conn in self.subscribers:
                        self.subscribers.remove(conn)
            return
        if action == 'REGISTER_COORDINATOR':
            candidate = request.get('candidate', {})
            address = peer_address(candidate)
            proof = rpc(address, {'action': 'PING'}, timeout=1)
            if proof.get('session') != candidate.get('session'):
                raise ValueError('Coordinator process session cannot be verified')
            for member in request.get('members', []):
                if int(member['restaurant_id'][1:]) <= int(candidate['restaurant_id'][1:]):
                    continue
                try:
                    higher = rpc(peer_address(member), {'action': 'PING'}, timeout=0.5)
                except OSError:
                    continue
                if higher.get('status') == 'OK':
                    raise ValueError('A higher-ID peer is alive; election must continue')
        # Publish serialized with command commits to preserve service event order.
        with self.orders_lock:
            reply, event_type, order = self.execute(request)
            if event_type:
                self.publish(event_type, order, request)
            if event_type == 'ORDER_CREATED':
                try:
                    self.multicast(order, request)
                    reply['offer_sent'] = True
                except OSError as error:
                    reply['offer_sent'] = False
                    log(f'Order committed but multicast failed: {error}; drivers can pull available orders')
            send_message(conn, reply)


def order_reply(order):
    return {'order_id': order['order_id'], 'order_status': order['status'],
            'total_cents': order['total_cents'], 'items': order['items'],
            'message': f'Order {order["order_id"]}: {order["status"]}',
            'customer_id': order['customer_id'], 'order': dict(order)}


def main():
    parser = argparse.ArgumentParser(description='DiDi central order service')
    parser.add_argument('--port', type=int, default=6000)
    parser.add_argument('--multicast-port', type=int, default=MULTICAST_PORT)
    parser.add_argument('--details', action='store_true')
    parser.add_argument('--db', help='SQLite database path; default runtime/didi.sqlite')
    args = parser.parse_args()
    configure('SERVICE', args.details)
    server = service = None
    try:
        service = OrderService(args.multicast_port, db_path=args.db)
        server = start_server(service, args.port)
        log(f'Listening on {HOST}:{args.port}; PID={os.getpid()}; orders are stored in SQLite; restart reopens the same database. Ctrl+C to stop.')
        log('Type crash and Enter to simulate failure; Ctrl+C stops normally.')
        console = Console()
        while True:
            command = console.poll()
            if command is None:
                if console.eof:
                    time.sleep(0.15)
                continue
            if command.strip().lower() == 'crash':
                crash_process()
            else:
                log('Type crash to simulate failure, or Ctrl+C to stop normally.')
    except KeyboardInterrupt:
        log('Service stopped. Committed orders are saved; clients will reconnect after restart.')
        return 0
    except (OSError, ValueError) as error:
        log(f'Service failed: {error}')
        return 1
    finally:
        if server:
            server.shutdown()
            server.server_close()
        if service:
            service.close()


if __name__ == '__main__':
    raise SystemExit(main())
