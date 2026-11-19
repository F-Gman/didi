"""Restaurant cooking client AND ring peer. No separate peer process is needed."""
import argparse
import json
from pathlib import Path
import threading
import time
import re
import os
from coordination import Coordinator

from common import (HOST, Console, Subscription, configure, endpoint, envelope, log,
                    peer_address, response, rpc, reliable_rpc, fetch_orders, send_message, start_server, PROCESS_SESSION, crash_process, OrderTracker, TERMINAL_ORDER_STATES, SUPPORT_MESSAGE)


RESTAURANTS = {
    'burger': ('r1.json', 7001),
    'pizza': ('r2.json', 7002),
    'sushi': ('r3.json', 7003),
    'chicken': ('r4.json', 7004),
    'noodle': ('r5.json', 7005),
}


def load_restaurant(path):
    data = json.loads(Path(path).read_text(encoding='utf-8-sig'))
    for field in ('restaurant_id', 'name', 'address'):
        if not isinstance(data.get(field), str) or not data[field].strip():
            raise ValueError(f'{field} must be a non-empty string')
    if not re.fullmatch(r'r[0-9]+', data['restaurant_id']):
        raise ValueError('restaurant_id must have the form r1, r2, ...')
    if not isinstance(data.get('menu'), list) or not data['menu']:
        raise ValueError('Menu must be a non-empty list')
    seen = set()
    for item in data['menu']:
        if not isinstance(item, dict) or not isinstance(item.get('item_id'), str) or not item['item_id']:
            raise ValueError('Every menu item needs an item_id')
        if item['item_id'] in seen:
            raise ValueError('Menu item IDs must be unique')
        seen.add(item['item_id'])
        if not isinstance(item.get('name'), str) or not item['name'].strip():
            raise ValueError('Every menu item needs a name')
        if type(item.get('price_cents')) is not int or not 0 < item['price_cents'] <= 100000:
            raise ValueError('Price must be integer cents between 1 and 100000')
        if type(item.get('available')) is not bool:
            raise ValueError('available must be true or false')
    return data


class RestaurantPeer:
    def __init__(self, resource, port):
        self.resource = resource
        self.me = {'restaurant_id': resource['restaurant_id'], 'name': resource['name'],
                   'host': HOST, 'port': port, 'session': PROCESS_SESSION}
        self.coordinator = None
        self.members = [self.me]
        self.lock = threading.Lock()
        self.stopped = threading.Event()

    def snapshot(self):
        with self.lock:
            return [dict(member) for member in self.members]

    def set_members(self, members):
        if not isinstance(members, list) or not 1 <= len(members) <= 16:
            raise ValueError('Ring must contain 1 to 16 members')
        ids, addresses = set(), set()
        for member in members:
            address = peer_address(member)
            rid = member.get('restaurant_id')
            if not isinstance(rid, str) or not rid or rid in ids or address in addresses:
                raise ValueError('Peer IDs and addresses must be unique')
            ids.add(rid)
            addresses.add(address)
        if self.me not in members:
            raise ValueError('Membership snapshot must contain this peer')
        with self.lock:
            self.members = sorted(members, key=lambda m: m['restaurant_id'])
        log('Ring membership: ' + ' -> '.join(m['restaurant_id'] for m in self.snapshot()))

    def distribute(self, members):

        failed = []
        for member in members:
            try:
                reply = rpc(peer_address(member), {'action': 'SET_MEMBERS', 'members': members}, timeout=2)
                if reply.get('status') != 'OK':
                    failed.append(member['restaurant_id'])
            except (OSError, ValueError):
                failed.append(member['restaurant_id'])
        return failed

    def join(self, bootstrap):
        reply = rpc(bootstrap, {'action': 'TOPOLOGY'})
        if reply.get('status') != 'OK':
            raise ValueError('Bootstrap did not return a topology')
        members = reply['members']
        for member in members:
            if member['restaurant_id'] != self.me['restaurant_id'] and peer_address(member) != peer_address(self.me):
                continue

            try:
                proof = rpc(peer_address(member), {'action': 'PING'}, timeout=0.8)
            except OSError:
                proof = {}
            if proof.get('session') not in (None, self.me['session']):
                raise ValueError('Peer ID or endpoint belongs to another live process')
            if member['restaurant_id'] != self.me['restaurant_id']:
                raise ValueError('Endpoint belongs to a different peer ID')
        members = [m for m in members if m['restaurant_id'] != self.me['restaurant_id']]
        members = members + [self.me]
        self.set_members(members)
        failed = self.distribute(self.snapshot())
        if failed:
            # Best effort rollback: report failed update, do not claim atomic membership.
            self.distribute(reply['members'])
            raise ConnectionError(f'Join incomplete at {failed}; restart ring to restore consistent membership')
        log(f'JOIN completed for {self.me["restaurant_id"]}')

    def leave(self, request):
        members = [m for m in self.snapshot() if m != self.me]
        failed = self.distribute(members) if members else []
        if failed:
            return response(request, 'ERROR', message=f'Leave incomplete at {failed}; peer remains running')
        self.stopped.set()
        log(f'LEAVE completed for {self.me["restaurant_id"]}')
        return response(request, 'LEFT')

    def lookup(self, request):
        rid = request.get('restaurant_id')
        if not isinstance(rid, str) or not rid.strip():
            raise ValueError('restaurant_id must be a non-empty string')
        path = request.get('path', [])
        hops = request.get('hops_left', 16)
        unreachable = request.get('unreachable', [])
        if (not isinstance(path, list) or not all(isinstance(p, str) for p in path)
                or not isinstance(unreachable, list) or not all(isinstance(p, str) for p in unreachable)
                or type(hops) is not int or not 0 <= hops <= 16):
            raise ValueError('Invalid lookup path or hop budget')
        if self.me['restaurant_id'] in path:
            return response(request, 'NOT_FOUND', path=path, unreachable=unreachable)
        if hops == 0:
            return response(request, 'HOP_LIMIT', path=path, unreachable=unreachable)
        path = path + [self.me['restaurant_id']]
        log(f'LOOKUP {rid} request={request.get("request_id")} path={" -> ".join(path)}')
        if rid == self.me['restaurant_id']:
            return response(request, 'FOUND', resource=self.resource, peer=self.me,
                            path=path, unreachable=unreachable)
        if hops == 1:
            return response(request, 'HOP_LIMIT', path=path, unreachable=unreachable)
        members = self.snapshot()
        position = members.index(self.me)

        candidates = members[position + 1:] + members[:position]
        for member in candidates:
            target = member['restaurant_id']
            if target in path or target in unreachable:
                continue
            forwarded = envelope({'action': 'LOOKUP', 'restaurant_id': rid,
                                  'request_id': request.get('request_id'), 'path': path,
                                  'hops_left': hops - 1, 'unreachable': unreachable})
            try:
                reply = rpc(peer_address(member), forwarded, timeout=10)

                reply.pop('message_id', None)
                reply.pop('sender_id', None)
                reply.pop('timestamp', None)
                return envelope(reply)
            except (OSError, ValueError) as error:
                log(f'Peer {target} unreachable; trying next known successor: {error}')
                unreachable = unreachable + [target]
        return response(request, 'NOT_FOUND', path=path, unreachable=unreachable)

    def handle_connection(self, conn, stream, request):
        action = request.get('action')
        if action == 'PING':
            reply = response(request, 'OK', restaurant_id=self.me['restaurant_id'], session=self.me['session'])
        elif self.coordinator and action in ('ELECTION', 'COORDINATOR', 'FEATURE_REQUEST', 'FEATURE_STOP',
                                             'COORD_STATUS', 'LOCK_REQUEST', 'LOCK_RENEW', 'LOCK_RELEASE'):
            reply = response(request, **self.coordinator.handle(request))
        elif action == 'TOPOLOGY':
            reply = response(request, 'OK', members=self.snapshot())
        elif action == 'SET_MEMBERS':
            self.set_members(request.get('members'))
            reply = response(request, 'OK')
        elif action == 'LOOKUP':
            reply = self.lookup(request)
        elif action == 'LEAVE':
            reply = self.leave(request)
        else:
            reply = response(request, 'ERROR', message='Unknown peer action')
        send_message(conn, reply)


def main():
    parser = argparse.ArgumentParser(description='Restaurant app and P2P ring node')
    parser.add_argument('name', nargs='?', type=str.lower, choices=RESTAURANTS,
                        help='Restaurant name, for example Sushi')
    parser.add_argument('--config', help='Custom menu JSON file (advanced)')
    parser.add_argument('--port', type=int, help='Override the default port (advanced)')
    parser.add_argument('--join', help='Existing peer, for example 127.0.0.1:7001')
    parser.add_argument('--service', default='127.0.0.1:6000')
    parser.add_argument('--peer-only', action='store_true', help='Run overlay without order service')
    parser.add_argument('--auto-ready', action='store_true', help='Noninteractive cooking for tests')
    parser.add_argument('--details', action='store_true')
    args = parser.parse_args()
    if args.name:
        if args.config:
            parser.error('Choose a restaurant name OR --config, not both')
        filename, default_port = RESTAURANTS[args.name]

        args.config = Path(__file__).resolve().parent / 'data' / filename
        if args.port is None:
            args.port = default_port
        if args.name != 'burger' and args.join is None:
            args.join = '127.0.0.1:7001'
    elif not args.config or args.port is None:
        parser.error('Run: python restaurant.py Burger (or Pizza, Sushi, Chicken, Noodle)')
    server = subscription = None
    peer = None
    service_failed = threading.Event()
    tracker = OrderTracker()
    try:
        resource = load_restaurant(args.config)
        configure(resource['restaurant_id'], args.details)
        peer = RestaurantPeer(resource, args.port)
        server = start_server(peer, args.port)
        if args.join:
            peer.join(endpoint(args.join))
        service_address = endpoint(args.service)

        def observe(order):
            if not isinstance(order, dict) or order.get('restaurant_id') != resource['restaurant_id']:
                return
            previous = tracker.get(order['order_id'])
            if not tracker.update(order):
                return
            if previous is None and order['status'] == 'ACCEPTED':
                log(f'New order {order["order_id"]}: {order["items"]}; total={order["total_cents"]} cents')
            else:
                log(f'Order {order["order_id"]}: {order["status"]}')
            if order['status'] == 'CANCELLED':
                log(SUPPORT_MESSAGE)

        def on_event(event):
            observe(event.get('payload'))

        def on_disconnect(error):
            service_failed.set()
            log(f'Order service disconnected: {error}. Waiting to reconnect; orders are not cancelled.')

        def resynchronize(announce=True):
            for order in fetch_orders(service_address, restaurant_id=resource['restaurant_id']):
                observe(order)
            service_failed.clear()
            if announce:
                log('Restaurant state resynchronized from durable orders')

        if not args.peer_only:
            peer.coordinator = Coordinator(peer, service_address)
            peer.coordinator.start()
            subscription = Subscription(service_address, resource['restaurant_id'], on_event, on_disconnect,
                                        on_connect=resynchronize)
            subscription.start()
        log(f'Restaurant peer ready at {HOST}:{args.port}; PID={os.getpid()}')
        log('Commands: Enter for cooking/pickup handoff, ready ID, pickup ID, issue [ID], topology, leader, feature, release, leave, crash')
        console = Console()
        next_snapshot = 0
        shown = set()
        while not peer.stopped.is_set():
            if not args.peer_only and time.monotonic() >= next_snapshot:
                next_snapshot = time.monotonic() + 2
                try:
                    resynchronize(announce=False)
                except (OSError, ValueError):
                    service_failed.set()
            active = [o for o in tracker.all() if o['status'] not in TERMINAL_ORDER_STATES]
            actionable = [o for o in active if o['status'] in ('ACCEPTED', 'WAITING_PICKUP_CONFIRMATION')]
            for order in actionable:
                key = (order['order_id'], order['status'])
                if key not in shown:
                    if order['status'] == 'ACCEPTED':
                        log(f'Order {order["order_id"]}: Press Enter when cooking is finished, or type issue.')
                    else:
                        log(f'Order {order["order_id"]}: Confirm pickup handoff to {order["assigned_driver"]} with Enter, or type issue.')
                    shown.add(key)
            command = '' if args.auto_ready and actionable and not service_failed.is_set() else console.poll()
            if command is None:
                if console.eof:
                    peer.stopped.wait(0.15)
                continue
            parts = command.strip().split()
            verb = parts[0].lower() if parts else ''
            if verb == 'crash':
                crash_process()
            elif verb == 'leave':
                reply = peer.leave({})
                log(reply['status'] + ': ' + reply.get('message', ''))
            elif verb in ('feature', 'release', 'leader'):
                if not peer.coordinator:
                    log('Coordination needs the order service; omit --peer-only')
                    continue
                action = {'feature': 'FEATURE_REQUEST', 'release': 'FEATURE_STOP', 'leader': 'COORD_STATUS'}[verb]
                try:
                    log(str(peer.coordinator.handle({'action': action})))
                except (OSError, ValueError) as error:
                    log(f'Coordination request pending or failed: {error}')
            elif verb == 'topology':
                log('Ring: ' + ' -> '.join(m['restaurant_id'] for m in peer.snapshot()))
            elif verb in ('', 'ready', 'pickup', 'issue'):
                if len(parts) > 2:
                    log('Use ready ID, pickup ID, or issue ID.')
                    continue
                if len(parts) == 2:
                    order = next((o for o in active if o['order_id'] == parts[1]), None)
                elif verb == 'issue':
                    if len(active) != 1:
                        log('Use issue ORDER_ID. Active orders: ' + ', '.join(o['order_id'] for o in active))
                        continue
                    order = active[0]
                else:
                    order = actionable[0] if actionable else None
                if not order:
                    log('No matching pending order; waiting for the next handoff or new order.')
                    continue
                if verb == 'issue':
                    message = {'action': 'REPORT_ISSUE', 'role': 'restaurant'}
                elif order['status'] == 'ACCEPTED' and verb in ('', 'ready'):
                    message = {'action': 'UPDATE_ORDER_STATUS', 'status': 'READY_FOR_PICKUP'}
                elif order['status'] == 'WAITING_PICKUP_CONFIRMATION' and verb in ('', 'pickup'):
                    message = {'action': 'CONFIRM_PICKUP', 'role': 'restaurant'}
                else:
                    log('This order is waiting for another participant; use issue ID if needed.')
                    continue
                reply = reliable_rpc(service_address, {**message, 'order_id': order['order_id'],
                                     'restaurant_id': resource['restaurant_id']}, stop=peer.stopped)
                log(reply.get('message', reply.get('status')))
                observe(reply.get('order'))
                next_snapshot = 0
            else:
                log('Commands: Enter, ready ID, pickup ID, issue [ID], topology, leader, feature, release, leave, crash')
        return 0
    except KeyboardInterrupt:
        if peer:
            reply = peer.leave({})
            log(reply.get('message', 'Graceful leave requested'))
        return 0
    except (OSError, ValueError, KeyError, TypeError, EOFError) as error:
        log(f'Restaurant failed: {error}')
        return 1
    finally:
        if peer and peer.coordinator:
            peer.coordinator.stop.set()
        if subscription:
            subscription.close()
        if server:
            server.shutdown()
            server.server_close()


if __name__ == '__main__':
    raise SystemExit(main())
