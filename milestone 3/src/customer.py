"""Discover a restaurant through peers, choose menu items, and track an order."""
import argparse
import threading
import time
import uuid

from common import Console, Subscription, configure, endpoint, log, money, rpc, reliable_rpc, peer_address, OrderTracker, SUPPORT_MESSAGE


def choose_items(text, menu, numbered=False):
    items = []
    for part in text.split(','):
        key, quantity_text = part.strip().rsplit(':', 1)
        quantity = int(quantity_text)
        if not 1 <= quantity <= 99:
            raise ValueError('Quantity must be between 1 and 99')
        if numbered:
            index = int(key) - 1
            if not 0 <= index < len(menu):
                raise ValueError('Menu number is out of range')
            key = menu[index]['item_id']
        item = next((m for m in menu if m['item_id'] == key), None)
        if item is None or item['available'] is not True:
            raise ValueError(f'Item {key} is unknown or sold out')
        if any(m['item_id'] == key for m in items):
            raise ValueError('Select each item once with its total quantity')
        items.append({'item_id': key, 'quantity': quantity})
    return items


def discover_restaurant(bootstrap, console, restaurant_id=None):
    topology = rpc(bootstrap, {'action': 'TOPOLOGY'})
    if topology.get('status') != 'OK':
        raise ValueError(topology.get('message', 'Cannot read topology'))
    members = topology['members']
    names = {p['restaurant_id']: p.get('name', p['restaurant_id']) for p in members}
    if restaurant_id:
        rid = restaurant_id
    else:
        log('Restaurants:\n' + '\n'.join(f'{i}. {names[p["restaurant_id"]]}' for i, p in enumerate(members, 1)))
        choice = console.read('Choose a restaurant number:')
        index = int(choice) - 1
        if not 0 <= index < len(members):
            raise ValueError('Restaurant number is out of range')
        rid = members[index]['restaurant_id']
    discovery = rpc(bootstrap, {'action': 'LOOKUP', 'restaurant_id': rid}, timeout=12)
    path = discovery.get('path', [])
    log('Lookup path: ' + ' -> '.join(names.get(rid, rid) for rid in path))
    log('Peer IDs: ' + ' -> '.join(path))
    if discovery.get('unreachable'):
        log(f'Unreachable peers bypassed: {discovery["unreachable"]}')
    if discovery.get('status') != 'FOUND':
        raise ValueError(f'Restaurant not discovered: {discovery.get("status")}')
    return discovery


def show_menu(discovery):
    restaurant = discovery['resource']
    menu = restaurant['menu']
    log(f'{restaurant["name"]} - {restaurant["address"]}')
    for number, item in enumerate(menu, 1):
        log(f'{number}. {item["name"]} ({item["item_id"]}) {money(item["price_cents"])}'
            + ('' if item['available'] else ' [SOLD OUT]'))


def show_featured(address):
    try:
        state = rpc(address, {'action': 'GET_COORDINATION'}, timeout=2)
        if state.get('status') != 'OK':
            raise ValueError(state.get('message', 'Featured status unavailable'))
    except OSError:
        log('Featured Restaurant unavailable while service is down')
        return
    featured = state.get('holder')
    if not featured:
        log('No restaurant is featured right now.')
        return
    try:
        leader = state.get('leader')
        if not leader:
            raise ValueError('Coordinator replacement is in progress')
        topology = rpc(peer_address(leader), {'action': 'TOPOLOGY'}, timeout=2)
        if topology.get('status') != 'OK':
            raise ValueError('Restaurant directory unavailable')
        member = next((p for p in topology['members'] if p['restaurant_id'] == featured['restaurant_id']), None)
        if member is None:
            raise ValueError('Featured restaurant is not in the current directory')
        result = rpc(peer_address(member), {'action': 'LOOKUP',
                     'restaurant_id': featured['restaurant_id']}, timeout=3)
        if result.get('status') != 'FOUND':
            raise ValueError('Featured restaurant details unavailable')
        restaurant = result['resource']
        log('\n' + '=' * 36 + '\nFEATURED RESTAURANT\n' + '-' * 36 +
            f'\n{restaurant["name"]}\nAddress: {restaurant["address"]}\n' + '=' * 36)
    except (OSError, ValueError, KeyError, TypeError):
        log('A restaurant is featured, but its details are temporarily unavailable.')


def select_for_order(console, bootstrap, address):

    selected = None
    while True:
        if selected:
            log('Selected restaurant: ' + selected['resource']['name'])
        choice = console.read('1. Place an order\n2. Find a restaurant\n3. Exit\nChoose an option:').strip()
        if choice == '3':
            return None
        if choice not in ('1', '2'):
            log('Please enter 1, 2, or 3.')
            continue
        try:
            show_featured(address)
            if choice == '2':
                found = discover_restaurant(bootstrap, console)
                show_menu(found)
                selected = found
                log('Selection saved for this session. Choose 1 to order here, or 2 to choose another restaurant.')
                continue
            if selected:

                log('Ordering from selected restaurant: ' + selected['resource']['name'])
                found = discover_restaurant(peer_address(selected['peer']), console,
                                            selected['resource']['restaurant_id'])
            else:
                found = discover_restaurant(bootstrap, console)
            show_menu(found)
            return found
        except (OSError, ValueError, KeyError, TypeError) as error:
            log(f'Restaurant lookup failed: {error}. No order was sent; choose another restaurant or retry.')


def main():
    parser = argparse.ArgumentParser(description='DiDi customer: P2P discovery and ordering')
    parser.add_argument('--peer', default='127.0.0.1:7001')
    parser.add_argument('--service', default='127.0.0.1:6000')
    parser.add_argument('--restaurant', help='Restaurant ID; ask interactively if omitted')
    parser.add_argument('--items', help='Item IDs and quantities, for example burger:2,fries:1')
    parser.add_argument('--name', default='Jordan Smith')
    parser.add_argument('--destination', default='1250 Bellflower Blvd, Long Beach, CA')
    parser.add_argument('--lookup-only', action='store_true')
    parser.add_argument('--details', action='store_true')
    parser.add_argument('--auto-confirm', action='store_true', help='Confirm receipt automatically for tests')
    args = parser.parse_args()
    configure('CUSTOMER', args.details)
    subscription = None
    stopped = threading.Event()
    tracker = OrderTracker()
    customer_id = uuid.uuid4().hex
    console = Console()
    try:
        address = endpoint(args.service)
        bootstrap = endpoint(args.peer)
        if not args.restaurant and not args.items and not args.lookup_only:
            discovery = select_for_order(console, bootstrap, address)
            if discovery is None:
                return 0
        else:
            show_featured(address)
            discovery = discover_restaurant(bootstrap, console, args.restaurant)
            show_menu(discovery)
        restaurant = discovery['resource']
        rid, menu = restaurant['restaurant_id'], restaurant['menu']
        if args.lookup_only:
            return 0
        if args.items:
            items = choose_items(args.items, menu)
        else:
            items = choose_items(console.read('Choose menu-number:quantity, for example 1:2,2:1'), menu, numbered=True)
        def disconnected(error):
            log(f'Service disconnected: {error}. Waiting to reconnect; delivery is not yet confirmed.')
        def on_event(event):
            order = event.get('payload', {})
            if order.get('customer_id') == customer_id and tracker.update(order):
                log(f'{event.get("event_type")} order={order["order_id"]}')
        subscription = Subscription(address, args.name, on_event, disconnected,
                                    on_connect=lambda: log('Customer subscription ready; state resynchronized by order snapshot'))
        subscription.start()
        reply = reliable_rpc(address, {'action': 'PLACE_ORDER', 'customer_name': args.name, 'customer_id': customer_id, 'restaurant_id': rid,
                              'peer': discovery['peer'], 'destination': args.destination, 'items': items}, stop=stopped)
        if reply.get('status') != 'ORDER_CONFIRMED':
            raise ValueError(reply.get('message', 'Order rejected'))
        oid = reply['order_id']
        tracker.update(reply.get('order'))
        log(f'ORDER_CONFIRMED {oid}; service-verified total: {money(reply["total_cents"])}')
        if reply.get('offer_sent') is False:
            log('Driver invitation failed; order remains accepted. No automatic invitation retry.')
        next_snapshot, shown = 0, None
        log('Type issue at any time while waiting if there is a handoff problem.')
        while True:
            if time.monotonic() >= next_snapshot:
                next_snapshot = time.monotonic() + 1
                try:
                    snapshot = rpc(address, {'action': 'GET_ORDER', 'order_id': oid}, timeout=2)
                    tracker.update(snapshot.get('order'))
                except OSError:
                    pass
            order = tracker.get(oid)
            if order is None:
                continue
            status = order['status']
            if status == 'CANCELLED':
                log(SUPPORT_MESSAGE)
                log('Customer app closed.')
                return 0
            if status == 'DELIVERED':
                log('Your food has arrived. Customer app closed.')
                return 0
            if status != shown:
                log(f'Order snapshot {oid}: {status}')
                if status == 'WAITING_DELIVERY_CONFIRMATION':
                    log('Press Enter to confirm you received the food, or type issue to cancel.')
                else:
                    log('Waiting for the next handoff. Type issue if there is a problem.')
                shown = status
            line = '' if args.auto_confirm and status == 'WAITING_DELIVERY_CONFIRMATION' else console.poll()
            if line is None:
                continue
            command = line.strip().lower()
            if command == 'issue':
                action = 'REPORT_ISSUE'
            elif command == '' and status == 'WAITING_DELIVERY_CONFIRMATION':
                action = 'CONFIRM_DELIVERY'
            else:
                log('No receipt confirmation is pending yet. Type issue to report a problem.')
                continue
            result = reliable_rpc(address, {'action': action, 'order_id': oid,
                                  'role': 'customer', 'customer_id': customer_id}, stop=stopped)
            log(result.get('message', result.get('status')))
            tracker.update(result.get('order'))
            next_snapshot = 0
    except KeyboardInterrupt:
        log('Customer stopped; this does not cancel an accepted order.')
        return 0
    except (OSError, ValueError, KeyError, TypeError, EOFError) as error:
        log(f'Customer failed: {error}. If a request was sent, its outcome may be unknown; no automatic retry.')
        return 1
    finally:
        if subscription:
            subscription.close()


if __name__ == '__main__':
    raise SystemExit(main())
