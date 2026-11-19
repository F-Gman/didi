"""Small shared networking helpers. Python standard library only."""
import json
import os
import select
import socket
import socketserver
import sys
import threading
import time
import uuid
from datetime import datetime, timezone

HOST = '127.0.0.1'
SERVICE_PORT = 6000
MULTICAST_GROUP = '224.1.1.1'
MULTICAST_PORT = 5007
MULTICAST_INTERFACE = HOST
MAX_MESSAGE_BYTES = 65536
APP_NAME = 'DIDI'
DETAILS = False
PRINT_LOCK = threading.Lock()
PROCESS_SESSION = uuid.uuid4().hex


class LamportClock:

    def __init__(self):
        self.value = 0
        self.lock = threading.Lock()

    def tick(self):
        with self.lock:
            self.value += 1
            return self.value

    def receive(self, received):
        if type(received) is not int or received < 0:
            raise ValueError('lamport must be a non-negative integer')
        with self.lock:
            self.value = max(self.value, received) + 1
            return self.value


CLOCK = LamportClock()


def configure(name, details=False):
    global APP_NAME, DETAILS
    APP_NAME, DETAILS = name, details


def log(message):

    with PRINT_LOCK:
        print(f'[{datetime.now():%H:%M:%S}] [{APP_NAME}] [L={CLOCK.value}] {message}', end='\n\n', flush=True)


def timestamp():
    return datetime.now(timezone.utc).isoformat(timespec='milliseconds')


def crash_process():

    log('CRASH: terminating this process immediately without graceful cleanup')
    os._exit(1)


def envelope(data):

    message = dict(data)
    message.setdefault('message_id', uuid.uuid4().hex)
    message.setdefault('request_id', message['message_id'])
    message.setdefault('sender_id', APP_NAME + '@' + PROCESS_SESSION[:8])
    message.setdefault('timestamp', timestamp())
    return message


def response(request, status, **fields):
    return envelope({'status': status, 'request_id': request.get('request_id', uuid.uuid4().hex), **fields})


def marshal(data):
    message = envelope(data)

    message.update(message_id=uuid.uuid4().hex, sender_id=APP_NAME + '@' + PROCESS_SESSION[:8],
                   timestamp=timestamp(), lamport=CLOCK.tick())
    wire = (json.dumps(message, ensure_ascii=True, allow_nan=False) + '\n').encode('utf-8')
    if len(wire) > MAX_MESSAGE_BYTES:
        raise ValueError('Message exceeds 65536 bytes')
    kind = message.get('action', message.get('event_type', message.get('status', 'MESSAGE')))
    if DETAILS or kind not in ('PING', 'OK', 'COORD_HEARTBEAT', 'GET_COORDINATION', 'GET_ORDERS', 'GET_ORDER', 'FEATURE_RENEW', 'FEATURE_GRANT', 'LOCK_RENEW'):
        log(f'SEND {kind} ts={message["lamport"]} request={message["request_id"]}')
    if DETAILS:
        log(f'Before marshalling: {message}')
        log(f'After marshalling: {wire!r}')
    return wire


def unmarshal(wire):
    if len(wire) > MAX_MESSAGE_BYTES:
        raise ValueError('Message exceeds 65536 bytes')
    data = json.loads(wire.decode('utf-8'))
    if not isinstance(data, dict):
        raise ValueError('Message must be a JSON object')
    received = data.get('lamport', 0)
    local = CLOCK.receive(received)
    kind = data.get('action', data.get('event_type', data.get('status', 'MESSAGE')))
    if DETAILS or kind not in ('PING', 'OK', 'COORD_HEARTBEAT', 'GET_COORDINATION', 'GET_ORDERS', 'GET_ORDER', 'FEATURE_RENEW', 'FEATURE_GRANT', 'LOCK_RENEW'):
        log(f'RECEIVE {kind} remote_ts={received} local_ts={local} request={data.get("request_id")}')
    if DETAILS:
        log(f'After unmarshalling: {data}')
    return data


def receive_message(stream):

    wire = stream.readline(MAX_MESSAGE_BYTES + 1)
    if not wire:
        raise ConnectionError('Remote process disconnected')
    if len(wire) > MAX_MESSAGE_BYTES or not wire.endswith(b'\n'):
        raise ValueError('Message is too large or missing its final newline')
    return unmarshal(wire)


def send_message(sock, message):
    sock.sendall(marshal(message))


def endpoint(text):
    host, port = text.rsplit(':', 1)

    if host not in (HOST, 'localhost') or not 1 <= int(port) <= 65535:
        raise ValueError('Use a localhost endpoint such as 127.0.0.1:7001')
    return HOST, int(port)


def peer_address(peer):
    if not isinstance(peer, dict):
        raise ValueError('Peer address must be an object')
    port = peer.get('port')
    if peer.get('host') != HOST or type(port) is not int or not 1 <= port <= 65535:
        raise ValueError('Peer must use 127.0.0.1 and a valid integer port')
    return HOST, port


def rpc(address, message, timeout=6):

    with socket.create_connection(address, timeout=min(timeout, 0.7)) as sock:
        sock.settimeout(timeout)
        send_message(sock, message)
        with sock.makefile('rb') as stream:
            return receive_message(stream)


def reliable_rpc(address, message, stop=None):

    request = envelope(message)
    reported = False
    while stop is None or not stop.is_set():
        try:
            reply = rpc(address, request, timeout=15)
            if reported:
                log('Service reconnected; command result recovered with the same request_id')
            return reply
        except OSError as error:
            if not reported:
                log(f'Service unavailable: {error}. Waiting to retry the SAME request_id; Ctrl+C to stop.')
                reported = True
            if stop:
                stop.wait(0.5)
            else:
                time.sleep(0.5)
    raise ConnectionError('Client stopped')


def fetch_orders(address, **filters):

    orders = []
    after = ''
    while True:
        reply = rpc(address, {'action': 'GET_ORDERS', 'after': after, **filters}, timeout=3)
        if reply.get('status') != 'OK':
            raise ValueError(reply.get('message', 'Order snapshot rejected'))
        orders.extend(reply['orders'])
        cursor = reply.get('next_cursor')
        if not cursor:
            return orders
        if cursor <= after:
            raise ValueError('Invalid snapshot cursor')
        after = cursor


class TCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


class TCPHandler(socketserver.StreamRequestHandler):
    def handle(self):
        request = {}
        self.request.settimeout(15)
        try:
            request = receive_message(self.rfile)
            self.server.node.handle_connection(self.request, self.rfile, request)
        except (OSError, ValueError, UnicodeError, KeyError, TypeError) as error:
            try:
                send_message(self.request, response(request, 'ERROR', message=str(error)))
            except (OSError, ValueError):
                pass


def start_server(node, port):
    server = TCPServer((HOST, port), TCPHandler)
    server.node = node
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


class Subscription:
    """One persistent TCP connection; callback receives network events."""
    def __init__(self, address, role, callback, on_disconnect, on_connect=None):
        self.address, self.role = address, role
        self.callback, self.on_disconnect = callback, on_disconnect
        self.ready = threading.Event()
        self.stopped = threading.Event()
        self.failed = threading.Event()
        self.sock = None
        self.on_connect = on_connect

    def start(self):
        threading.Thread(target=self.listen, daemon=True).start()
        self.ready.wait(3)

    def listen(self):
        reported = False
        while not self.stopped.is_set():
            try:
                self.sock = socket.create_connection(self.address, timeout=3)
                with self.sock.makefile('rb') as stream:
                    send_message(self.sock, {'action': 'SUBSCRIBE_UPDATES', 'role': self.role})
                    if receive_message(stream).get('status') != 'SUBSCRIBED':
                        raise ValueError('Subscription rejected')
                    self.sock.settimeout(None)
                    self.failed.clear()
                    self.ready.set()
                    if self.on_connect:
                        self.on_connect()
                    if reported:
                        log('Subscription reconnected; state resynchronized')
                    reported = False
                    while not self.stopped.is_set():
                        self.callback(receive_message(stream))
            except (OSError, ValueError, UnicodeError, KeyError, TypeError) as error:
                if not self.stopped.is_set():
                    self.failed.set()
                    if not reported:
                        self.on_disconnect(error)
                    reported = True
            finally:
                if self.sock:
                    self.sock.close()
            self.stopped.wait(0.5)

    def close(self):
        self.stopped.set()
        if self.sock:
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.sock.close()


class Console:
    """Interruptible input without a blocked input() thread stealing later input."""
    def __init__(self):
        self.buffer = ''
        self.pipe_buffer = b''
        self.eof = False

    def poll(self, timeout=0.15):
        if b'\n' in self.pipe_buffer:
            line, self.pipe_buffer = self.pipe_buffer.split(b'\n', 1)
            return line.rstrip(b'\r').decode('utf-8')
        if os.name == 'nt' and sys.stdin.isatty():
            import msvcrt
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                if not msvcrt.kbhit():
                    time.sleep(0.02)
                    continue
                char = msvcrt.getwch()
                if char == '\x03':
                    raise KeyboardInterrupt
                if char in ('\x00', '\xe0'):
                    msvcrt.getwch()
                elif char in ('\r', '\n'):
                    print(flush=True)
                    line, self.buffer = self.buffer, ''
                    return line
                elif char == '\b':
                    if self.buffer:
                        self.buffer = self.buffer[:-1]
                        print('\b \b', end='', flush=True)
                elif char.isprintable():
                    self.buffer += char
                    print(char, end='', flush=True)
            return None

        if os.name != 'nt':
            if not select.select([sys.stdin], [], [], timeout)[0]:
                return None
        elif not sys.stdin.isatty():

            import ctypes
            import msvcrt
            available = ctypes.c_ulong()
            kernel = ctypes.WinDLL('kernel32', use_last_error=True)
            kernel.PeekNamedPipe.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_ulong,
                                            ctypes.c_void_p, ctypes.POINTER(ctypes.c_ulong), ctypes.c_void_p]
            handle = msvcrt.get_osfhandle(sys.stdin.fileno())
            if not kernel.PeekNamedPipe(handle, None, 0, None, ctypes.byref(available), None):
                self.eof = True
                if self.pipe_buffer:
                    line, self.pipe_buffer = self.pipe_buffer, b''
                    return line.decode('utf-8').rstrip('\r')
                return None
            if not available.value:
                time.sleep(timeout)
                return None

            self.pipe_buffer += os.read(sys.stdin.fileno(), available.value)
            if b'\n' in self.pipe_buffer:
                line, self.pipe_buffer = self.pipe_buffer.split(b'\n', 1)
                return line.rstrip(b'\r').decode('utf-8')
            return None
        line = sys.stdin.readline()
        if line == '':
            self.eof = True
            return None
        return line.rstrip('\r\n')

    def read(self, prompt, stop=None):
        log(prompt)
        while stop is None or not stop.is_set():
            line = self.poll()
            if line is not None:
                return line
            if self.eof:
                raise EOFError('Console input closed')
        raise ConnectionError('Operation stopped')


def money(cents):
    return f'${cents // 100}.{cents % 100:02d}'


TERMINAL_ORDER_STATES = ('DELIVERED', 'CANCELLED')
SUPPORT_MESSAGE = 'Order cancelled because an issue was reported. Please contact DiDi service staff for assistance.'


class OrderTracker:
    """Keep the newest order revision when replies, events and snapshots interleave."""
    def __init__(self):
        self.orders = {}
        self.lock = threading.Lock()

    def update(self, order):
        if not isinstance(order, dict) or not isinstance(order.get('order_id'), str):
            return False
        with self.lock:
            old = self.orders.get(order['order_id'])
            if old and order.get('revision', 0) <= old.get('revision', 0):
                return False
            self.orders[order['order_id']] = dict(order)
            return True

    def get(self, oid):
        with self.lock:
            order = self.orders.get(oid)
            return dict(order) if order else None

    def all(self):
        with self.lock:
            return [dict(order) for order in self.orders.values()]
