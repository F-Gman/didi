"""Fenced shared resource at the service; Bully peer client is defined below."""
import time
import threading
import uuid

from common import CLOCK, log, peer_address, rpc


def record(state, event, **fields):
    entry = {'event': event, 'lamport': CLOCK.tick(), **fields}
    state['history'].append(entry)
    log(f'LOCAL {event} {fields}')


FEATURE_ACTIONS = {'REGISTER_COORDINATOR', 'COORD_HEARTBEAT', 'GET_COORDINATION',
                   'FEATURE_ENQUEUE', 'FEATURE_GRANT', 'FEATURE_RENEW',
                   'FEATURE_RELEASE', 'FEATURE_CANCEL'}


def feature_command(state, msg, lease_seconds, leader_seconds):
    """Caller holds the service's SQLite lock; each returned state is committed."""
    now = time.monotonic()
    if state['holder'] and state['holder']['deadline'] <= now:
        record(state, 'EXPIRE', token=state['holder']['token'])
        state['holder'] = None
    if state['leader'] and state['leader']['deadline'] <= now:
        state['leader'] = None
    action = msg['action']
    if action == 'GET_COORDINATION':
        return {'status': 'OK', 'boot_id': state['boot_id'], 'term': state['term'],
                'leader': state['leader'], 'holder': state['holder'], 'queue': state['queue']}
    if action == 'REGISTER_COORDINATOR':
        candidate = msg['candidate']
        current = state['leader']
        if current and current['session'] == candidate['session']:
            return {'status': 'OK', 'term': state['term'], 'boot_id': state['boot_id']}
        if current and int(current['restaurant_id'][1:]) > int(candidate['restaurant_id'][1:]):
            raise ValueError('Higher coordinator lease is still active; wait before replacement')
        state['term'] += 1
        state['leader'] = {**candidate, 'deadline': now + leader_seconds}
        record(state, 'COORDINATOR', leader=candidate['restaurant_id'], term=state['term'], boot_id=state['boot_id'])
        return {'status': 'OK', 'term': state['term'], 'boot_id': state['boot_id']}
    leader = state['leader']
    if (not leader or msg.get('boot_id') != state['boot_id'] or msg.get('term') != state['term']
            or msg.get('coordinator_id') != leader['restaurant_id']
            or msg.get('coordinator_session') != leader['session']):
        raise ValueError('STALE_COORDINATOR: coordinator session, term or boot is no longer valid')
    if action == 'COORD_HEARTBEAT':
        leader['deadline'] = now + leader_seconds
        return {'status': 'OK'}
    if action == 'FEATURE_ENQUEUE':
        rid, session, attempt = msg.get('restaurant_id'), msg.get('session'), msg.get('attempt')
        if not all(isinstance(x, str) and 0 < len(x) <= 100 for x in (rid, session, attempt)):
            raise ValueError('Feature request needs restaurant_id, session and attempt')
        holder = state['holder']
        if holder and holder['session'] == session and holder['attempt'] == attempt:
            return {'status': 'GRANTED', 'holder': holder}
        if not any(q['session'] == session and q['attempt'] == attempt for q in state['queue']):
            if len(state['queue']) >= 100:
                raise ValueError('Feature queue is full')
            state['queue'].append({'restaurant_id': rid, 'session': session, 'attempt': attempt})
        return {'status': 'QUEUED', 'position': next(i + 1 for i, q in enumerate(state['queue'])
                         if q['session'] == session and q['attempt'] == attempt)}
    if action == 'FEATURE_GRANT':
        if state['holder'] is None and state['queue']:
            state['token'] += 1
            state['holder'] = {**state['queue'].pop(0), 'token': state['token'],
                               'boot_id': state['boot_id'], 'deadline': now + lease_seconds}
            record(state, 'GRANT', **state['holder'])
        return {'status': 'OK', 'holder': state['holder']}
    if action == 'FEATURE_CANCEL':
        state['queue'] = [q for q in state['queue'] if not
                          (q['session'] == msg.get('session') and q['attempt'] == msg.get('attempt'))]
        return {'status': 'CANCELLED'}
    holder = state['holder']
    if not holder or holder['token'] != msg.get('token') or holder['session'] != msg.get('session'):
        raise ValueError('STALE_TOKEN: caller does not hold the current unexpired grant')
    if action == 'FEATURE_RENEW':
        holder['deadline'] = now + lease_seconds
        return {'status': 'OK', 'holder': holder}
    if action == 'FEATURE_RELEASE':
        record(state, 'RELEASE', token=holder['token'])
        state['holder'] = None
        return {'status': 'RELEASED'}
    raise ValueError('Unknown coordination action')


def rank(member):
    return int(member['restaurant_id'][1:])


class Coordinator:
    """Restaurant-side Bully election and REQUEST/GRANT/RELEASE client."""
    def __init__(self, peer, service):
        self.peer, self.service = peer, service
        self.stop = threading.Event()
        self.election_requested = threading.Event()
        self.lock = threading.RLock()
        self.leader = None
        self.fence = None
        self.boot_id = None
        self.wants_feature = False
        self.attempt = uuid.uuid4().hex
        self.grant = None
        self.last_status = None

    def start(self):
        threading.Thread(target=self.run, daemon=True).start()

    def service_command(self, action, **fields):
        reply = rpc(self.service, {'action': action, **fields}, timeout=2)
        if reply.get('status') == 'ERROR':
            raise ValueError(reply.get('message', 'Coordination request rejected'))
        return reply

    def elect(self):
        higher_answered = False
        log(f'ELECTION started by {self.peer.me["restaurant_id"]}')
        for member in self.peer.snapshot():
            if rank(member) <= rank(self.peer.me):
                continue
            try:
                reply = rpc(peer_address(member), {'action': 'ELECTION', 'candidate': self.peer.me}, timeout=0.8)
                higher_answered |= reply.get('status') == 'ALIVE'
            except OSError:
                pass
        if higher_answered:
            return
        result = self.service_command('REGISTER_COORDINATOR', candidate=self.peer.me,
                                      members=self.peer.snapshot())
        fence = {'coordinator_id': self.peer.me['restaurant_id'],
                 'coordinator_session': self.peer.me['session'],
                 'term': result['term'], 'boot_id': result['boot_id']}
        with self.lock:
            self.fence = fence
            self.leader = self.peer.me
        log(f'COORDINATOR elected: {self.peer.me["restaurant_id"]} term={result["term"]} boot={result["boot_id"]}')
        for member in self.peer.snapshot():
            if member == self.peer.me:
                continue
            try:
                rpc(peer_address(member), {'action': 'COORDINATOR', 'leader': self.peer.me,
                                           'term': result['term'], 'boot_id': result['boot_id']}, timeout=0.8)
            except OSError:
                pass

    def handle(self, request):
        action = request['action']
        if action == 'ELECTION':
            if rank(self.peer.me) > rank(request['candidate']):
                self.election_requested.set()
            return {'status': 'ALIVE', 'peer': self.peer.me}
        if action == 'COORDINATOR':
            # Announcement is a hint; run() checks the authoritative fenced service view.
            self.election_requested.clear()
            log(f'COORDINATOR announcement: {request["leader"]["restaurant_id"]}')
            return {'status': 'OK'}
        if action == 'FEATURE_REQUEST':
            with self.lock:
                if not self.wants_feature:
                    self.wants_feature = True
                    self.attempt = uuid.uuid4().hex
                    self.last_status = None
            log('REQUEST Featured Restaurant; waiting for coordinator permission')
            return {'status': 'REQUESTED'}
        if action == 'FEATURE_STOP':
            with self.lock:
                self.wants_feature = False
                grant, leader, attempt = self.grant, self.leader, self.attempt
            if leader:
                reply = rpc(peer_address(leader), {'action': 'LOCK_RELEASE', 'session': self.peer.me['session'],
                            'attempt': attempt, 'token': grant['token'] if grant else None}, timeout=2)
                if reply.get('status') == 'ERROR':
                    return reply
            with self.lock:
                self.grant = None
            log('RELEASE requested; no longer trying to hold the Featured slot')
            return {'status': 'RELEASED'}
        if action == 'COORD_STATUS':
            with self.lock:
                return {'status': 'OK', 'leader': self.leader, 'grant': self.grant,
                        'requesting': self.wants_feature}
        with self.lock:
            fence = dict(self.fence) if self.fence else None
        if not fence:
            return {'status': 'ERROR', 'message': 'Not the registered coordinator; retry after election'}
        if action == 'LOCK_REQUEST':
            requester = request['requester']
            # Verify requester incarnation before it may enter the service queue.
            proof = rpc(peer_address(requester), {'action': 'PING'}, timeout=1)
            if proof.get('session') != requester.get('session'):
                raise ValueError('Requester session no longer matches')
            result = self.service_command('FEATURE_ENQUEUE', **fence,
                        restaurant_id=requester['restaurant_id'], session=requester['session'], attempt=request['attempt'])
            self.service_command('FEATURE_GRANT', **fence)
            return result
        if action == 'LOCK_RENEW':
            return self.service_command('FEATURE_RENEW', **fence, token=request['token'], session=request['session'])
        if action == 'LOCK_RELEASE':
            self.service_command('FEATURE_CANCEL', **fence, session=request['session'], attempt=request['attempt'])
            if request.get('token') is not None:
                self.service_command('FEATURE_RELEASE', **fence, token=request['token'], session=request['session'])
            self.service_command('FEATURE_GRANT', **fence)
            return {'status': 'RELEASED'}
        return {'status': 'ERROR', 'message': 'Unknown coordinator action'}

    def step(self):
        state = self.service_command('GET_COORDINATION')
        with self.lock:
            if self.boot_id is not None and self.boot_id != state['boot_id']:
                self.grant = None
                self.attempt = uuid.uuid4().hex
                self.last_status = None
                log('Service restarted: old feature permission invalidated; requesting again if needed')
            self.boot_id = state['boot_id']
        leader = state['leader']
        alive = False
        if leader:
            try:
                proof = rpc(peer_address(leader), {'action': 'PING'}, timeout=0.8)
                alive = proof.get('session') == leader['session']
            except OSError:
                pass
        # A delayed lower-peer ELECTION must not interrupt a healthy leader's heartbeat.
        # The service view and session probe above decide whether election is needed.
        self.election_requested.clear()
        if not alive or (leader and rank(self.peer.me) > rank(leader)):
            self.elect()
            return
        with self.lock:
            self.leader = leader
            self.fence = ({'coordinator_id': leader['restaurant_id'], 'coordinator_session': leader['session'],
                           'term': state['term'], 'boot_id': state['boot_id']}
                          if leader['session'] == self.peer.me['session'] else None)
            fence = self.fence
            wants, attempt = self.wants_feature, self.attempt
        if fence:
            self.service_command('COORD_HEARTBEAT', **fence)
            self.service_command('FEATURE_GRANT', **fence)
        holder = state['holder']
        mine = holder and holder['session'] == self.peer.me['session'] and holder['attempt'] == attempt
        if wants:
            if mine:
                result = rpc(peer_address(leader), {'action': 'LOCK_RENEW', 'session': self.peer.me['session'],
                                                   'token': holder['token']}, timeout=2)
                if result.get('status') == 'ERROR':
                    raise ValueError(result.get('message'))
                status = f'GRANT Featured token={holder["token"]}; exclusive slot active'
                with self.lock:
                    self.grant = holder
            else:
                with self.lock:
                    self.grant = None
                result = rpc(peer_address(leader), {'action': 'LOCK_REQUEST', 'requester': self.peer.me,
                                                   'attempt': attempt}, timeout=3)
                if result.get('status') == 'ERROR':
                    raise ValueError(result.get('message'))
                status = 'WAIT Featured; another restaurant may hold the slot'
            if status != self.last_status:
                log(status)
                self.last_status = status
        elif mine:
            # Also cancel an in-flight acquisition if release raced a network reply.
            rpc(peer_address(leader), {'action': 'LOCK_RELEASE', 'session': self.peer.me['session'],
                                      'attempt': attempt, 'token': holder['token']}, timeout=2)

    def run(self):
        reported = False
        while not self.stop.is_set() and not self.peer.stopped.is_set():
            try:
                self.step()
                reported = False
            except (OSError, ValueError, KeyError, TypeError) as error:
                with self.lock:
                    self.grant = None
                if not reported:
                    log(f'Coordination waiting safely: {error}')
                reported = True
            self.stop.wait(0.8)
