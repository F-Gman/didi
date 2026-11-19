"""One durable SQLite state document; a transaction includes command deduplication."""
import json
import os
from pathlib import Path
import sqlite3
import threading


class Store:
    def __init__(self, path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.closed = False

        self.guard = open(str(path) + '.lock', 'a+b')
        self.guard.seek(0)
        self.guard.write(b'0')
        self.guard.flush()
        self.guard.seek(0)
        try:
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(self.guard.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.guard, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.guard.close()
            raise OSError('Another service is already using this database')
        self.db = sqlite3.connect(path, check_same_thread=False, timeout=5)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('CREATE TABLE IF NOT EXISTS state (id INTEGER PRIMARY KEY, body TEXT NOT NULL)')
        row = self.db.execute('SELECT body FROM state WHERE id=1').fetchone()
        state = json.loads(row[0]) if row else {'orders': {}, 'requests': {}, 'boot_id': 0,
                                               'term': 0, 'token': 0, 'history': []}
        state.update(boot_id=state['boot_id'] + 1, leader=None, holder=None, queue=[])
        self.save(state)

    def read(self):
        return json.loads(self.db.execute('SELECT body FROM state WHERE id=1').fetchone()[0])

    def save(self, state):

        with self.db:
            self.db.execute('INSERT OR REPLACE INTO state(id,body) VALUES(1,?)',
                            (json.dumps(state, allow_nan=False),))

    def close(self):
        if self.closed:
            return
        self.closed = True
        self.db.close()
        self.guard.seek(0)
        if os.name == 'nt':
            import msvcrt
            msvcrt.locking(self.guard.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(self.guard, fcntl.LOCK_UN)
        self.guard.close()
