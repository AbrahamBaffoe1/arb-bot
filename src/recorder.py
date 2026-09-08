"""Bounded, asynchronous SQLite event tape. No credentials or account payloads belong here."""
from __future__ import annotations
import json
import queue
import sqlite3
import threading
import time
import uuid
import zlib
import shutil
from pathlib import Path

SCHEMA = '''
CREATE TABLE IF NOT EXISTS sessions(id TEXT PRIMARY KEY, started REAL, config TEXT, identity TEXT, ended REAL);
CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY, session TEXT, received REAL, monotonic_ns INTEGER,
    source_ts REAL, kind TEXT, venue TEXT, symbol TEXT, body TEXT);
CREATE INDEX IF NOT EXISTS events_session_id ON events(session,id);
CREATE INDEX IF NOT EXISTS events_kind_time ON events(kind,received);
'''


class Recorder:
    def __init__(self, path, cfg, identity):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.session = str(uuid.uuid4())
        self.identity = identity
        self.cfg = cfg
        settings = cfg.get('recording', {})
        self.queue = queue.Queue(maxsize=settings.get('queue_capacity', 20000))
        self.max_bytes = settings.get('max_disk_gb', 5) * 1024**3
        self.min_free_bytes=settings.get('min_free_disk_gb',10)*1024**3
        self.error = None
        self.dropped = 0
        self.written = 0
        self.last_commit = 0.0
        self._stop = threading.Event()
        self._ready = threading.Event()
        self._thread = threading.Thread(target=self._write, name='event-recorder', daemon=True)
        self._thread.start()
        self._ready.wait(5)
        if self.error or not self._ready.is_set():
            raise RuntimeError(self.error or 'Recorder startup timed out')
        self.emit('session_start', body={'identity':identity})

    def emit(self, kind, venue='', symbol='', body=None, source_ts=None, received=None):
        if self.error or self._stop.is_set():
            return False
        try:
            payload = zlib.compress(json.dumps(body or {}, separators=(',', ':'), allow_nan=False).encode(),level=1)
            row = (self.session, time.time() if received is None else received, time.monotonic_ns(),
                   source_ts, kind, venue, symbol, payload)
            self.queue.put_nowait(row)
            return True
        except (queue.Full, ValueError, TypeError):
            self.dropped += 1
            self.error = 'Event tape gap: recorder queue overflow or invalid event; restart required'
            return False

    def _write(self):
        db = None
        try:
            db = sqlite3.connect(self.path)
            db.execute('PRAGMA journal_mode=WAL')
            db.execute('PRAGMA synchronous=FULL')
            db.executescript(SCHEMA)
            # Configuration contains only public settings, never environment credentials.
            db.execute('INSERT INTO sessions VALUES(?,?,?,?,NULL)',
                       (self.session,time.time(),json.dumps(self.cfg,sort_keys=True),self.identity))
            db.commit()
            self._ready.set()
            last_size_check = 0.0
            while not self._stop.is_set() or not self.queue.empty():
                batch = []
                try:
                    batch.append(self.queue.get(timeout=.2))
                except queue.Empty:
                    pass
                while len(batch) < 500:
                    try:
                        batch.append(self.queue.get_nowait())
                    except queue.Empty:
                        break
                if batch:
                    with db:
                        db.executemany('INSERT INTO events(session,received,monotonic_ns,source_ts,kind,venue,symbol,body) VALUES(?,?,?,?,?,?,?,?)',batch)
                    self.written += len(batch)
                    for _ in batch:
                        self.queue.task_done()
                    self.last_commit = time.time()
                if time.monotonic()-last_size_check > 5:
                    size = sum(p.stat().st_size for p in (self.path,Path(str(self.path)+'-wal')) if p.exists())
                    if size > self.max_bytes:
                        self.error = 'Event tape storage budget exceeded; archive recordings before restart'
                    if shutil.disk_usage(self.path.parent).free<self.min_free_bytes:
                        self.error = 'Free disk reserve reached; archive recordings before restart'
                    last_size_check = time.monotonic()
            if self.error:
                db.execute('INSERT INTO events(session,received,monotonic_ns,kind,venue,symbol,body) VALUES(?,?,?,?,?,?,?)',
                           (self.session,time.time(),time.monotonic_ns(),'recording_gap','','',json.dumps({'reason':self.error,'dropped':self.dropped})))
            db.execute('UPDATE sessions SET ended=? WHERE id=?',(time.time(),self.session))
            db.commit()
        except Exception as exc:
            self.error = f'Recorder storage failure: {type(exc).__name__}'
            self._ready.set()
        finally:
            if db:
                db.close()

    def snapshot(self):
        return dict(session=self.session,healthy=self.error is None and self._thread.is_alive(),
                    error=self.error,queued=self.queue.qsize(),dropped=self.dropped,written=self.written,
                    last_commit=self.last_commit,path=str(self.path))

    def close(self):
        self._stop.set()
        self._thread.join(timeout=10)
        if self._thread.is_alive():
            raise RuntimeError('Recorder did not finish flushing; tape may be incomplete')


def sessions(path):
    with sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True) as db:
        return [dict(id=r[0],started=r[1],ended=r[2],events=r[3]) for r in db.execute(
            'SELECT s.id,s.started,s.ended,count(e.id) FROM sessions s LEFT JOIN events e ON e.session=s.id GROUP BY s.id ORDER BY s.started DESC')]


def read_events(path, session):
    db = sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True)
    try:
        for row in db.execute('SELECT id,received,source_ts,kind,venue,symbol,body FROM events WHERE session=? ORDER BY id',(session,)):
            body=zlib.decompress(row[6]).decode() if isinstance(row[6],bytes) else row[6]
            yield dict(id=row[0],received=row[1],source_ts=row[2],kind=row[3],venue=row[4],symbol=row[5],body=json.loads(body))
    finally:
        db.close()
