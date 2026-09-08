"""Local telemetry, incident journal, heartbeat and consistent SQLite backups."""
from __future__ import annotations
import collections
import json
import os
import shutil
import sqlite3
import time
import hashlib
from pathlib import Path


def atomic_json(path,payload):
    path=Path(path); path.parent.mkdir(parents=True,exist_ok=True)
    temp=path.with_name(path.name+f'.{os.getpid()}.tmp')
    with temp.open('w') as f:
        json.dump(payload,f,allow_nan=False); f.flush(); os.fsync(f.fileno())
    os.replace(temp,path)


class Operations:
    def __init__(self,cfg,db,run_id):
        self.cfg=cfg; self.db=db; self.run_id=run_id
        self.samples=collections.defaultdict(lambda:collections.deque(maxlen=2000))
        self.last_alert={}
        self.last_heartbeat=0
        self.db.execute('CREATE TABLE IF NOT EXISTS incidents(id INTEGER PRIMARY KEY,ts REAL,severity TEXT,code TEXT,detail TEXT)')
        self.db.commit()
        self.settings=cfg.get('operations',{})
        self.heartbeat=Path(self.settings.get('heartbeat_file','data/heartbeat.json'))
        self.last_tick=time.monotonic()
        self.max_lag=0
        self.feeds_unhealthy_since=None

    def observe(self,name,milliseconds):
        self.samples[name].append(float(milliseconds))

    def alert(self,code,detail,severity='warning'):
        now=time.time()
        if now-self.last_alert.get(code,0)<self.settings.get('alert_cooldown_s',60): return
        self.last_alert[code]=now
        with self.db:
            self.db.execute('INSERT INTO incidents(ts,severity,code,detail) VALUES(?,?,?,?)',(now,severity,code,detail))
        import logging
        logging.getLogger('operations').warning('%s: %s',code,detail)

    def tick(self,mode,feeds_healthy,recording_healthy,halted=None):
        now=time.monotonic()
        expected=self.cfg['scanner']['interval_ms']/1000
        lag=max(0,now-self.last_tick-expected)*1000
        self.observe('event_loop_lag_ms',lag);self.last_tick=now
        if feeds_healthy:
            self.feeds_unhealthy_since=None
        elif self.feeds_unhealthy_since is None:
            self.feeds_unhealthy_since=now
        unhealthy_s=now-self.feeds_unhealthy_since if self.feeds_unhealthy_since is not None else 0
        if unhealthy_s>self.settings.get('feed_outage_max_s',60):
            self.alert('feed_outage',f'No usable cross-venue route for {unhealthy_s:.0f} seconds','critical')
        if lag>self.settings.get('max_event_loop_lag_ms',1000):
            self.alert('loop_lag',f'Event loop delayed {lag:.0f} ms')
        if time.time()-self.last_heartbeat>=1:
            atomic_json(self.heartbeat,dict(run_id=self.run_id,pid=os.getpid(),ts=time.time(),mode=mode,
                status='running',feeds_healthy=feeds_healthy,recording_healthy=recording_healthy,halted=halted,
                feed_outage_s=unhealthy_s,feed_outage_max_s=self.settings.get('feed_outage_max_s',60)))
            self.last_heartbeat=time.time()
        if not recording_healthy: self.alert('recorder_unhealthy','Market recording is incomplete; new execution is blocked','critical')
        if halted: self.alert('engine_halted',halted,'critical')
        return lag

    def snapshot(self):
        latency={}
        for name,values in self.samples.items():
            ordered=sorted(values)
            if ordered:
                latency[name]=dict(count=len(ordered),p50=ordered[len(ordered)//2],p95=ordered[min(len(ordered)-1,int(len(ordered)*.95))],max=ordered[-1])
        incidents=[dict(ts=r[0],severity=r[1],code=r[2],detail=r[3]) for r in self.db.execute(
            'SELECT ts,severity,code,detail FROM incidents ORDER BY id DESC LIMIT 20')]
        return dict(run_id=self.run_id,latency_ms=latency,incidents=incidents,
                    disk_free_gb=shutil.disk_usage(self.heartbeat.parent).free/1024**3)

    def stopped(self):
        atomic_json(self.heartbeat,dict(run_id=self.run_id,pid=os.getpid(),ts=time.time(),status='stopped'))


def backup_database(source,destination):
    source=Path(source).resolve(); destination=Path(destination).resolve()
    if source==destination or destination.exists(): raise ValueError('Backup destination must be a new path')
    destination.parent.mkdir(parents=True,exist_ok=True)
    temporary=destination.with_suffix(destination.suffix+'.tmp')
    if temporary.exists(): raise ValueError('Incomplete backup already exists at destination')
    try:
        with sqlite3.connect(source.as_uri()+'?mode=ro',uri=True) as origin:
            with sqlite3.connect(temporary) as target:
                origin.backup(target,pages=256,sleep=.01)
                if target.execute('PRAGMA integrity_check').fetchone()[0]!='ok':
                    raise RuntimeError('Backup integrity check failed')
        os.replace(temporary,destination)
        os.chmod(destination,0o600)
        return dict(source=str(source),backup=str(destination),bytes=destination.stat().st_size,integrity='ok')
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def database_inventory(path):
    """Content digest and row counts, independent of SQLite page layout."""
    with sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True) as db:
        if db.execute('PRAGMA integrity_check').fetchone()[0]!='ok':
            raise ValueError('Database integrity check failed')
        tables={}
        for (name,) in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"):
            quoted='"'+name.replace('"','""')+'"'
            digest=hashlib.sha256();count=0
            for row in db.execute(f'SELECT * FROM {quoted} ORDER BY rowid'):
                digest.update(repr(row).encode());digest.update(b'\n');count+=1
            tables[name]=dict(rows=count,sha256=digest.hexdigest())
        return tables


def restore_drill(backup,destination):
    """Restore to a new path and compare every stored row; never touch production."""
    source_inventory=database_inventory(backup)
    result=backup_database(backup,destination)
    restored_inventory=database_inventory(destination)
    if source_inventory!=restored_inventory:
        raise RuntimeError('Restored content does not match backup')
    result.update(content_verified=True,tables=restored_inventory,production_modified=False)
    return result


def watchdog_check(heartbeat,now,max_age,expected_run=None):
    if expected_run and heartbeat.get('run_id')!=expected_run:
        return 'Heartbeat belongs to another engine run'
    if heartbeat.get('status')=='stopped': return None
    age=now-heartbeat.get('ts',0)
    if not 0<=age<=max_age: return 'Engine heartbeat expired or clock moved backwards'
    if not heartbeat.get('recording_healthy',True): return 'Market recorder is unhealthy'
    if heartbeat.get('feed_outage_s',0)>heartbeat.get('feed_outage_max_s',60):
        return 'Market feed outage exceeded limit'
    return None
