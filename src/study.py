"""Preregistered future-data study. No trade execution and no automatic promotion."""
import hashlib
import json
import sqlite3
import time
import zlib
import os
from pathlib import Path
from .configuration import ROOT,fingerprint
from .operations import atomic_json
from .replay import MakerReplay


def code_identity():
    digest=hashlib.sha256()
    paths=sorted((ROOT/'src').glob('*.py'))+[ROOT/'study.py',ROOT/'uv.lock']
    for path in paths:
        digest.update(path.name.encode());digest.update(path.read_bytes())
    return digest.hexdigest()


def initialize_study(cfg,directory,now=None,training_hours=24,validation_hours=72):
    if training_hours<=0 or validation_hours<=0:
        raise ValueError('Study periods must be positive')
    directory=Path(directory)
    directory.mkdir(parents=True,exist_ok=False)
    now=time.time() if now is None else now
    protocol=dict(version=1,created=now,start=now,training_end=now+training_hours*3600,
        validation_end=now+(training_hours+validation_hours)*3600,
        configuration_identity=fingerprint(cfg),code_identity=code_identity(),
        parameters=dict(entry_delay_ms=200,hedge_delay_ms=300,quote_lifetime_ms=2000,
            cancel_delay_ms=200,queue_multiplier=1,depth_fraction=1,min_edge=cfg['scanner']['min_net_edge']),
        stress=dict(hedge_delay_ms=1000,queue_multiplier=2,depth_fraction=.5),
        minimum_healthy_fraction=.95,minimum_validation_maker_fills=100,
        method='Parameters fixed before collection. First period is diagnostic; the later period is held out. No automatic tuning or live promotion.',
        cfg=cfg)
    atomic_json(directory/'protocol.json',protocol)
    return protocol


def phase(protocol,latest_received,now):
    if now<protocol['training_end'] or latest_received<protocol['training_end']:
        return 'collecting_training'
    if now<protocol['validation_end'] or latest_received<protocol['validation_end']:
        return 'collecting_validation'
    return 'ready_to_evaluate'


def period_replay(path,protocol,start,end,highwater,parameters):
    sim=MakerReplay(protocol['cfg'],**parameters)
    healthy_s=0;last_scan=None
    with sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True) as db:
        identities={r[0] for r in db.execute('SELECT DISTINCT s.identity FROM events e JOIN sessions s ON e.session=s.id WHERE e.id<=? AND e.received>=? AND e.received<?',
                                            (highwater,start,end))}
        if identities!={protocol['configuration_identity']}:
            raise ValueError('Study period has missing or mixed economic configurations')
        def decode(row):
            body=zlib.decompress(row[6]).decode() if isinstance(row[6],bytes) else row[6]
            return dict(id=row[0],received=row[1],source_ts=row[2],kind=row[3],venue=row[4],symbol=row[5],body=json.loads(body))
        # Only metadata from before the window is allowed as warm-up.
        for row in db.execute("SELECT id,received,source_ts,kind,venue,symbol,body FROM events WHERE id<=? AND received<? AND kind='markets' ORDER BY id",(highwater,start)):
            sim.consume(decode(row),allow_quotes=False)
        sim.first=sim.last=None
        for row in db.execute('SELECT id,received,source_ts,kind,venue,symbol,body FROM events WHERE id<=? AND received>=? AND received<? ORDER BY id',(highwater,start,end)):
            event=decode(row)
            if event['kind']=='session_start':
                # A collector restart is a disconnect, not proof resting orders vanished.
                for venue in list(sim.balances):
                    sim.consume({**event,'kind':'feed_error','venue':venue,'symbol':''},allow_quotes=False)
            sim.consume(event)
            if event['kind']=='scan':
                healthy=bool(event['body'].get('candidates')) and event['body'].get('fresh_inventory',False)
                if healthy and last_scan is not None:
                    elapsed=event['received']-last_scan
                    if 0<=elapsed<=2:healthy_s+=elapsed
                last_scan=event['received'] if healthy else None
    result=sim.report()
    result.update(period_start=start,period_end=end,highwater_event_id=highwater,
        healthy_seconds=healthy_s,healthy_fraction=healthy_s/(end-start),parameters=parameters)
    return result,sim.log


def evaluate_study(cfg,directory,now=None):
    directory=Path(directory)
    protocol=json.loads((directory/'protocol.json').read_text())
    now=time.time() if now is None else now
    status=dict(checked_at=now,complete=False,live_ready=False,protocol=str(directory/'protocol.json'),
                training_end=protocol['training_end'],validation_end=protocol['validation_end'])
    if fingerprint(cfg)!=protocol['configuration_identity'] or code_identity()!=protocol['code_identity']:
        status.update(phase='blocked',reason='Code or economics changed after preregistration; create a separate study')
    else:
        path=Path(cfg['recording']['db_path'])
        if not path.exists():
            status.update(phase='waiting_for_recorder')
        else:
            with sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True) as db:
                highwater,latest=db.execute('SELECT id,received FROM events ORDER BY id DESC LIMIT 1').fetchone() or (0,0)
            status.update(phase=phase(protocol,latest or 0,now),latest_received=latest)
            if status['phase']!='collecting_training':
                if not (directory/'training.json').exists():
                    report,_=period_replay(path,protocol,protocol['start'],protocol['training_end'],highwater,protocol['parameters'])
                    atomic_json(directory/'training.json',report)
            if status['phase']=='ready_to_evaluate':
                if not (directory/'validation.json').exists():
                    report,log=period_replay(path,protocol,protocol['training_end'],protocol['validation_end'],highwater,protocol['parameters'])
                    stress,_=period_replay(path,protocol,protocol['training_end'],protocol['validation_end'],highwater,{**protocol['parameters'],**protocol['stress']})
                    blockers=[]
                    for name,result in [('validation',report),('stress',stress)]:
                        if result['healthy_fraction']<protocol['minimum_healthy_fraction']:blockers.append(name+': insufficient healthy coverage')
                        if not result['data_integrity_ok'] or result['stopped']:blockers.append(name+': integrity or risk halt')
                        if result['open_quotes'] or result['pending_hedges'] or result['unhedged_usdt']>=.01:blockers.append(name+': unfinished exposure')
                        if result['maker_fill_events']<protocol['minimum_validation_maker_fills']:blockers.append(name+': insufficient fills')
                        if result['net_pnl_usdt']<=0:blockers.append(name+': no positive net return')
                    temporary=directory/'validation-decisions.jsonl.tmp'
                    with temporary.open('w') as f:
                        for event in log:f.write(json.dumps(event)+'\n')
                        f.flush();os.fsync(f.fileno())
                    os.replace(temporary,directory/'validation-decisions.jsonl')
                    atomic_json(directory/'validation.json',dict(base=report,stress=stress,blockers=blockers,
                        statistically_validated=False,live_ready=False,
                        verdict='Candidate for further research' if not blockers else 'Strategy not established'))
                status.update(phase='evaluation_finished',complete=True,
                    result=str(directory/'validation.json'),meaning='The scheduled experiment finished; the six-part project and live readiness are not certified')
    atomic_json(directory/'status.json',status)
    return status
