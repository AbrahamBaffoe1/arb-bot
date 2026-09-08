"""Research and operations CLI. All subcommands are read-only with respect to exchanges."""
from __future__ import annotations
import argparse
import asyncio
import json
import sqlite3
import time
from pathlib import Path
from dotenv import load_dotenv
from src.configuration import ROOT,load_config
from src.economics import AccountInspector,economics_report,save_receipts
from src.locking import ProcessLock
from src.operations import backup_database,atomic_json,restore_drill
from src.recorder import sessions
from src.replay import replay,walk_forward


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',type=Path)
    sub=parser.add_subparsers(dest='command',required=True)
    economics=sub.add_parser('economics');economics.add_argument('--accounts',action='store_true');economics.add_argument('--save-receipts',action='store_true')
    sub.add_parser('discover')
    sub.add_parser('sessions')
    for command in ('replay','walk-forward'):
        p=sub.add_parser(command);p.add_argument('--session',required=True);p.add_argument('--output',type=Path,required=True)
        if command=='replay':
            p.add_argument('--entry-delay-ms',type=float,default=200)
            p.add_argument('--hedge-delay-ms',type=float,default=300)
            p.add_argument('--queue-multiplier',type=float,default=1)
            p.add_argument('--depth-fraction',type=float,default=1)
    backup=sub.add_parser('backup');backup.add_argument('--output',type=Path,required=True)
    restore=sub.add_parser('restore-drill');restore.add_argument('--backup',type=Path,required=True);restore.add_argument('--output',type=Path,required=True)
    sub.add_parser('recovery-audit')
    service=sub.add_parser('service-files');service.add_argument('--output',type=Path,default=ROOT/'deploy')
    args=parser.parse_args()
    cfg=load_config(args.config)
    load_dotenv(ROOT/'.env')
    if args.command=='economics':
        if args.save_receipts and not args.accounts:parser.error('--save-receipts requires --accounts')
        async def inspect():
            inspector=AccountInspector(cfg)
            try:return await inspector.inspect()
            finally:await inspector.close()
        accounts=asyncio.run(inspect()) if args.accounts else None
        report=economics_report(cfg,accounts)
        if args.save_receipts:
            with ProcessLock(ROOT/'data/engine.lock'):
                from src.paper import PaperExecutor
                paper=PaperExecutor(cfg)
                try:save_receipts(paper.db,cfg,accounts)
                finally:paper.close()
            report['receipts_saved']=True
        print(json.dumps(report,indent=2))
    elif args.command=='discover':
        from src.discovery import discover
        print(json.dumps(asyncio.run(discover(cfg)),indent=2))
    elif args.command=='sessions':
        print(json.dumps(sessions(cfg['recording']['db_path']),indent=2))
    elif args.command in ('replay','walk-forward'):
        if args.output.exists():raise ValueError('Output already exists; choose a new directory')
        if args.command=='walk-forward': report,events=walk_forward(cfg['recording']['db_path'],args.session)
        else:
            report,events=replay(cfg['recording']['db_path'],args.session,dict(entry_delay_ms=args.entry_delay_ms,
                hedge_delay_ms=args.hedge_delay_ms,queue_multiplier=args.queue_multiplier,depth_fraction=args.depth_fraction))
        args.output.mkdir(parents=True)
        atomic_json(args.output/'report.json',report)
        with (args.output/'decisions.jsonl').open('w') as f:
            for event in events:f.write(json.dumps(event)+'\n')
        print(json.dumps(report,indent=2))
    elif args.command=='backup':
        reports=[]
        for key in ('db_path','live_db_path'):
            path=Path(cfg['storage'][key])
            if path.exists():reports.append(backup_database(path,args.output/path.name))
        path=Path(cfg['recording']['db_path'])
        if path.exists():reports.append(backup_database(path,args.output/path.name))
        print(json.dumps(reports,indent=2))
    elif args.command=='restore-drill':
        result=restore_drill(args.backup,args.output)
        atomic_json(args.output.with_suffix('.restore.json'),result)
        print(json.dumps(result,indent=2))
    elif args.command=='recovery-audit':
        from src.live import LiveExecutor
        async def audit():
            ex=LiveExecutor(cfg)
            try:
                await ex.initialize(establish_baseline=False,recovery=True)
                return await ex.ledger.audit(ex.exchanges)
            finally:await ex.aclose()
        with ProcessLock(ROOT/'data/engine.lock'):
            print(json.dumps(asyncio.run(audit()),indent=2))
    elif args.command=='service-files':
        from src.service import service_manifests
        paths=service_manifests(args.config or ROOT/'config/config.yaml',ROOT/'data/research/future-study-001',args.output)
        print(json.dumps({'launchd_manifests':[str(p) for p in paths],'installed':False,'live_enabled':False},indent=2))


if __name__=='__main__':
    try:main()
    except (ValueError,RuntimeError,sqlite3.Error) as exc:raise SystemExit(str(exc))
