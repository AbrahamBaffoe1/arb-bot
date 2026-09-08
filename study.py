"""Run a fixed future-data study. Never connects to private APIs or submits orders."""
import argparse
import json
import time
from pathlib import Path
from src.configuration import load_config,ROOT
from src.locking import ProcessLock
from src.operations import atomic_json
from src.study import initialize_study,evaluate_study


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command',choices=['init','status','worker'])
    parser.add_argument('--directory',type=Path,default=ROOT/'data/research/future-study-001')
    parser.add_argument('--config',type=Path)
    args=parser.parse_args()
    cfg=load_config(args.config)
    if args.command=='init':
        print(json.dumps(initialize_study(cfg,args.directory),indent=2));return
    if args.command=='status':
        path=args.directory/'status.json'
        print(path.read_text() if path.exists() else json.dumps({'phase':'worker_not_started','complete':False}));return
    with ProcessLock(args.directory/'worker.lock'):
        while True:
            try:
                status=evaluate_study(load_config(args.config),args.directory)
                print(json.dumps(status),flush=True)
            except Exception as exc:
                atomic_json(args.directory/'status.json',dict(phase='error',checked_at=time.time(),complete=False,live_ready=False,reason=f'{type(exc).__name__}: {exc}'))
            time.sleep(30)


if __name__=='__main__':main()
