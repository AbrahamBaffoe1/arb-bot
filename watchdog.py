"""Independent process: latch KILL if the engine heartbeat expires. Never places trades."""
import argparse
import json
import logging
import time
from pathlib import Path
from src.operations import watchdog_check,atomic_json


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--heartbeat',required=True,type=Path)
    p.add_argument('--kill-file',required=True,type=Path)
    p.add_argument('--run-id',required=True)
    p.add_argument('--max-age',type=float,default=10)
    p.add_argument('--startup-grace',type=float,default=30)
    args=p.parse_args()
    start=time.monotonic()
    while True:
        try:
            heartbeat=json.loads(args.heartbeat.read_text())
            if heartbeat.get('run_id')!=args.run_id and time.monotonic()-start<args.startup_grace:
                time.sleep(1);continue
            if heartbeat.get('status')=='stopped' and heartbeat.get('run_id')==args.run_id:return
            reason=watchdog_check(heartbeat,time.time(),args.max_age,args.run_id)
        except (OSError,ValueError):
            if time.monotonic()-start<args.startup_grace:
                time.sleep(1);continue
            reason='Engine heartbeat missing or unreadable'
        if reason:
            args.kill_file.parent.mkdir(parents=True,exist_ok=True)
            args.kill_file.touch(exist_ok=True)
            atomic_json(args.heartbeat.with_name('watchdog-alert.json'),dict(ts=time.time(),run_id=args.run_id,reason=reason))
            logging.critical('%s; new trading latched off',reason)
            return
        time.sleep(1)


if __name__=='__main__':main()
