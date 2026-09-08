"""Unattended public-data entrypoint. This service cannot authorize live trading."""
import asyncio
import argparse
from src.configuration import load_config,ROOT
from src.locking import ProcessLock
from main import run


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--config',type=__import__('pathlib').Path,default=ROOT/'config/config.yaml')
    args=parser.parse_args()
    if load_config(args.config)['mode']!='paper':
        raise SystemExit('The unattended collector only permits mode: paper')
    args.enable_live=False
    args.preflight=False
    args.duration=None
    args.no_dashboard=False
    with ProcessLock(ROOT/'data/engine.lock'):
        asyncio.run(run(args))


if __name__=='__main__':
    try:main()
    except KeyboardInterrupt:pass
