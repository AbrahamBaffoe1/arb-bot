"""Run public-data evaluation, account preflight, or an explicitly enabled live pilot."""
from __future__ import annotations
import argparse
import asyncio
import json
import logging
import time
from pathlib import Path
import uvicorn
from dotenv import load_dotenv
from rich.logging import RichHandler
from src.configuration import ROOT, load_config
from src.dashboard import create_app
from src.engine import Engine
from src.locking import ProcessLock


def arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=Path)
    parser.add_argument('--duration', type=float, help='stop after this many seconds')
    parser.add_argument('--no-dashboard', action='store_true')
    parser.add_argument('--enable-live', action='store_true', help='explicitly authorize configured live pilot orders')
    parser.add_argument('--preflight', action='store_true', help='read account fees and balances; places no orders')
    args = parser.parse_args()
    if args.duration is not None and args.duration <= 0:
        parser.error('--duration must be positive')
    return args


async def run(args):
    load_dotenv(ROOT / '.env')
    logging.basicConfig(level=logging.INFO, format='%(message)s', handlers=[RichHandler()])
    logging.getLogger('uvicorn.access').setLevel(logging.WARNING)
    cfg = load_config(args.config)
    if args.preflight:
        from src.economics import AccountInspector,economics_report,save_receipts
        from src.paper import PaperExecutor
        from src.evidence import Evidence
        inspector=AccountInspector(cfg)
        try:
            accounts=await inspector.inspect()
            paper = PaperExecutor(cfg)
            try:
                Evidence(cfg, paper.db)
                save_receipts(paper.db,cfg,accounts)
            finally:
                paper.close()
            print(json.dumps(dict(economics=economics_report(cfg,accounts),orders_submitted=False),indent=2))
        finally:
            await inspector.close()
        return
    if cfg['mode'] == 'live' and not args.enable_live:
        raise SystemExit('Live orders require mode: live AND --enable-live, plus passing funding evidence and account preflight.')
    if args.enable_live and cfg['mode'] != 'live':
        raise SystemExit('--enable-live requires mode: live; current configuration is paper.')
    engine = Engine(cfg)
    tasks = []
    server = None
    try:
        tasks.append(asyncio.create_task(engine.run()))
        if not args.no_dashboard:
            server = uvicorn.Server(uvicorn.Config(create_app(engine), host=cfg['dashboard']['host'],
                                                   port=cfg['dashboard']['port'], log_level='warning'))
            tasks.append(asyncio.create_task(server.serve()))
            logging.info('Dashboard: http://%s:%s', cfg['dashboard']['host'], cfg['dashboard']['port'])
        done, _ = await asyncio.wait(tasks, timeout=args.duration, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()
        if args.duration:
            snapshot = engine.snapshot()
            print(json.dumps({k: snapshot[k] for k in ('scans', 'venue_status', 'route_estimates', 'funding','recording','operations','portfolio')}, indent=2))
    finally:
        if server:
            server.should_exit = True
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await engine.close()


async def main():
    args = arguments()
    with ProcessLock(ROOT / 'data/engine.lock'):
        await run(args)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except (ValueError, RuntimeError) as exc:
        raise SystemExit(str(exc))
