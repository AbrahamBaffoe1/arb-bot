"""Observed opportunities and funding readiness. Never extrapolates guaranteed income."""
from __future__ import annotations
import argparse
import json
import sqlite3
import time
from pathlib import Path
from rich.console import Console
from rich.table import Table
from src.configuration import fingerprint, load_config
from src.evidence import Evidence


def read_report(cfg, hours=None):
    path = Path(cfg['storage']['db_path'])
    if not path.exists():
        return dict(ready=False, status='collecting_evidence', message='No observations yet. Do not fund trading.', blockers=['Run main.py to collect live market evidence.'])
    db = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)
    try:
        row = db.execute('SELECT fingerprint FROM paper_state WHERE id=1').fetchone()
        if not row or row[0] != fingerprint(cfg):
            return dict(ready=False, status='configuration_changed', message='Evidence does not match current costs and risk settings. Start a new evaluation database.', blockers=['Configuration fingerprint mismatch'])
        # Reading a report must not expire active observations or mutate a running engine.
        evidence = object.__new__(Evidence)
        evidence.cfg, evidence.db = cfg, db
        halt = 'kill switch file present' if Path(cfg['risk']['kill_switch_file']).exists() else None
        report = evidence.funding(halt)
        since = time.time() - hours * 3600 if hours is not None else 0
        report['recent_windows'] = [dict(ts=r[0], route=r[1], status=r[2], reason=r[3], pnl=r[4]) for r in db.execute(
            'SELECT started,route,status,reason,pnl FROM windows WHERE started>=? ORDER BY started DESC LIMIT 25', (since,))]
        report['window_filter_hours'] = hours
        return report
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=Path)
    parser.add_argument('--hours', type=float, help='filter recent windows only; funding uses all matching evidence')
    parser.add_argument('--json', action='store_true')
    args = parser.parse_args()
    if args.hours is not None and args.hours <= 0:
        parser.error('--hours must be positive')
    report = read_report(load_config(args.config), args.hours)
    if args.json:
        print(json.dumps(report, indent=2))
        return
    con = Console()
    con.print(report['message'], style='green' if report['ready'] else 'yellow', markup=False)
    if 'observation_hours' in report:
        con.print(f"Healthy observation hours: {report['observation_hours']:.2f} | Independent paper fills: {report['independent_paper_fills']} | Net simulated P&L: {report['net_paper_pnl_usdt']:+.4f} USDT")
    for blocker in report['blockers']:
        con.print(f'• {blocker}', markup=False)
    if report.get('proposal'):
        con.print('Indicative pilot funding (historical quote; reprice before trading):')
        con.print_json(data=report['proposal'])
    table = Table(title='Recent observed windows (no daily income extrapolation)')
    for title in ('UTC', 'Route', 'Outcome', 'Reason', 'Simulated P&L'):
        table.add_column(title)
    for row in report.get('recent_windows', []):
        table.add_row(time.strftime('%m-%d %H:%M:%S', time.gmtime(row['ts'])), row['route'], row['status'], row['reason'],
                      '—' if row['pnl'] is None else f"{row['pnl']:+.4f}")
    con.print(table)
    if report.get('limitations'):
        con.print(report['limitations'], markup=False)


if __name__ == '__main__':
    main()
