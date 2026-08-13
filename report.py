"""Edge report: replay recorded cross-venue spreads under fee scenarios.

    uv run report.py            # all recorded data
    uv run report.py --hours 24 # last 24h only

For each scenario it groups above-cost seconds into discrete windows
(one capture per window — you can't harvest the same dislocation twice)
and estimates daily P&L at your configured trade size.
Maker scenarios are an UPPER BOUND: they assume your resting order is
filled whenever the spread persisted, ignoring queue position.
"""
from __future__ import annotations

import argparse
import sqlite3
import time
from pathlib import Path

import yaml
from rich.console import Console
from rich.table import Table

ROOT = Path(__file__).resolve().parent
WINDOW_GAP_S = 3.0   # samples further apart than this start a new window


def scenarios(cfg: dict) -> dict[str, callable]:
    v = cfg["venues"]
    tk = {name: c["taker_fee"] for name, c in v.items()}
    mk = {name: c["maker_fee"] for name, c in v.items()}
    buf = 2 * cfg["scanner"]["slippage_buffer"]
    return {
        "taker-taker (your tier today)": lambda a, b: tk[a] + tk[b] + buf,
        "maker-taker (post-only entry)": lambda a, b: min(mk[a] + tk[b], tk[a] + mk[b]) + buf,
        "maker-maker (both post-only)":  lambda a, b: mk[a] + mk[b] + buf,
        "binance-pair taker (VPS, 0.10%/leg)": lambda a, b: 0.0020 + buf,
        "binance-pair maker (VPS, 0.075%/leg)": lambda a, b: 0.0015 + buf,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=float, default=None)
    args = ap.parse_args()

    cfg = yaml.safe_load(open(ROOT / "config" / "config.yaml"))
    notional = cfg["risk"]["trade_notional_usdt"]
    db = sqlite3.connect(ROOT / "data" / "trades.db")

    q = "SELECT ts, grp, buy_venue, sell_venue, gross FROM spreads"
    params: tuple = ()
    if args.hours:
        q += " WHERE ts >= ?"
        params = (time.time() - args.hours * 3600,)
    rows = db.execute(q + " ORDER BY grp, ts", params).fetchall()

    con = Console()
    if not rows:
        con.print("[yellow]No spread samples recorded yet — let the bot run a while first.[/]")
        return

    span_s = max(r[0] for r in rows) - min(r[0] for r in rows)
    span_h = max(span_s / 3600, 1 / 60)
    per_group: dict[str, list] = {}
    for r in rows:
        per_group.setdefault(r[1], []).append(r)

    con.print(f"\n[bold]Edge report[/] — {len(rows):,} samples over {span_h:.1f}h "
              f"across {len(per_group)} assets, trade size ${notional}\n")

    # observed spread stats per asset
    t = Table(title="Observed gross spreads (pre-fee, depth-walked)")
    for col in ("asset", "median", "p95", "max", "best route seen"):
        t.add_column(col, justify="right" if col != "best route seen" else "left")
    for grp, rs in sorted(per_group.items()):
        gs = sorted(r[4] for r in rs)
        best = max(rs, key=lambda r: r[4])
        t.add_row(grp, f"{gs[len(gs)//2]*100:+.4f}%", f"{gs[int(len(gs)*0.95)]*100:+.4f}%",
                  f"{gs[-1]*100:+.4f}%", f"buy {best[2]} -> sell {best[3]}")
    con.print(t)

    # fee scenarios
    t2 = Table(title="What each execution model would have captured")
    for col in ("scenario", "windows", "windows/day", "avg net edge", "est $/day"):
        t2.add_column(col, justify="right" if col != "scenario" else "left")
    verdicts = []
    for name, cost_fn in scenarios(cfg).items():
        windows = 0
        total_net = 0.0
        for grp, rs in per_group.items():
            in_window = False
            last_ts = 0.0
            win_best = 0.0
            for ts, _, bv, sv, gross in rs:
                net = gross - cost_fn(bv, sv)
                if net > 0:
                    if not in_window or ts - last_ts > WINDOW_GAP_S:
                        if in_window:
                            windows += 1
                            total_net += win_best
                        in_window, win_best = True, net
                    else:
                        win_best = max(win_best, net)
                    last_ts = ts
                elif in_window and ts - last_ts > WINDOW_GAP_S:
                    windows += 1
                    total_net += win_best
                    in_window = False
            if in_window:
                windows += 1
                total_net += win_best
        per_day = windows / span_h * 24
        dollars_day = total_net * notional / span_h * 24
        avg = (total_net / windows * 100) if windows else 0.0
        style = "green" if dollars_day > 1 else ("yellow" if windows else "dim")
        t2.add_row(name, f"{windows}", f"{per_day:.1f}", f"{avg:+.4f}%",
                   f"${dollars_day:,.2f}", style=style)
        verdicts.append((name, dollars_day, windows))
    con.print(t2)

    best = max(verdicts, key=lambda v: v[1])
    con.print("\n[bold]Verdict:[/]")
    if best[1] <= 0.5:
        con.print("  No execution model clears costs on this venue pair right now. "
                  "That is the honest answer — do not fund live trading on this setup.")
    else:
        con.print(f"  Best model: [bold]{best[0]}[/] ≈ [green]${best[1]:,.2f}/day[/] "
                  f"at ${notional} size ({best[2]} windows). Maker numbers are an upper "
                  f"bound (assume every resting order fills). Run longer before deciding.")
    con.print("  More data → better answer. 24h minimum, a week is meaningful.\n")


if __name__ == "__main__":
    main()
