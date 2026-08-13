# arb-bot

Multi-venue crypto arbitrage engine — cross-exchange + triangular — built
paper-first with **honest fee and slippage accounting**. Live websocket market
data from Kraken and Coinbase (Binance auto-disables on US geo-block), a risk
gate in front of every trade, and a dark dashboard made for an iPad on your desk.

> **Reality check, up front.** The viral "$68 → $750K" posts are engagement
> bait — their own screenshots say *SIMULATED*. Real cross-venue arb edges are
> 0.05–0.3% and get eaten by taker fees (Kraken 0.40% + Coinbase 0.60% at
> starter tiers = 1.0% round trip). This engine will show you **zero fills** on
> those venues at starter fee tiers, because that is the truth. The path to
> real fills is: volume-tier fee discounts, maker (post-only) execution, or a
> venue pair with tighter fees. The engine exists so you can *measure* the edge
> instead of believing a tweet.

## Run it (no API keys needed)

```bash
cd ~/Development/arb-bot
uv sync          # once
uv run main.py
```

- Dashboard: **http://localhost:8420**
- iPad on the same Wi-Fi: `http://<your-Mac-LAN-IP>:8420`
  (find it: `ipconfig getifaddr en0`) — Add to Home Screen for full-screen mode.
- Instant halt: `touch data/KILL`

## What it does

```
websocket books (kraken, coinbase, [binance])
        │ top-10 depth, per-symbol watch loops
        ▼
   BookStore (shared cache, staleness-checked)
        │ every 200ms
        ▼
 CrossExchangeScanner ──┐   walks REAL book depth for the trade size,
 TriangularScanner ─────┤   subtracts taker fees on EVERY leg,
                        │   subtracts a slippage buffer on every leg
                        ▼
                   RiskManager    daily loss cap · inventory skew limit ·
                        │         per-group cooldown · kill-switch file
                        ▼
                  PaperExecutor   fills at book prices, tracks per-venue
                        │         balances, logs every fill to SQLite
                        ▼
                FastAPI dashboard (1s websocket push)
```

- **Cross-exchange**: same asset, USDT-quoted on 2+ venues. Assumes inventory
  pre-positioned on both sides (how real desks do it — no per-trade transfers).
- **Triangular**: USDT → A → B → USDT loops on a single venue, both directions.

## Config knobs — `config/config.yaml`

| Key | Meaning |
|---|---|
| `scanner.min_net_edge` | edge required AFTER all costs (default 0.05%) |
| `risk.trade_notional_usdt` | size per attempt |
| `risk.max_daily_loss_usdt` | halts the engine when hit |
| `venues.*.taker_fee` | **update these to YOUR fee tier** — the whole result depends on them |
| `arb_groups` / `triangles` | what gets scanned |

## Trade log

Every fill lands in `data/trades.db` (SQLite):

```bash
sqlite3 data/trades.db "SELECT datetime(ts,'unixepoch'), kind, detail, pnl FROM fills ORDER BY ts DESC LIMIT 20;"
```

## Going live (deliberately not wired yet)

`mode: live` currently refuses to start. The sequence that earns it:

1. Run paper for ≥ 2 weeks. Look at the fill count and net P&L.
2. If paper shows a real edge at **your actual fee tier**, we wire a
   `LiveExecutor` behind the same risk gate (trade-only API keys, never
   withdrawal permission).
3. Start with `trade_notional_usdt: 25` and a `max_daily_loss_usdt` you can
   genuinely shrug at.

## Roadmap

- [ ] Maker-side execution (post-only limit orders) — kills the taker-fee wall
- [ ] Funding-rate / basis capture module (needs a derivatives venue)
- [ ] MT5 signal bridge to the existing EAs — see `docs/MT5_BRIDGE.md`
- [ ] Binance via non-US VPS (re-enable in config; engine already supports it)
- [ ] Latency histograms + opportunity half-life stats (how fast do edges close?)

## Layout

```
main.py            entrypoint
config/config.yaml everything tunable
src/feeds.py       ccxt.pro websocket book feeds (self-healing)
src/scanners.py    cross-exchange + triangular scanners
src/paper.py       paper executor + SQLite trade log
src/risk.py        risk gate + kill switch
src/engine.py      orchestrator + dashboard snapshot
src/dashboard.py   FastAPI + websocket server
dashboard/         single-file iPad UI
data/              trades.db, KILL switch
```
