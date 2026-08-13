# MT5 bridge (phase 2 — design)

MetaTrader can't join crypto cross-exchange arb directly: broker CFD quotes are
synthetic, brokers void "latency arb" trades, and the `MetaTrader5` Python
package is Windows-only. What *is* valuable is using this engine as a
**signal source** for the EAs already in `Production-Trading-Bot`.

## Architecture

```
arb-bot (this repo, macOS)          MT5 terminal (Windows VPS or broker)
┌──────────────────────────────┐           ┌──────────────────────────────┐
│ engine.py                    │  HTTP GET │ SmartStockTrader.mq5         │
│  └─ /api/signals endpoint ───┼──────────▶│  └─ WebRequest() poll (1–5s) │
│     crypto momentum/regime   │   JSON    │     gates entries by signal  │
└──────────────────────────────┘           └──────────────────────────────┘
```

1. **Engine side** — add a `/api/signals` endpoint exposing digested state:
   per-asset direction, cross-venue spread z-score, volatility regime.
   (~30 lines in `src/dashboard.py`.)
2. **EA side** — the existing EAs already use `WebRequest`-style polling for
   the backend; point them at this engine's endpoint and treat the signal as
   an entry filter (e.g. only long BTC-correlated symbols when regime=risk-on).
3. **Transport** — if the MT5 terminal is on a VPS, expose the endpoint via a
   Cloudflare tunnel or Tailscale, never a raw open port.

## Why signals and not execution

- MT5 brokers quote their own book; there is no cross-venue price to capture.
- Round-trip broker spread + commission on crypto CFDs (often 0.1–0.3%)
  exceeds typical arb edges on its own.
- Signal-gating your existing momentum EAs is where the crypto engine's
  real-time information actually adds value.

Say the word when paper mode has run a while and we'll wire this.
