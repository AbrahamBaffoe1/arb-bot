# Coinbase + Kraken arbitrage engine

A spot opportunity engine using real public websocket order books from Coinbase Advanced and Kraken. It evaluates executable depth at several sizes and charges configured taker fees, per-leg slippage and a rebalancing reserve. It can submit live IOC limit orders after evidence and account checks pass. Profitability and live fills are **not proven** by the software tests.

## Run

```bash
uv sync --frozen
uv run main.py
```

Dashboard: **http://127.0.0.1:8421**. It shows current after-cost routes (including negative edges), simulated account equity, independent opportunity outcomes, and funding blockers. By default it runs in paper mode with real market data and requires no keys. Stop with Ctrl-C. Run a bounded data check with:

```bash
uv run main.py --no-dashboard --duration 60
uv run report.py
uv run report.py --json
uv run report.py --hours 24
```

`--hours` filters the recent window list; funding readiness always uses all evidence for the matching economic configuration. The report never projects daily income from hypothetical maker fills. Existing `data/trades.db` and earlier test databases remain untouched; this version uses `data/research-v2.db`, preserving earlier `data/opportunities.db` evidence.

## Pricing and evidence

- Coinbase and Kraken market metadata must confirm active spot markets. Quantity precision and amount/cost minimums are applied before a route is considered.
- Both sides must have enough depth. Books with invalid prices, crossed sides, old source timestamps or excessive arrival-time skew are rejected. Missing exchange timestamps are tolerated but local arrival freshness is still required.
- Cross-venue routes use the **same base and USDT quote**; USD/USDC/USDT are not treated as equivalent. USDT-denominated results are not USD cash balances.
- A signal must remain above the thresholds for at least 600 ms and receive new updates on both books. The original size is rechecked; one attempt is allowed per continuous opportunity window.
- Paper inventory is bought from the starting cash budget, with fees and depth costs. No negative cash, free inventory or automatic transfers. Baselines, balances, costs, fills, windows and healthy observation time persist in SQLite. Offline time does not count.
- Paper fills simulate all legs together. They do **not** model queue priority, actual fill probability or the timing of individual live fills. Book depth can disappear before an order reaches the exchange.
- Funding review requires 72 healthy observation hours, 100 independent paper fills on seven UTC days, at least 10 USDT net simulated profit after initial inventory costs, nonnegative marked equity relative to initial capital, recent evidence and account fee verification. These configurable thresholds are screening rules, not proof of future profit.
- Fee, size, market or risk changes require a new `storage.db_path` (and new live DB if live economics changed), preserving previous evidence separately. Fee-verification receipts are stored in the matching paper DB.

## Connect your accounts

Put read-only API credentials in a local `.env` using `.env.example` for account research. Trade permission is needed only for explicitly enabled live orders and cancellation. Do not paste secrets in chat or commit them. Use Coinbase Advanced CDP credentials and Kraken Spot API credentials with balance, order and trade-history access; do not enable withdrawals. For Coinbase, `COINBASE_SECRET` may contain a PEM key with literal `\n` escapes.

```bash
uv run main.py --preflight
```

This command reads actual account fees and available balances, reads public depth for inventory capacity, and records successful fee verification for the current evidence configuration. It submits **no orders**, does not allocate funds, and does not establish a trading baseline. The default Coinbase 1.2% and Kraken 0.4% taker settings are conservative working estimates, **not your verified fee tiers**. If your actual fee is higher, preflight fails and reports it; update the configuration and start a new evaluation DB. You may set lower verified fees before collecting new evidence. Preflight is repeated at live startup and fees are checked every 15 minutes during live operation.

The dashboard and report ask for funding review only when all evidence gates pass. The proposal lists indicative USDT and base-asset balances for a recent route, with reserves. Fund **your own exchange accounts**, and recheck the current opportunity: the proposal is historical and expires as evidence ages. There is no deposit address or money transfer to this assistant.

## Live pilot

Only after reviewing evidence and account readiness, set `mode: live` and run:

```bash
uv run main.py --enable-live
```

Both settings are required. Defaults cap an attempt at 25 USDT and daily strategy/marked-equity losses at 10 USDT. Live mode supports cross-venue spot routes; triangular scanning is available for paper research only and is off by default. Use accounts/portfolios dedicated to the pilot so external trades and transfers do not distort its inventory and daily-equity baseline.

The executor refreshes available balances, rejects expired quotes, checks projected inventory drift and tests profitability at the worst IOC limit prices. It durably writes both client order IDs before submitting the legs concurrently. It never retries order submission after an uncertain response. Final order status, filled quantities and actual quote-currency fees must reconcile before a fill is credited. Balanced partial fills can settle; unequal fills, missing fees, unknown submissions, invalid responses or delayed terminal status latch a persistent halt. No automatic emergency market order is sent.

**Cross-exchange execution is not atomic.** One leg can fill and the other fail. The daily cap stops subsequent orders; it cannot bound losses from an already-submitted order, exchange outage, asset move or unmatched inventory. Real authenticated trading has not been exercised by the public-data smoke tests.

## Halt and recovery

```bash
touch data/KILL
```

The kill switch prevents new attempts and is checked again before live submission. It cannot recall an in-flight order. Remove it only when ready to resume. A workspace process lock prevents overlapping CLI engine/preflight processes; a crash releases the OS lock automatically.

For a persistent live halt, stop the engine and inspect `data/live.db` tables `live_orders`, `live_halt` and `fills`, then reconcile every client/exchange order ID and the balances in both exchange accounts. Unknown submissions must be investigated by client ID; do not blindly resubmit. Resolve/cancel any remaining orders in the exchange UI. Do not delete or mark journal rows settled to bypass reconciliation. This version intentionally provides no automatic restart or unhalt for uncertain orders; recovery needs an operator audit and a reviewed state repair.

Paper account initial costs and completed trade P&L are persisted. Marked equity includes changing inventory prices. Live strategy P&L deducts an estimated rebalancing reserve; that reserve is accounting, not an exchange debit. Displayed equity uses the last available mark and does not assume stale marks are current for risk approval. External account activity can affect account equity.

## Validation

```bash
uv run python -m unittest discover -s tests -v
```

Tests cover depth, fees, precision, stale/invalid books, quote mismatch, cash constraints, restart persistence, inventory limits, confirmation windows, funding gates, process exclusion, and mocked live settlement/partial-fill/timeout failures. Unit-test market fixtures are confined to `tests/`; the running engine only uses exchange feeds.

API references: [CCXT manual](https://docs.ccxt.com/docs/manual), [Coinbase order API](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/rest-api/orders/create-order), [Coinbase fees](https://help.coinbase.com/en/coinbase/trading-and-funding/advanced-trade/advanced-trade-fees), [Kraken IOC examples](https://support.kraken.com/articles/360000920786-examples-of-placing-orders-with-different-parameters), [Kraken fees](https://www.kraken.com/features/fee-schedule).

## Six-part research upgrade

See [the research and operations runbook](docs/RESEARCH_OPERATIONS.md) for account economics, recorder/replay, maker-entry research, recovery auditing, portfolio risk and service controls. Start with `uv run research.py economics` and `uv run research.py sessions`. Actual account costs and maker profitability remain unverified; live trading is disabled by default.
