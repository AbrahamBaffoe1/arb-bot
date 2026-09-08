# Research and operations runbook

Live trading stays disabled (`mode: paper`). Maker entry followed by a hedge is a replay strategy only. The existing live executor uses concurrent IOC orders; replay results cannot authorize a maker pilot or prove that those IOC orders will settle together.

## 1. Account economics

Use separate read-only keys with balance and order/trade-history access in local `.env`. Do not enable withdrawals. No credential values are written to the event tape.

```bash
uv run research.py economics
uv run research.py economics --accounts
uv run research.py economics --accounts --save-receipts
```

The first command uses configured estimates. The second reads actual maker/taker fees, total/free balances, and public depth, and reports available hedge inventory, cash-limited capacity, indicative profit, and operating-cost sensitivity at 1, 10, and 100 completed hedges per day. These turnover values are scenarios, not forecasts. Snapshots are sequential and indicative, not execution quotes. Capacity still requires precision/minimum checks at execution. Missing keys block account verification. Saving receipts requires stopping the collector because it writes to its evidence database.

Set `economics.operating_cost_usdt_per_day` and `scanner.rebalance_buffer` from measured bills and transfer/conversion expenses. Zero operating cost is an unverified assumption. Rebalancing is a reserve, not an automatic transfer. Costs remain explicitly unverified in reports; balance and fee verification does not certify transfer costs or profitability. Changing economic settings requires a new evidence DB and recording session.

Coinbase determines fees from the tier when an order is placed, so receipts are historical snapshots. Live startup and periodic checks reject fees above modeled costs; settlement requires actual fees. [Coinbase fee documentation](https://help.coinbase.com/en/coinbase/trading-and-funding/advanced-trade/advanced-trade-fees).

## 2. Recorder and replay

The collector records normalized top-of-book-depth snapshots on feed updates, public trades, exchange and local timestamps, scan outcomes, execution decisions, metadata, and feed errors. It does not archive raw websocket frames or every exchange-internal update. Kraken checksum validation uses the local precision-preserving CCXT adapter and is tested against Kraken’s published CRC32 example. Recording is compressed and bounded by queue and disk budgets. A dropped event or storage failure blocks new execution; never erase an old tape to clear the error.

```bash
uv run research.py sessions
# Stop the collector gracefully to close its recording session before walk-forward.
uv run research.py replay --session SESSION_ID --output data/research/replay-001
uv run research.py walk-forward --session SESSION_ID --output data/research/walk-forward-001
```

Each output directory contains `report.json` and `decisions.jsonl`. Replay models entry/hedge/cancel delays, visible queue ahead, partial fills, hedge depth consumption, precision, exchange minimums, inventory/cash reservation, fees, slippage, drawdown, and operating costs. Only actual simulated maker fills schedule hedges. Public trade sides are normalized to the taker/aggressor side; legacy Coinbase tape rows are interpreted as maker-side observations. [Coinbase market-trade side semantics](https://docs.cdp.coinbase.com/coinbase-business/advanced-trade-apis/websocket/websocket-channels). Halts stop new quotes but allow already resting orders to fill until cancellation arrives. Pending hedges and open quotes remain visible at the end; no future market update is invented to finish them.

Walk-forward selects parameters on the first 60% of the recording and evaluates the later 40%, including slower hedges, larger queues and reduced depth. Unfinished exposure, recording gaps, unsupported selection and losing periods cannot qualify. `live_ready` is always false. Repeatedly inspecting the held-out period turns it into training data; collect a new period for the next evaluation. Initial inventory is acquired from the full starting cash budget, so return includes sell inventory and its acquisition cost.

## 3. Market discovery

```bash
uv run research.py discover > data/discovery.json
```

Discovery screens shared spot markets in configured quotes, selects liquid candidates, then ranks a bounded depth shortlist by indicative maker-entry/hedge return after configured fees and rebalancing. USD and USDT results retain their own quote labels. Discovery never changes configured routes. Public snapshots, queue assumptions, available inventory and costs still require recorder/replay validation. A blank or negative shortlist is a valid result.

## 4. Order recovery

Every live attempt journals both client intents before submission. Authenticated user-order streams add durable observations; REST reconciliation is required before settlement. Unknown submissions never retry automatically. Missing fees, mismatched quantities, stream failure and unresolved journals latch a persistent halt across restart.

```bash
touch data/KILL
# Stop the collector before the local audit.
uv run research.py recovery-audit > data/recovery-audit.json
```

The audit reads exchange order history and writes a local audit trail; it never submits/cancels orders or clears a halt. An empty bounded history search is inconclusive. Check every client/exchange ID, terminal quantity, actual fee and account balance. Resolve remaining orders in the exchange UI, record unmatched base quantities and operator action, and review any state repair before restart. Do not delete journal rows or retry unknown intents.

The explicitly enabled live path arms Kraken's account-wide 60-second disconnect cancellation timer and renews every 20 seconds. Kill, private-stream failure, process death or failed renewal leaves it to expire. This applies to **all orders in the Kraken account**, so a dedicated pilot account is required. Read-only economics and audits never arm the timer. No automatic hedge is sent for an unmatched IOC batch; that remains an operator recovery event. [Kraken disconnect cancellation](https://docs-legacy.kraken.com/api/docs/websocket-v1/cancelallordersafter/), [Coinbase user-order websocket](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-overview).

## 5. Portfolio

Dashboard/API portfolio state reports committed capital, venue and asset exposure, available hedge quantities, cash by venue, drawdown, and inventory drift. Pre-trade checks include projected inventory, free cash, concentration, drift and cash reserves. All nonzero live holdings are included; an unpriced asset blocks approval instead of disappearing from capital accounting. Rebalance proposals are indicative and never execute automatically.

## 6. Operations and recovery drills

The independent watchdog observes `data/heartbeat.json` and latches `data/KILL` for an expired heartbeat, unhealthy recorder, or prolonged feed outage. It writes `data/watchdog-alert.json`; engine incidents and latency samples appear in the API/dashboard and local logs. Alerts are local only; external paging requires a chosen destination. A watchdog cannot reverse submitted orders, and the exchange timer remains the remote protection when the local process freezes.

```bash
uv run research.py backup --output data/backups/manual-001
uv run research.py service-files --output deploy
uv run python -m unittest discover -s tests -v
```

Backups use SQLite's online backup API and integrity verification. Never copy only the main file of a running WAL database. Scheduled backups run every six hours; archive tape files and backups to a separate storage device according to your retention policy. The `service-files` command only generates manifests. The collector and study jobs were subsequently installed and verified as documented below; neither enables live trading.

The test suite performs isolated failure drills: a subprocess commits an order intent and exits abruptly, reconciliation finds it unresolved after restart, and a separate watchdog process latches a kill file without engine cooperation. It also checks backup content/integrity, disconnect renewal failure, delayed cancellation fills and partial-hedge exposure. These use temporary paths and no exchange orders.

For restore rehearsal: stop the engine, restore a verified backup into a **new directory**, run SQLite integrity checks, compare session/journal counts, and audit all unsettled live orders before any recovery. Never overwrite production files during a drill. Paper balances and evidence survive graceful restart; a crashed tape session remains open and cannot pass walk-forward evaluation.

Compare hosting candidates using the same public-data sample duration and symbols. Capture event-loop lag, source timestamp age, disconnect count and availability. Source timestamp age includes clock differences and exchange batching; it is not an order round-trip measurement. Live order acknowledgement latency is collected only during an authorized pilot. No hosting region has been selected or provisioned without these measurements.

## Installed local services and future-data study (September 7, 2026)

The user launchd jobs `local.arb.collector` and `local.arb.study` are installed in `~/Library/LaunchAgents`. They start at login and restart after process exit. The collector uses `paper_service.py`, which refuses live mode and never passes `--enable-live`. This is a local service on an awake, connected Mac; it does not establish cloud-host reliability or uptime while asleep/logged out.

```bash
launchctl print gui/$(id -u)/local.arb.collector
launchctl print gui/$(id -u)/local.arb.study
.venv/bin/python study.py status
```

Stop the jobs before manually running an engine or receipt-writing account check:

```bash
launchctl bootout gui/$(id -u)/local.arb.collector
launchctl bootout gui/$(id -u)/local.arb.study
```

Start them again using their installed plist files:

```bash
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/local.arb.collector.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/local.arb.study.plist
```

`data/research/future-study-001/protocol.json` freezes strategy parameters, code identity, and economics before observing the study's future data. The first 24 hours are diagnostic; the following 72 hours are held out. The worker records coverage and performs the base and stressed replay when each period exists. It requires at least 95% healthy coverage and 100 maker-fill events in validation for its limited research screen, but these are screening thresholds, not a statistical certification. It never authorizes live orders. A code/economics change blocks this study; create a separate study explicitly instead of rewriting its protocol. The public dataset is read up to a captured event ID and period boundary, so later events cannot alter earlier hedge decisions.

The study uses configured costs, which remain estimates until accounts and real bills are verified. Current credentials are absent. No tested edge at estimated costs is grounds to manufacture a profitable strategy or turn trading on.

Tape capacity is now bounded at 56 GiB based on the observed initial rate of roughly 11 GiB/day, with 20 GiB of free disk reserved. The rate can change. Local backups have a separate 24 GiB budget: when exhausted, the engine preserves existing backups and raises a local incident rather than silently deleting history or filling the disk. Off-machine archival is still required for hardware-failure protection. It has not been configured.

Actual backup restoration reports are in `data/research/restore-drill-001/`. They compare every stored row and verify SQLite integrity in new destination files. The launchd graceful-restart verification is in `data/research/service-restart-drill.json`; it is distinct from the isolated abrupt-exit tests in the suite. No authenticated recovery test or live order was performed.
