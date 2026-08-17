# Architecture

## Design goal

The public project is a long-running **read-only scanner service**, not a transaction executor. Its job is to discover a useful pool universe, keep state reasonably fresh, search bounded atomic cycles, validate plausible routes, and preserve enough evidence to audit what it saw later.

## Pipeline

1. **Configuration validation** checks chain IDs, addresses, numeric bounds, enabled venues, tokens and cache paths.
2. **Universe discovery** loads a compatible fresh cache when possible. On a cold/stale cache it uses configured factories, enumerable pool arrays and supported creation-event discovery to expand around seed tokens.
3. **State refresh** batches pool reads using Multicall3 and tracks partial/empty/uninitialized pool telemetry.
4. **Hot-pool selection** applies liquidity gates before route enumeration.
5. **Graph search** performs bounded DFS for 2-hop, 3-hop and 4-hop cycles.
6. **Cheap screens** remove routes that fail marginal profitability, liquidity/capacity or optimistic upper-bound checks.
7. **Sizing** uses local AMM math where safe and falls back to exact RPC sizing for unsupported/cross-tick cases.
8. **Exact quotes** validate survivors using venue-specific quote paths.
9. **Economics** records gross profit, funding mode, financing fee and a conservative execution-cost estimate.
10. **Persistence** upserts interesting routes to per-chain SQLite databases and writes final JSON artifacts atomically.

## Reliability properties

### Per-chain isolation

Each enabled chain runs in its own task. A chain that cannot initialize or repeatedly fails RPC calls is marked failed without terminating healthy chain workers.

### RPC backpressure

The RPC client uses:

- bounded attempts;
- exponential/bounded retry delays;
- `Retry-After` handling for HTTP 429 responses;
- a shared provider cooldown across cloned clients;
- optional minimum request spacing;
- request/retry/failure/rate-limit counters.

Provider URLs are never intentionally logged. Transport errors are reduced to safe categories because `reqwest` errors may otherwise embed the full request URL, including API keys.

### Cache safety

Pool/universe caches are schema-checked and written through a temporary file + rename. Stale or incompatible caches are rebuilt rather than trusted.

### Output safety

Final JSON files are written to `*.json.tmp` and atomically renamed into place. Opportunity SQLite databases persist during the run.

### Shutdown

A fixed `--seconds` duration or `Ctrl+C` sends a watch-channel shutdown signal to workers. The coordinator joins workers and writes final summary/top/health artifacts before exit.

### Fail-loud behavior

Partial chain failures are tolerated. If **zero successful scans** complete across all enabled chains, the process still writes diagnostics and then exits with an error.

## What “analytical sizing” means here

The scanner uses deterministic local sizing when the route can be safely modeled from known pool state. Constant-product legs are directly modelable. Concentrated-liquidity sizing is deliberately bounded to known active-range information; if the candidate presses into unknown tick liquidity, the scanner falls back rather than extrapolating.

This is a latency/research optimization, not a claim that every route shape has a closed-form globally exact optimizer.

## Non-goals

The public scanner does not:

- hold or read private keys;
- build signed transactions;
- deploy contracts;
- submit bundles;
- broadcast transactions;
- sandwich users;
- claim observed route maxima are simultaneously capturable;
- claim breadth-stage `estimated_net_usd` is exact execution PnL.
