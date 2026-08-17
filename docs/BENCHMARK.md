# Final six-hour benchmark

## Run

- Scanner lineage: V3.4.2 final expanded-universe breadth engine
- Duration: **21,600 seconds (6 hours)**
- Funding model: up to **$200 self-funded** before flash-financing assumptions apply
- Search depths: **2, 3, 4 hops**
- Output: per-chain SQLite evidence + global summary/top/health JSON

## Universe expansion

Compared with the earlier V3.3 universe:

| Chain | Earlier discovered pools | Final discovered pools | Approx. expansion |
|---|---:|---:|---:|
| Arbitrum | ~290 | 1,308 | ~4.5x |
| Base | ~97 | 1,421 | ~14.6x |
| Optimism | ~105 | 973 | ~9.3x |

This matters because the final economic conclusion was reached **after** materially broadening the search space rather than from a tiny hand-picked pool set.

## Final chain metrics

| Chain | Scans | Discovered | Hot | Notes |
|---|---:|---:|---:|---|
| Arbitrum | 1,330 | 1,308 | 126 | Large route population; many overlapping pools |
| Base | 215 | 1,421 | 247 | Broadest hot set; scans were comparatively expensive |
| Optimism | 4,043 | 973 | 110 | Healthiest/highest-frequency useful chain in this run |
| Linea | 2,140 | 19 | 5 | Very small configured universe |
| Scroll | 2,155 | 25 | 3 | No useful candidate population in this run |
| zkSync | 1,670 | 38 | 6 | RPC quality was poor; interpret cautiously |

## Best observed breadth-stage candidates

These are **profit after financing but before the breadth scanner's execution-cost estimate**, so they are upper bounds on exact net rather than realized PnL.

| Chain | Best |
|---|---:|
| Optimism | **$0.06964955** |
| zkSync | $0.03815055 |
| Arbitrum | $0.02745155 |
| Base | $0.01195755 |
| Linea | $0.00999400 |
| Scroll | none |

The highest candidate observed in the entire six-hour run was an Optimism 4-hop cycle around **$63.10 input** with about **$0.06965 gross/self-funded profit before execution cost**.

## RPC health

The final run recorded substantial provider pressure on some chains. zkSync was the weakest endpoint in the test, with 51,727 requests, 12,374 retries and 4,108 failures. This is why the public `config.toml` leaves Tier-3 chains disabled by default.

## Interpretation

The scanner succeeded technically:

- materially expanded the pool universe;
- found exact-quoted finite-size discrepancies;
- found persistent route identities;
- demonstrated that self-funding improves small-trade economics;
- preserved route evidence and health telemetry over a multi-hour run.

The strategy result was economically weak. Even before exact execution costs, the best opportunity was below ten cents, and many nominally unique routes shared pools/liquidity.

That distinction is intentional: this repository is an engineering/research artifact, not a “profitable bot” marketing claim.
