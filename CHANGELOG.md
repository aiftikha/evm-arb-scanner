# Changelog

## 1.0.0 — Public scanner release

- Extracted the validated V3.4.2 breadth engine into a scanner-only binary.
- Removed transaction-signing, relay, deployment and execution modules from the public source tree.
- Simplified CLI to `--config` and optional `--seconds`.
- Sanitized RPC transport failures to avoid leaking credential-bearing provider URLs.
- Added richer per-chain health artifacts with status, last error, scan latency and RPC counters.
- Added fail-loud behavior when no enabled chain completes a successful scan.
- Added temporary-file JSON replacement for final artifacts.
- Enabled Arbitrum, Base and Optimism by default; left Tier-3 chains configured but disabled.
- Added CI, security policy, contribution guide, architecture documentation and final benchmark evidence.
