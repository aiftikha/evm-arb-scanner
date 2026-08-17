# Contributing

Contributions are welcome for scanner correctness, protocol adapters, RPC resilience, performance, observability, tests and documentation.

Before opening a pull request:

```powershell
cargo test --locked
cargo clippy --locked --all-targets
```

Please keep the public project read-only. Changes that add transaction signing, key handling, bundle submission, public-mempool broadcast or user-targeting MEV are out of scope for this repository.

For protocol math or adapter changes, include a deterministic unit test or a reproducible read-only validation case where practical.
