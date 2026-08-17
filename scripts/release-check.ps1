$ErrorActionPreference = "Stop"

Write-Host "[1/2] cargo test --locked"
cargo test --locked
if ($LASTEXITCODE -ne 0) {
    Write-Error "cargo test failed with exit code $LASTEXITCODE"
    exit $LASTEXITCODE
}

Write-Host "[2/2] cargo clippy --locked --all-targets"
cargo clippy --locked --all-targets
if ($LASTEXITCODE -ne 0) {
    Write-Error "cargo clippy failed with exit code $LASTEXITCODE"
    exit $LASTEXITCODE
}

Write-Host "Release checks passed."
