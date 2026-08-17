param(
    [int]$Seconds = 300
)

$ErrorActionPreference = "Stop"
if ($Seconds -le 0) { throw "Seconds must be > 0" }

cargo run --release -- --seconds $Seconds
