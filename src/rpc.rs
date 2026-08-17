use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, bail, Context, Result};
use ethabi::{
    decode as abi_decode, encode as abi_encode,
    ethereum_types::{H160, U256},
    ParamType, Token,
};
use reqwest::{header::RETRY_AFTER, Client, StatusCode};
use serde_json::{json, Value};
use tracing::debug;

use crate::types::{
    DexKind, PoolDefinition, PoolState, RpcStatsSnapshot, V2PoolState, V3PoolState, V3Quote,
};

#[derive(Debug, Clone, Default)]
pub struct StateSnapshot {
    pub states: Vec<PoolState>,
    pub expected_calls: usize,
    pub returned_calls: usize,
    pub inner_failures: usize,
    pub decode_failures: usize,
    pub rpc_batches: usize,
    pub empty_or_uninitialized_pools: usize,
    pub complete: bool,
}

#[derive(Debug, Clone)]
pub struct BalancerV2SwapStep {
    pub pool_id: String,
    pub asset_in_index: usize,
    pub asset_out_index: usize,
    pub amount_raw: u128,
}


#[derive(Debug, Clone)]
pub struct RpcLog {
    pub address: String,
    pub topics: Vec<String>,
    pub data: String,
    pub block_number: u64,
}

#[derive(Debug, Clone)]
pub struct CurvePoolQuote {
    pub source_index: u32,
    pub dest_index: u32,
    pub is_underlying: bool,
    pub amount_out_raw: u128,
    pub pool: String,
    pub source_balance_raw: u128,
    pub dest_balance_raw: u128,
    /// 0 = Stableswap, 1 = Cryptoswap, 2 = LLAMMA. V0.3.2 ignores type 2.
    pub pool_type: u8,
}

#[derive(Debug, Clone)]
struct MulticallCall {
    target: String,
    data: String,
}

#[derive(Debug, Clone)]
struct MulticallResult {
    success: bool,
    return_data: String,
}

#[derive(Debug, Clone, Copy)]
enum StateCallKind {
    V2Reserves(usize),
    V3Slot0(usize),
    V3Liquidity(usize),
    AlgebraGlobalState(usize),
    SolidlyFee(usize),
    SlipstreamFee(usize),
}

#[derive(Debug, Clone)]
struct StateCallSpec {
    call: MulticallCall,
    kind: StateCallKind,
}

#[derive(Clone)]
pub struct RpcClient {
    client: Client,
    url: String,
    id: Arc<AtomicU64>,
    retries: Arc<AtomicU64>,
    failures: Arc<AtomicU64>,
    rate_limits: Arc<AtomicU64>,
    /// Shared epoch-millisecond deadline. Any worker receiving a 429 pushes this
    /// deadline forward, causing every concurrent worker to cool down together.
    rate_limit_until_ms: Arc<AtomicU64>,
    /// Optional per-client request pacing. The atomic slot is shared across clones,
    /// smoothing short RPC bursts without changing route breadth or quote budgets.
    next_request_slot_ms: Arc<AtomicU64>,
    min_request_spacing_ms: u64,
    max_attempts: u32,
    retry_base_ms: u64,
    retry_max_ms: u64,
}

impl RpcClient {
    pub fn new(
        url: String,
        max_attempts: u32,
        retry_base_ms: u64,
        retry_max_ms: u64,
        request_timeout_ms: u64,
    ) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_millis(request_timeout_ms.max(1)))
            .build()
            .context("failed to build RPC HTTP client")?;
        Ok(Self {
            client,
            url,
            id: Arc::new(AtomicU64::new(1)),
            retries: Arc::new(AtomicU64::new(0)),
            failures: Arc::new(AtomicU64::new(0)),
            rate_limits: Arc::new(AtomicU64::new(0)),
            rate_limit_until_ms: Arc::new(AtomicU64::new(0)),
            next_request_slot_ms: Arc::new(AtomicU64::new(0)),
            min_request_spacing_ms: 0,
            max_attempts: max_attempts.max(1),
            retry_base_ms,
            retry_max_ms: retry_max_ms.max(retry_base_ms),
        })
    }

    pub fn with_min_request_spacing_ms(mut self, spacing_ms: u64) -> Self {
        self.min_request_spacing_ms = spacing_ms;
        self
    }

    /// Override retry policy on a clone. The shared counters/rate-limit deadline and
    /// request pacing slot remain shared across clones, so a discovery-only client can
    /// use slower pacing and longer backoff without creating an independent burst source.
    pub fn with_retry_policy(mut self, max_attempts: u32, retry_base_ms: u64, retry_max_ms: u64) -> Self {
        self.max_attempts = max_attempts.max(1);
        self.retry_base_ms = retry_base_ms;
        self.retry_max_ms = retry_max_ms.max(retry_base_ms);
        self
    }

    pub fn stats_snapshot(&self) -> RpcStatsSnapshot {
        RpcStatsSnapshot {
            requests: self.id.load(Ordering::Relaxed).saturating_sub(1),
            retries: self.retries.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
            rate_limits: self.rate_limits.load(Ordering::Relaxed),
        }
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value> {
        let mut last_error = String::new();

        for attempt in 1..=self.max_attempts {
            self.wait_for_global_rate_limit().await;
            self.wait_for_request_slot().await;
            // A sibling call may have received a 429 while this request was waiting
            // for its pacing slot. Re-check the shared provider cooldown before send.
            self.wait_for_global_rate_limit().await;

            let id = self.id.fetch_add(1, Ordering::Relaxed);
            let body = json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": method,
                "params": params.clone(),
            });

            let response = match self.client.post(&self.url).json(&body).send().await {
                Ok(response) => response,
                Err(err) => {
                    // reqwest error Display output can include the request URL. RPC URLs often
                    // contain provider API keys, so never propagate the raw error into logs.
                    last_error = format!(
                        "RPC request failed on {method}: {}",
                        safe_reqwest_error_kind(&err)
                    );
                    if attempt < self.max_attempts {
                        self.record_retry(method, attempt, &last_error).await;
                        continue;
                    }
                    self.failures.fetch_add(1, Ordering::Relaxed);
                    bail!("RPC request failed after {attempt} attempts on {method}: {}", safe_reqwest_error_kind(&err));
                }
            };

            let status = response.status();
            let retry_after_ms = response
                .headers()
                .get(RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(parse_retry_after_ms);
            let text = match response.text().await {
                Ok(text) => text,
                Err(err) => {
                    last_error = format!(
                        "RPC response body failed on {method}: {}",
                        safe_reqwest_error_kind(&err)
                    );
                    if attempt < self.max_attempts {
                        self.record_retry(method, attempt, &last_error).await;
                        continue;
                    }
                    self.failures.fetch_add(1, Ordering::Relaxed);
                    bail!("failed reading RPC response after {attempt} attempts on {method}: {}", safe_reqwest_error_kind(&err));
                }
            };

            if !status.is_success() {
                last_error = format!(
                    "RPC HTTP {} on {}: {}",
                    status.as_u16(),
                    method,
                    truncate(&text, 240)
                );
                if status == StatusCode::TOO_MANY_REQUESTS && attempt < self.max_attempts {
                    self.record_rate_limit_retry(method, attempt, retry_after_ms, &last_error)
                        .await;
                    continue;
                }
                if is_retryable_http(status) && attempt < self.max_attempts {
                    self.record_retry(method, attempt, &last_error).await;
                    continue;
                }
                self.failures.fetch_add(1, Ordering::Relaxed);
                bail!("{last_error}");
            }

            let value: Value = match serde_json::from_str(&text) {
                Ok(value) => value,
                Err(err) => {
                    last_error = format!("invalid RPC JSON response on {method}: {err}");
                    if attempt < self.max_attempts {
                        self.record_retry(method, attempt, &last_error).await;
                        continue;
                    }
                    self.failures.fetch_add(1, Ordering::Relaxed);
                    return Err(err).context("invalid RPC JSON response");
                }
            };

            if let Some(err) = value.get("error") {
                last_error = format!("RPC error on {method}: {err}");
                if is_json_rpc_rate_limit(err) && attempt < self.max_attempts {
                    self.record_rate_limit_retry(method, attempt, None, &last_error)
                        .await;
                    continue;
                }
                if is_retryable_json_rpc_error(err) && attempt < self.max_attempts {
                    self.record_retry(method, attempt, &last_error).await;
                    continue;
                }
                self.failures.fetch_add(1, Ordering::Relaxed);
                bail!("{last_error}");
            }

            if let Some(result) = value.get("result") {
                return Ok(result.clone());
            }

            last_error = format!("RPC response missing result for {method}");
            if attempt < self.max_attempts {
                self.record_retry(method, attempt, &last_error).await;
                continue;
            }
            self.failures.fetch_add(1, Ordering::Relaxed);
            bail!("{last_error}");
        }

        self.failures.fetch_add(1, Ordering::Relaxed);
        bail!("RPC call exhausted retries for {method}: {last_error}")
    }

    async fn wait_for_request_slot(&self) {
        let spacing = self.min_request_spacing_ms;
        if spacing == 0 {
            return;
        }

        loop {
            let now = unix_millis();
            let current = self.next_request_slot_ms.load(Ordering::Acquire);
            let slot = current.max(now);
            let next = slot.saturating_add(spacing);
            match self.next_request_slot_ms.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    if slot > now {
                        tokio::time::sleep(Duration::from_millis(slot - now)).await;
                    }
                    return;
                }
                Err(_) => continue,
            }
        }
    }

    async fn wait_for_global_rate_limit(&self) {
        loop {
            let until = self.rate_limit_until_ms.load(Ordering::Acquire);
            let now = unix_millis();
            if until <= now {
                return;
            }
            tokio::time::sleep(Duration::from_millis(until.saturating_sub(now))).await;
        }
    }

    async fn record_rate_limit_retry(
        &self,
        method: &str,
        attempt: u32,
        retry_after_ms: Option<u64>,
        reason: &str,
    ) {
        self.retries.fetch_add(1, Ordering::Relaxed);
        self.rate_limits.fetch_add(1, Ordering::Relaxed);
        let exp = 1u64 << (attempt.saturating_sub(1).min(10));
        // Rate limits need a much slower retry cadence than an ordinary transient
        // RPC failure. With defaults this yields roughly 1s, 2s, 4s ... while all
        // concurrent workers share the same cooldown deadline.
        let rate_backoff = 1_000u64
            .saturating_mul(exp)
            .min(self.retry_max_ms.saturating_mul(4).max(6_000));
        let jitter = self.id.load(Ordering::Relaxed) % 97;
        let delay_ms = match retry_after_ms {
            // Provider guidance wins. Do not truncate Retry-After to our normal
            // transient-error ceiling.
            Some(ms) => ms.max(rate_backoff).saturating_add(jitter),
            None => rate_backoff.saturating_add(jitter),
        };
        self.push_global_rate_limit(delay_ms);
        debug!(
            method,
            attempt, delay_ms, reason, "global RPC rate-limit cooldown"
        );
        self.wait_for_global_rate_limit().await;
    }

    fn push_global_rate_limit(&self, delay_ms: u64) {
        let target = unix_millis().saturating_add(delay_ms.max(1));
        let mut current = self.rate_limit_until_ms.load(Ordering::Acquire);
        while target > current {
            match self.rate_limit_until_ms.compare_exchange_weak(
                current,
                target,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
    }

    async fn record_retry(&self, method: &str, attempt: u32, reason: &str) {
        self.retries.fetch_add(1, Ordering::Relaxed);
        let exp = 1u64 << (attempt.saturating_sub(1).min(10));
        let base = self
            .retry_base_ms
            .saturating_mul(exp)
            .min(self.retry_max_ms);
        let jitter = self.id.load(Ordering::Relaxed) % 47;
        let delay_ms = base
            .saturating_add(jitter)
            .min(self.retry_max_ms.saturating_add(47));
        debug!(method, attempt, delay_ms, reason, "retrying RPC call");
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }

    pub async fn block_number(&self) -> Result<u64> {
        let v = self.call("eth_blockNumber", json!([])).await?;
        parse_hex_u64(v.as_str().ok_or_else(|| anyhow!("bad block number"))?)
    }

    /// Returns the canonical block timestamp in Unix seconds. This is used only
    /// for live-readiness latency diagnostics; a timestamp failure never weakens
    /// state-snapshot or opportunity-validation safety gates.
    pub async fn block_timestamp(&self, block: u64) -> Result<u64> {
        let tag = format!("0x{block:x}");
        let v = self
            .call("eth_getBlockByNumber", json!([tag, false]))
            .await?;
        let raw = v
            .get("timestamp")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("block {block} missing timestamp"))?;
        parse_hex_u64(raw)
    }

    pub async fn gas_price_wei(&self) -> Result<u128> {
        let v = self.call("eth_gasPrice", json!([])).await?;
        parse_hex_u128(v.as_str().ok_or_else(|| anyhow!("bad gas price"))?)
    }

    /// Ethereum chain id used when constructing EIP-1559 typed transactions.
    pub async fn chain_id(&self) -> Result<u64> {
        let v = self.call("eth_chainId", json!([])).await?;
        parse_hex_u64(v.as_str().ok_or_else(|| anyhow!("bad chain id"))?)
    }

    /// Pending nonce is used by the V3 dry-run signer so the deployment and
    /// execution transactions form one contiguous private bundle.
    pub async fn transaction_count_pending(&self, address: &str) -> Result<u64> {
        let v = self
            .call("eth_getTransactionCount", json!([address, "pending"]))
            .await?;
        parse_hex_u64(v.as_str().ok_or_else(|| anyhow!("bad pending nonce"))?)
    }

    pub async fn transaction_count_at(&self, address: &str, block: u64) -> Result<u64> {
        let tag = format!("0x{block:x}");
        let v = self
            .call("eth_getTransactionCount", json!([address, tag]))
            .await?;
        parse_hex_u64(v.as_str().ok_or_else(|| anyhow!("bad confirmed nonce"))?)
    }

    pub async fn balance_wei(&self, address: &str) -> Result<u128> {
        let v = self
            .call("eth_getBalance", json!([address, "latest"]))
            .await?;
        parse_hex_u128(v.as_str().ok_or_else(|| anyhow!("bad account balance"))?)
    }

    /// Returns the canonical baseFeePerGas for a specific block. V3 targets the
    /// following block and applies the protocol's maximum one-block +12.5% move.
    pub async fn block_base_fee_wei(&self, block: u64) -> Result<u128> {
        let tag = format!("0x{block:x}");
        let v = self
            .call("eth_getBlockByNumber", json!([tag, false]))
            .await?;
        let raw = v
            .get("baseFeePerGas")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("block {block} missing baseFeePerGas"))?;
        parse_hex_u128(raw)
    }

    /// Estimate contract-creation gas against the candidate block. This is used
    /// only to construct a realistic signed deployment transaction for relay
    /// simulation; the transaction is never broadcast by V3.0.
    pub async fn estimate_contract_creation_gas_at(
        &self,
        from: &str,
        creation_data: &str,
        block: u64,
    ) -> Result<u64> {
        let tag = format!("0x{block:x}");
        let v = self
            .call(
                "eth_estimateGas",
                json!([{"from": from, "data": creation_data, "value": "0x0"}, tag]),
            )
            .await?;
        parse_hex_u64(
            v.as_str()
                .ok_or_else(|| anyhow!("bad contract creation gas estimate"))?,
        )
    }

    /// Historical log reader used by V3.4 cold-start factory universe expansion.
    /// Read-only JSON-RPC only; this never subscribes, signs, or submits.
    pub async fn get_logs(
        &self,
        address: &str,
        topic0: &str,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<RpcLog>> {
        if to_block < from_block {
            return Ok(Vec::new());
        }
        let value = self
            .call(
                "eth_getLogs",
                json!([{
                    "address": address,
                    "fromBlock": format!("0x{from_block:x}"),
                    "toBlock": format!("0x{to_block:x}"),
                    "topics": [topic0],
                }]),
            )
            .await?;
        let rows = value
            .as_array()
            .ok_or_else(|| anyhow!("eth_getLogs result is not an array"))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let address = row.get("address").and_then(Value::as_str)
                .ok_or_else(|| anyhow!("eth_getLogs row missing address"))?
                .to_string();
            let topics = row.get("topics").and_then(Value::as_array)
                .ok_or_else(|| anyhow!("eth_getLogs row missing topics"))?
                .iter()
                .map(|v| v.as_str().map(str::to_string).ok_or_else(|| anyhow!("bad log topic")))
                .collect::<Result<Vec<_>>>()?;
            let data = row.get("data").and_then(Value::as_str)
                .ok_or_else(|| anyhow!("eth_getLogs row missing data"))?
                .to_string();
            let block_number = parse_hex_u64(
                row.get("blockNumber").and_then(Value::as_str)
                    .ok_or_else(|| anyhow!("eth_getLogs row missing blockNumber"))?
            )?;
            out.push(RpcLog { address, topics, data, block_number });
        }
        Ok(out)
    }

    /// Minimal ERC-20 metadata probe for auto-discovered V3.4 routing tokens.
    pub async fn erc20_decimals(&self, token: &str) -> Result<u8> {
        let raw = self.eth_call(token, "0x313ce567").await?;
        let value = parse_hex_u128(word(&raw, 0)?)?;
        if value > 36 {
            bail!("unreasonable ERC20 decimals {value} for {token}");
        }
        Ok(value as u8)
    }

    pub async fn erc20_symbol(&self, token: &str) -> Result<String> {
        let raw = self.eth_call(token, "0x95d89b41").await?;
        let bytes = decode_hex_bytes(&raw)?;
        if let Ok(decoded) = abi_decode(&[ParamType::String], &bytes) {
            if let Some(Token::String(symbol)) = decoded.first() {
                let trimmed = symbol.trim();
                if !trimmed.is_empty() {
                    return Ok(trimmed.chars().take(24).collect());
                }
            }
        }
        if bytes.len() >= 32 {
            let candidate = &bytes[..32];
            let end = candidate.iter().position(|b| *b == 0).unwrap_or(candidate.len());
            if let Ok(symbol) = std::str::from_utf8(&candidate[..end]) {
                let trimmed = symbol.trim();
                if !trimmed.is_empty() {
                    return Ok(trimmed.chars().take(24).collect());
                }
            }
        }
        bail!("unable to decode ERC20 symbol for {token}")
    }

    pub async fn eth_call(&self, to: &str, data: &str) -> Result<String> {
        self.eth_call_tag(to, data, "latest").await
    }

    pub async fn eth_call_at(&self, to: &str, data: &str, block: u64) -> Result<String> {
        let tag = format!("0x{block:x}");
        self.eth_call_tag(to, data, &tag).await
    }

    /// Same-block read-only eth_call with explicit sender and legacy gasPrice.
    /// V3.0.4e uses this only to isolate EVM GASPRICE sensitivity in external
    /// protocol quote functions; it never submits a transaction.
    pub async fn eth_call_at_with_from_and_gas_price(
        &self,
        from: &str,
        to: &str,
        data: &str,
        block: u64,
        gas_price_wei: u128,
    ) -> Result<String> {
        let tag = format!("0x{block:x}");
        let v = self
            .call(
                "eth_call",
                json!([{
                    "from": from,
                    "to": to,
                    "data": data,
                    "gasPrice": format!("0x{gas_price_wei:x}")
                }, tag]),
            )
            .await?;
        Ok(v.as_str()
            .ok_or_else(|| anyhow!("gas-price-context eth_call result is not a string"))?
            .to_string())
    }

    /// Same-block read-only eth_call with explicit legacy gasPrice and default sender.
    /// V3.0.4f uses this as a fail-closed Curve safety probe; it never submits.
    pub async fn eth_call_at_with_gas_price(
        &self,
        to: &str,
        data: &str,
        block: u64,
        gas_price_wei: u128,
    ) -> Result<String> {
        let tag = format!("0x{block:x}");
        let v = self
            .call(
                "eth_call",
                json!([{
                    "to": to,
                    "data": data,
                    "gasPrice": format!("0x{gas_price_wei:x}")
                }, tag]),
            )
            .await?;
        Ok(v.as_str()
            .ok_or_else(|| anyhow!("gas-price-context eth_call result is not a string"))?
            .to_string())
    }

    async fn eth_call_tag(&self, to: &str, data: &str, block_tag: &str) -> Result<String> {
        let v = self
            .call("eth_call", json!([{"to": to, "data": data}, block_tag]))
            .await?;
        Ok(v.as_str()
            .ok_or_else(|| anyhow!("eth_call result is not a string"))?
            .to_string())
    }

    /// Same-block read-only eth_call with a temporary code override. The override is
    /// scoped to this single RPC simulation and never mutates canonical chain state.
    pub async fn eth_call_at_with_code_override(
        &self,
        to: &str,
        data: &str,
        block: u64,
        override_address: &str,
        runtime_code: &str,
    ) -> Result<String> {
        self.eth_call_at_with_code_and_state_override(
            None,
            to,
            data,
            block,
            override_address,
            runtime_code,
            serde_json::Map::new(),
        )
        .await
    }

    /// Same-block eth_call with temporary runtime code plus an explicit stateDiff.
    /// Used by V2 to exercise the exact authorization/allowlist path before deployment.
    /// The override exists only for this RPC call and is never persisted on-chain.
    pub async fn eth_call_at_with_code_and_state_override(
        &self,
        from: Option<&str>,
        to: &str,
        data: &str,
        block: u64,
        override_address: &str,
        runtime_code: &str,
        state_diff: serde_json::Map<String, Value>,
    ) -> Result<String> {
        let tag = format!("0x{block:x}");
        let mut account_override = serde_json::Map::new();
        account_override.insert("code".to_string(), Value::String(runtime_code.to_string()));
        if !state_diff.is_empty() {
            account_override.insert("stateDiff".to_string(), Value::Object(state_diff));
        }

        let mut overrides = serde_json::Map::new();
        overrides.insert(
            override_address.to_ascii_lowercase(),
            Value::Object(account_override),
        );

        let mut call_object = serde_json::Map::new();
        call_object.insert("to".to_string(), Value::String(to.to_string()));
        call_object.insert("data".to_string(), Value::String(data.to_string()));
        if let Some(from) = from {
            call_object.insert("from".to_string(), Value::String(from.to_string()));
        }

        let v = self
            .call(
                "eth_call",
                json!([Value::Object(call_object), tag, Value::Object(overrides)]),
            )
            .await?;
        Ok(v.as_str()
            .ok_or_else(|| anyhow!("state-override eth_call result is not a string"))?
            .to_string())
    }

    /// Same-block state-override eth_call with explicit EIP-1559 transaction context.
    /// V3.0.4 uses this only for diagnostic parity against the signed bundle; it never
    /// sends or persists a transaction.
    pub async fn eth_call_at_with_code_state_and_tx_context(
        &self,
        from: &str,
        to: &str,
        data: &str,
        block: u64,
        override_address: &str,
        runtime_code: &str,
        state_diff: serde_json::Map<String, Value>,
        gas_limit: u64,
        max_fee_per_gas: u128,
        max_priority_fee_per_gas: u128,
    ) -> Result<String> {
        let tag = format!("0x{block:x}");
        let mut account_override = serde_json::Map::new();
        account_override.insert("code".to_string(), Value::String(runtime_code.to_string()));
        if !state_diff.is_empty() {
            account_override.insert("stateDiff".to_string(), Value::Object(state_diff));
        }

        let mut overrides = serde_json::Map::new();
        overrides.insert(
            override_address.to_ascii_lowercase(),
            Value::Object(account_override),
        );

        let mut call_object = serde_json::Map::new();
        call_object.insert("from".to_string(), Value::String(from.to_string()));
        call_object.insert("to".to_string(), Value::String(to.to_string()));
        call_object.insert("data".to_string(), Value::String(data.to_string()));
        call_object.insert("gas".to_string(), Value::String(format!("0x{gas_limit:x}")));
        call_object.insert(
            "maxFeePerGas".to_string(),
            Value::String(format!("0x{max_fee_per_gas:x}")),
        );
        call_object.insert(
            "maxPriorityFeePerGas".to_string(),
            Value::String(format!("0x{max_priority_fee_per_gas:x}")),
        );
        call_object.insert("value".to_string(), Value::String("0x0".to_string()));

        let v = self
            .call(
                "eth_call",
                json!([Value::Object(call_object), tag, Value::Object(overrides)]),
            )
            .await?;
        Ok(v.as_str()
            .ok_or_else(|| anyhow!("tx-context state-override eth_call result is not a string"))?
            .to_string())
    }

    /// Same-block state-override eth_call with selectively applied transaction-context
    /// fields. V3.0.4c uses this only for field-isolation diagnostics. Supplying legacy
    /// gasPrice together with EIP-1559 fee fields is intentionally rejected by callers.
    pub async fn eth_call_at_with_code_state_and_custom_tx_context(
        &self,
        from: &str,
        to: &str,
        data: &str,
        block: u64,
        override_address: &str,
        runtime_code: &str,
        state_diff: serde_json::Map<String, Value>,
        gas_limit: Option<u64>,
        max_fee_per_gas: Option<u128>,
        max_priority_fee_per_gas: Option<u128>,
        gas_price: Option<u128>,
        value_wei: Option<u128>,
    ) -> Result<String> {
        if gas_price.is_some() && (max_fee_per_gas.is_some() || max_priority_fee_per_gas.is_some())
        {
            return Err(anyhow!(
                "custom tx context cannot mix gasPrice with EIP-1559 fee fields"
            ));
        }
        if max_fee_per_gas.is_some() != max_priority_fee_per_gas.is_some() {
            return Err(anyhow!(
                "custom tx context requires both EIP-1559 fee fields or neither"
            ));
        }

        let tag = format!("0x{block:x}");
        let mut account_override = serde_json::Map::new();
        account_override.insert("code".to_string(), Value::String(runtime_code.to_string()));
        if !state_diff.is_empty() {
            account_override.insert("stateDiff".to_string(), Value::Object(state_diff));
        }

        let mut overrides = serde_json::Map::new();
        overrides.insert(
            override_address.to_ascii_lowercase(),
            Value::Object(account_override),
        );

        let mut call_object = serde_json::Map::new();
        call_object.insert("from".to_string(), Value::String(from.to_string()));
        call_object.insert("to".to_string(), Value::String(to.to_string()));
        call_object.insert("data".to_string(), Value::String(data.to_string()));
        if let Some(gas_limit) = gas_limit {
            call_object.insert("gas".to_string(), Value::String(format!("0x{gas_limit:x}")));
        }
        if let (Some(max_fee_per_gas), Some(max_priority_fee_per_gas)) =
            (max_fee_per_gas, max_priority_fee_per_gas)
        {
            call_object.insert(
                "maxFeePerGas".to_string(),
                Value::String(format!("0x{max_fee_per_gas:x}")),
            );
            call_object.insert(
                "maxPriorityFeePerGas".to_string(),
                Value::String(format!("0x{max_priority_fee_per_gas:x}")),
            );
        }
        if let Some(gas_price) = gas_price {
            call_object.insert(
                "gasPrice".to_string(),
                Value::String(format!("0x{gas_price:x}")),
            );
        }
        if let Some(value_wei) = value_wei {
            call_object.insert(
                "value".to_string(),
                Value::String(format!("0x{value_wei:x}")),
            );
        }

        let v = self
            .call(
                "eth_call",
                json!([Value::Object(call_object), tag, Value::Object(overrides)]),
            )
            .await?;
        Ok(v.as_str()
            .ok_or_else(|| {
                anyhow!("custom tx-context state-override eth_call result is not a string")
            })?
            .to_string())
    }

    /// V3.0.4c diagnostic-only call trace using the exact same code/state override
    /// mechanism as the parity eth_call. This never submits a transaction. The
    /// callTracer output is used to compare zero-gas-price execution against the
    /// same call with a non-zero effective gas price and identify the first nested
    /// protocol call whose calldata/output/error diverges.
    pub async fn debug_trace_call_at_with_code_state_and_gas_price(
        &self,
        from: &str,
        to: &str,
        data: &str,
        block: u64,
        override_address: &str,
        runtime_code: &str,
        state_diff: serde_json::Map<String, Value>,
        gas_price: u128,
    ) -> Result<Value> {
        let tag = format!("0x{block:x}");
        let mut account_override = serde_json::Map::new();
        account_override.insert("code".to_string(), Value::String(runtime_code.to_string()));
        if !state_diff.is_empty() {
            account_override.insert("stateDiff".to_string(), Value::Object(state_diff));
        }

        let mut overrides = serde_json::Map::new();
        overrides.insert(
            override_address.to_ascii_lowercase(),
            Value::Object(account_override),
        );

        let mut call_object = serde_json::Map::new();
        call_object.insert("from".to_string(), Value::String(from.to_string()));
        call_object.insert("to".to_string(), Value::String(to.to_string()));
        call_object.insert("data".to_string(), Value::String(data.to_string()));
        call_object.insert(
            "gasPrice".to_string(),
            Value::String(format!("0x{gas_price:x}")),
        );
        call_object.insert("value".to_string(), Value::String("0x0".to_string()));

        let options = json!({
            "tracer": "callTracer",
            "tracerConfig": {
                "onlyTopCall": false
            },
            "stateOverrides": Value::Object(overrides)
        });

        self.call(
            "debug_traceCall",
            json!([Value::Object(call_object), tag, options]),
        )
        .await
    }

    /// Fetch a complete pool-state snapshot using Multicall3.aggregate3. The outer
    /// JSON-RPC request count is bounded by `max_calls_per_batch`, while each inner
    /// contract read retains an independent success flag. Callers must treat
    /// `complete == false` as fail-closed and skip route evaluation for that block.
    pub async fn pool_states_multicall_at(
        &self,
        multicall3: &str,
        pools: &[PoolDefinition],
        block: u64,
        max_calls_per_batch: usize,
    ) -> Result<StateSnapshot> {
        let mut specs = Vec::with_capacity(pools.len().saturating_mul(2));
        for (idx, pool) in pools.iter().enumerate() {
            match pool.dex.kind {
                DexKind::V2 => specs.push(StateCallSpec {
                    call: MulticallCall {
                        target: pool.pool_address.clone(),
                        data: "0x0902f1ac".to_string(), // getReserves()
                    },
                    kind: StateCallKind::V2Reserves(idx),
                }),
                DexKind::Solidly => {
                    specs.push(StateCallSpec {
                        call: MulticallCall {
                            target: pool.pool_address.clone(),
                            data: "0x0902f1ac".to_string(), // getReserves()
                        },
                        kind: StateCallKind::V2Reserves(idx),
                    });
                    let selector = ethabi::short_signature(
                        "getFee",
                        &[ParamType::Address, ParamType::Bool],
                    );
                    let mut calldata = selector.to_vec();
                    calldata.extend(abi_encode(&[
                        Token::Address(parse_h160(&pool.pool_address)?),
                        Token::Bool(pool.dex.solidly_stable),
                    ]));
                    specs.push(StateCallSpec {
                        call: MulticallCall {
                            target: pool.dex.factory.clone(),
                            data: format!("0x{}", encode_hex_bytes(&calldata)),
                        },
                        kind: StateCallKind::SolidlyFee(idx),
                    });
                }
                DexKind::V3 => {
                    specs.push(StateCallSpec {
                        call: MulticallCall {
                            target: pool.pool_address.clone(),
                            data: "0x3850c7bd".to_string(), // slot0()
                        },
                        kind: StateCallKind::V3Slot0(idx),
                    });
                    specs.push(StateCallSpec {
                        call: MulticallCall {
                            target: pool.pool_address.clone(),
                            data: "0x1a686502".to_string(), // liquidity()
                        },
                        kind: StateCallKind::V3Liquidity(idx),
                    });
                }
                DexKind::Algebra => {
                    let selector = ethabi::short_signature("globalState", &[]);
                    specs.push(StateCallSpec {
                        call: MulticallCall {
                            target: pool.pool_address.clone(),
                            data: format!("0x{}", encode_hex_bytes(&selector)),
                        },
                        kind: StateCallKind::AlgebraGlobalState(idx),
                    });
                    specs.push(StateCallSpec {
                        call: MulticallCall {
                            target: pool.pool_address.clone(),
                            data: "0x1a686502".to_string(), // liquidity()
                        },
                        kind: StateCallKind::V3Liquidity(idx),
                    });
                }
                DexKind::Slipstream => {
                    specs.push(StateCallSpec {
                        call: MulticallCall {
                            target: pool.pool_address.clone(),
                            data: "0x3850c7bd".to_string(), // slot0()
                        },
                        kind: StateCallKind::V3Slot0(idx),
                    });
                    specs.push(StateCallSpec {
                        call: MulticallCall {
                            target: pool.pool_address.clone(),
                            data: "0x1a686502".to_string(), // liquidity()
                        },
                        kind: StateCallKind::V3Liquidity(idx),
                    });
                    let selector = ethabi::short_signature("getSwapFee", &[ParamType::Address]);
                    let mut calldata = selector.to_vec();
                    calldata.extend(abi_encode(&[Token::Address(parse_h160(&pool.pool_address)?)]));
                    specs.push(StateCallSpec {
                        call: MulticallCall {
                            target: pool.dex.factory.clone(),
                            data: format!("0x{}", encode_hex_bytes(&calldata)),
                        },
                        kind: StateCallKind::SlipstreamFee(idx),
                    });
                }
            }
        }

        let expected_calls = specs.len();
        let mut returned_calls = 0usize;
        let mut inner_failures = 0usize;
        let mut decode_failures = 0usize;
        let mut rpc_batches = 0usize;
        let mut v2_reserves: Vec<Option<(u128, u128)>> = vec![None; pools.len()];
        let mut v3_slot0: Vec<Option<(f64, i32)>> = vec![None; pools.len()];
        let mut v3_liquidity: Vec<Option<f64>> = vec![None; pools.len()];
        // (sqrt_price_x96, tick, fee_bps). Algebra globalState fee is in 1e-6
        // fractional units, i.e. divide by 100 to convert to basis points.
        let mut algebra_global: Vec<Option<(f64, i32, f64)>> = vec![None; pools.len()];
        // Solidly-family PoolFactory.getFee(pool,stable) is already in basis points.
        let mut solidly_fee_bps: Vec<Option<f64>> = vec![None; pools.len()];
        // Slipstream ICLFactory.getSwapFee(pool) is denominated in pips (1e-6).
        let mut slipstream_fee_bps: Vec<Option<f64>> = vec![None; pools.len()];

        for chunk in specs.chunks(max_calls_per_batch.max(1)) {
            let calls = chunk.iter().map(|s| s.call.clone()).collect::<Vec<_>>();
            let results = self.multicall3_at(multicall3, &calls, block).await?;
            rpc_batches += 1;
            returned_calls = returned_calls.saturating_add(results.len());

            if results.len() != chunk.len() {
                decode_failures =
                    decode_failures.saturating_add(chunk.len().abs_diff(results.len()));
            }

            for (spec, result) in chunk.iter().zip(results.iter()) {
                if !result.success {
                    inner_failures += 1;
                    continue;
                }
                let decoded = match spec.kind {
                    StateCallKind::V2Reserves(idx) => decode_u128_word(&result.return_data, 0)
                        .and_then(|r0| decode_u128_word(&result.return_data, 1).map(|r1| (r0, r1)))
                        .map(|value| v2_reserves[idx] = Some(value)),
                    StateCallKind::V3Slot0(idx) => decode_uint_word_f64(&result.return_data, 0)
                        .and_then(|sqrt_price_x96| {
                            decode_i24_word(&result.return_data, 1)
                                .map(|tick| (sqrt_price_x96, tick))
                        })
                        .map(|value| v3_slot0[idx] = Some(value)),
                    StateCallKind::V3Liquidity(idx) => decode_uint_word_f64(&result.return_data, 0)
                        .map(|value| v3_liquidity[idx] = Some(value)),
                    StateCallKind::AlgebraGlobalState(idx) => {
                        decode_uint_word_f64(&result.return_data, 0)
                            .and_then(|sqrt_price_x96| {
                                decode_i24_word(&result.return_data, 1).and_then(|tick| {
                                    decode_u32_word(&result.return_data, 2).map(|fee_raw| {
                                        (sqrt_price_x96, tick, fee_raw as f64 / 100.0)
                                    })
                                })
                            })
                            .map(|value| algebra_global[idx] = Some(value))
                    }
                    StateCallKind::SolidlyFee(idx) => decode_u32_word(&result.return_data, 0)
                        .map(|fee_raw| solidly_fee_bps[idx] = Some(fee_raw as f64)),
                    StateCallKind::SlipstreamFee(idx) => decode_u32_word(&result.return_data, 0)
                        .map(|fee_raw| slipstream_fee_bps[idx] = Some(fee_raw as f64 / 100.0)),
                };
                if decoded.is_err() {
                    decode_failures += 1;
                }
            }
        }

        let mut states = Vec::with_capacity(pools.len());
        let mut empty_or_uninitialized_pools = 0usize;
        for (idx, pool) in pools.iter().cloned().enumerate() {
            match pool.dex.kind {
                DexKind::V2 => {
                    let Some((r0_raw, r1_raw)) = v2_reserves[idx] else {
                        continue;
                    };
                    let reserve0 = r0_raw as f64 / 10f64.powi(pool.token0_decimals as i32);
                    let reserve1 = r1_raw as f64 / 10f64.powi(pool.token1_decimals as i32);
                    if reserve0 <= 0.0 || reserve1 <= 0.0 {
                        empty_or_uninitialized_pools += 1;
                        continue;
                    }
                    states.push(PoolState::V2(V2PoolState { def: pool, reserve0, reserve1 }));
                }
                DexKind::Solidly => {
                    let (Some((r0_raw, r1_raw)), Some(fee_bps)) =
                        (v2_reserves[idx], solidly_fee_bps[idx])
                    else { continue; };
                    let reserve0 = r0_raw as f64 / 10f64.powi(pool.token0_decimals as i32);
                    let reserve1 = r1_raw as f64 / 10f64.powi(pool.token1_decimals as i32);
                    if reserve0 <= 0.0 || reserve1 <= 0.0 || !fee_bps.is_finite() || !(0.0..1000.0).contains(&fee_bps) {
                        empty_or_uninitialized_pools += 1;
                        continue;
                    }
                    let mut def = pool;
                    def.dex.fee_bps = Some(fee_bps);
                    states.push(PoolState::V2(V2PoolState { def, reserve0, reserve1 }));
                }
                DexKind::V3 => {
                    let (Some((sqrt_price_x96, tick)), Some(liquidity)) =
                        (v3_slot0[idx], v3_liquidity[idx])
                    else {
                        continue;
                    };
                    if sqrt_price_x96 <= 0.0 || !liquidity.is_finite() || liquidity <= 0.0 {
                        empty_or_uninitialized_pools += 1;
                        continue;
                    }
                    states.push(PoolState::V3(V3PoolState {
                        def: pool,
                        sqrt_price_x96,
                        tick,
                        liquidity,
                        tick_cache: None,
                    }));
                }
                DexKind::Algebra => {
                    let (Some((sqrt_price_x96, tick, fee_bps)), Some(liquidity)) =
                        (algebra_global[idx], v3_liquidity[idx])
                    else {
                        continue;
                    };
                    if sqrt_price_x96 <= 0.0
                        || !liquidity.is_finite()
                        || liquidity <= 0.0
                        || !fee_bps.is_finite()
                        || !(0.0..1000.0).contains(&fee_bps)
                    {
                        empty_or_uninitialized_pools += 1;
                        continue;
                    }
                    let mut def = pool;
                    def.dex.fee_bps = Some(fee_bps);
                    states.push(PoolState::V3(V3PoolState {
                        def,
                        sqrt_price_x96,
                        tick,
                        liquidity,
                        tick_cache: None,
                    }));
                }
                DexKind::Slipstream => {
                    let (Some((sqrt_price_x96, tick)), Some(liquidity), Some(fee_bps)) =
                        (v3_slot0[idx], v3_liquidity[idx], slipstream_fee_bps[idx])
                    else {
                        continue;
                    };
                    if sqrt_price_x96 <= 0.0
                        || !liquidity.is_finite()
                        || liquidity <= 0.0
                        || !fee_bps.is_finite()
                        || !(0.0..1000.0).contains(&fee_bps)
                    {
                        empty_or_uninitialized_pools += 1;
                        continue;
                    }
                    let mut def = pool;
                    def.dex.fee_bps = Some(fee_bps);
                    states.push(PoolState::V3(V3PoolState {
                        def,
                        sqrt_price_x96,
                        tick,
                        liquidity,
                        tick_cache: None,
                    }));
                }
            }
        }

        let complete =
            returned_calls == expected_calls && inner_failures == 0 && decode_failures == 0;
        Ok(StateSnapshot {
            states,
            expected_calls,
            returned_calls,
            inner_failures,
            decode_failures,
            rpc_batches,
            empty_or_uninitialized_pools,
            complete,
        })
    }

    /// Generic read-only Multicall3 helper for discovery/hydration. Each tuple is
    /// (target, calldata). Inner-call failures are returned as None while outer RPC
    /// failures still fail the batch and use the RpcClient retry/backoff policy.
    pub async fn multicall_read_many_at(
        &self,
        multicall3: &str,
        calls: &[(String, String)],
        block: u64,
        max_calls_per_batch: usize,
    ) -> Result<Vec<Option<String>>> {
        if calls.is_empty() {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity(calls.len());
        for chunk in calls.chunks(max_calls_per_batch.max(1)) {
            let inner = chunk
                .iter()
                .map(|(target, data)| MulticallCall {
                    target: target.clone(),
                    data: data.clone(),
                })
                .collect::<Vec<_>>();
            let results = self.multicall3_at(multicall3, &inner, block).await?;
            if results.len() != chunk.len() {
                bail!(
                    "Multicall3 discovery result count mismatch: expected {}, got {}",
                    chunk.len(),
                    results.len()
                );
            }
            out.extend(results.into_iter().map(|result| {
                if result.success { Some(result.return_data) } else { None }
            }));
        }
        Ok(out)
    }

    async fn multicall3_at(
        &self,
        multicall3: &str,
        calls: &[MulticallCall],
        block: u64,
    ) -> Result<Vec<MulticallResult>> {
        if calls.is_empty() {
            return Ok(Vec::new());
        }

        let mut encoded_calls = Vec::with_capacity(calls.len());
        for call in calls {
            let address = parse_h160(&call.target)?;
            let data = decode_hex_bytes(&call.data)?;
            encoded_calls.push(Token::Tuple(vec![
                Token::Address(address),
                Token::Bool(true), // allowFailure; fail-closed is enforced by the scanner.
                Token::Bytes(data),
            ]));
        }

        // aggregate3((address,bool,bytes)[]) selector = 0x82ad56cb.
        let mut calldata = vec![0x82, 0xad, 0x56, 0xcb];
        calldata.extend(abi_encode(&[Token::Array(encoded_calls)]));
        let raw = self
            .eth_call_at(
                multicall3,
                &format!("0x{}", encode_hex_bytes(&calldata)),
                block,
            )
            .await?;
        let bytes = decode_hex_bytes(&raw)?;
        let decoded = abi_decode(
            &[ParamType::Array(Box::new(ParamType::Tuple(vec![
                ParamType::Bool,
                ParamType::Bytes,
            ])))],
            &bytes,
        )
        .context("failed to decode Multicall3 aggregate3 response")?;

        let Some(Token::Array(items)) = decoded.into_iter().next() else {
            bail!("unexpected Multicall3 aggregate3 return type");
        };
        let mut out = Vec::with_capacity(items.len());
        for item in items {
            let Token::Tuple(mut fields) = item else {
                bail!("unexpected Multicall3 result tuple");
            };
            if fields.len() != 2 {
                bail!("unexpected Multicall3 result tuple length");
            }
            let data_token = fields.pop().expect("length checked");
            let success_token = fields.pop().expect("length checked");
            let Token::Bool(success) = success_token else {
                bail!("unexpected Multicall3 success type");
            };
            let Token::Bytes(return_data) = data_token else {
                bail!("unexpected Multicall3 returnData type");
            };
            out.push(MulticallResult {
                success,
                return_data: format!("0x{}", encode_hex_bytes(&return_data)),
            });
        }
        Ok(out)
    }

    pub async fn get_pair(&self, factory: &str, token_a: &str, token_b: &str) -> Result<String> {
        // getPair(address,address) -> 0xe6a43905
        let data = format!(
            "0xe6a43905{}{}",
            encode_address_word(token_a)?,
            encode_address_word(token_b)?
        );
        let raw = self.eth_call(factory, &data).await?;
        decode_address_word(&raw, 0)
    }

    /// SyncSwap BasePoolFactory exposes getPool(address,address), unlike
    /// Uniswap V2's getPair(address,address).
    pub async fn get_syncswap_pool(&self, factory: &str, token_a: &str, token_b: &str) -> Result<String> {
        let selector = ethabi::short_signature("getPool", &[ParamType::Address, ParamType::Address]);
        let mut calldata = selector.to_vec();
        calldata.extend(abi_encode(&[
            Token::Address(parse_h160(token_a)?),
            Token::Address(parse_h160(token_b)?),
        ]));
        let raw = self
            .eth_call(factory, &format!("0x{}", encode_hex_bytes(&calldata)))
            .await?;
        decode_address_word(&raw, 0)
    }

    pub async fn get_v3_pool(
        &self,
        factory: &str,
        token_a: &str,
        token_b: &str,
        fee_tier: u32,
    ) -> Result<String> {
        // getPool(address,address,uint24) -> 0x1698ee82
        let data = format!(
            "0x1698ee82{}{}{}",
            encode_address_word(token_a)?,
            encode_address_word(token_b)?,
            encode_u32_word(fee_tier)
        );
        let raw = self.eth_call(factory, &data).await?;
        decode_address_word(&raw, 0)
    }

    pub async fn get_algebra_pool(
        &self,
        factory: &str,
        token_a: &str,
        token_b: &str,
    ) -> Result<String> {
        // AlgebraFactory.poolByPair(address,address)
        let selector =
            ethabi::short_signature("poolByPair", &[ParamType::Address, ParamType::Address]);
        let mut calldata = selector.to_vec();
        calldata.extend(abi_encode(&[
            Token::Address(parse_h160(token_a)?),
            Token::Address(parse_h160(token_b)?),
        ]));
        let raw = self
            .eth_call(factory, &format!("0x{}", encode_hex_bytes(&calldata)))
            .await?;
        decode_address_word(&raw, 0)
    }

    /// Discover an Aerodrome/Velodrome classic pool. Solidly-family factories key
    /// volatile/basic and stable pools with a boolean.
    pub async fn get_solidly_pool(
        &self,
        factory: &str,
        token_a: &str,
        token_b: &str,
        stable: bool,
    ) -> Result<String> {
        let selector = ethabi::short_signature(
            "getPool",
            &[ParamType::Address, ParamType::Address, ParamType::Bool],
        );
        let mut calldata = selector.to_vec();
        calldata.extend(abi_encode(&[
            Token::Address(parse_h160(token_a)?),
            Token::Address(parse_h160(token_b)?),
            Token::Bool(stable),
        ]));
        let raw = self
            .eth_call(factory, &format!("0x{}", encode_hex_bytes(&calldata)))
            .await?;
        decode_address_word(&raw, 0)
    }

    /// Current classic pool fee in basis points. Aerodrome/Velodrome factories may
    /// apply per-pool custom fees, so V3.3 reads it instead of assuming a default.
    pub async fn solidly_fee_bps(
        &self,
        factory: &str,
        pool: &str,
        stable: bool,
    ) -> Result<f64> {
        let selector = ethabi::short_signature(
            "getFee",
            &[ParamType::Address, ParamType::Bool],
        );
        let mut calldata = selector.to_vec();
        calldata.extend(abi_encode(&[
            Token::Address(parse_h160(pool)?),
            Token::Bool(stable),
        ]));
        let raw = self
            .eth_call(factory, &format!("0x{}", encode_hex_bytes(&calldata)))
            .await?;
        let fee = decode_u128_word(&raw, 0)? as f64;
        if !fee.is_finite() || !(0.0..1000.0).contains(&fee) {
            bail!("invalid Solidly pool fee returned by factory: {fee}");
        }
        Ok(fee)
    }

    /// Exact read-only quote from an Aerodrome/Velodrome classic pool. Calling the
    /// pool itself preserves the live dynamic/custom fee and avoids pretending this
    /// venue is routed through a Uniswap V2 router.
    pub async fn quote_solidly_exact_input_at(
        &self,
        pool: &str,
        token_in: &str,
        amount_in_raw: u128,
        block: u64,
    ) -> Result<u128> {
        let selector = ethabi::short_signature(
            "getAmountOut",
            &[ParamType::Uint(256), ParamType::Address],
        );
        let mut calldata = selector.to_vec();
        calldata.extend(abi_encode(&[
            Token::Uint(amount_in_raw.into()),
            Token::Address(parse_h160(token_in)?),
        ]));
        let raw = self
            .eth_call_at(pool, &format!("0x{}", encode_hex_bytes(&calldata)), block)
            .await?;
        decode_u128_word(&raw, 0)
    }

    /// Exact read-only SyncSwap Classic quote. The pool's own view function applies
    /// the master-controlled live fee, so the breadth marginal stage may remain an
    /// optimistic upper bound while survivor verification stays exact.
    pub async fn quote_syncswap_classic_exact_input_at(
        &self,
        pool: &str,
        token_in: &str,
        amount_in_raw: u128,
        block: u64,
    ) -> Result<u128> {
        let selector = ethabi::short_signature(
            "getAmountOut",
            &[ParamType::Address, ParamType::Uint(256), ParamType::Address],
        );
        let mut calldata = selector.to_vec();
        calldata.extend(abi_encode(&[
            Token::Address(parse_h160(token_in)?),
            Token::Uint(amount_in_raw.into()),
            Token::Address(H160::zero()),
        ]));
        let raw = self
            .eth_call_at(pool, &format!("0x{}", encode_hex_bytes(&calldata)), block)
            .await?;
        decode_u128_word(&raw, 0)
    }

    /// Enabled Slipstream tick spacings. ICLFactory guarantees this list is append-only,
    /// making it safe to cache with the discovered pool universe.
    pub async fn slipstream_tick_spacings(&self, factory: &str) -> Result<Vec<i32>> {
        let selector = ethabi::short_signature("tickSpacings", &[]);
        let raw = self.eth_call(factory, &format!("0x{}", encode_hex_bytes(&selector))).await?;
        let bytes = decode_hex_bytes(&raw)?;
        let decoded = abi_decode(&[ParamType::Array(Box::new(ParamType::Int(24)))], &bytes)
            .context("failed to decode Slipstream tickSpacings()")?;
        let arr = match decoded.into_iter().next() {
            Some(Token::Array(values)) => values,
            _ => bail!("Slipstream tickSpacings() did not return an array"),
        };
        let mut out = Vec::with_capacity(arr.len());
        for token in arr {
            let raw = match token {
                Token::Int(value) => value,
                _ => bail!("invalid Slipstream tick spacing"),
            };
            let spacing = raw.low_u32() as i32;
            if spacing <= 0 || spacing > 8_388_607 {
                bail!("invalid Slipstream tick spacing returned by factory: {spacing}");
            }
            out.push(spacing);
        }
        out.sort_unstable();
        out.dedup();
        Ok(out)
    }

    pub async fn get_slipstream_pool(
        &self,
        factory: &str,
        token_a: &str,
        token_b: &str,
        tick_spacing: i32,
    ) -> Result<String> {
        if tick_spacing <= 0 { bail!("Slipstream tick spacing must be positive"); }
        let selector = ethabi::short_signature(
            "getPool",
            &[ParamType::Address, ParamType::Address, ParamType::Int(24)],
        );
        let mut calldata = selector.to_vec();
        calldata.extend(abi_encode(&[
            Token::Address(parse_h160(token_a)?),
            Token::Address(parse_h160(token_b)?),
            Token::Int(U256::from(tick_spacing as u32)),
        ]));
        let raw = self.eth_call(factory, &format!("0x{}", encode_hex_bytes(&calldata))).await?;
        decode_address_word(&raw, 0)
    }

    /// Factory-specific Slipstream QuoterV2 exact-input single quote.
    pub async fn quote_slipstream_exact_input_at(
        &self,
        quoter: &str,
        token_in: &str,
        token_out: &str,
        amount_in_raw: u128,
        tick_spacing: i32,
        block: u64,
    ) -> Result<u128> {
        let tuple = ParamType::Tuple(vec![
            ParamType::Address,
            ParamType::Address,
            ParamType::Uint(256),
            ParamType::Int(24),
            ParamType::Uint(160),
        ]);
        let selector = ethabi::short_signature("quoteExactInputSingle", &[tuple]);
        let mut calldata = selector.to_vec();
        calldata.extend(abi_encode(&[Token::Tuple(vec![
            Token::Address(parse_h160(token_in)?),
            Token::Address(parse_h160(token_out)?),
            Token::Uint(amount_in_raw.into()),
            Token::Int(U256::from(tick_spacing as u32)),
            Token::Uint(U256::zero()),
        ])]));
        let raw = self.eth_call_at(quoter, &format!("0x{}", encode_hex_bytes(&calldata)), block).await?;
        decode_u128_word(&raw, 0)
    }

    /// MixedRouteQuoterV3 exact-input single quote. The configured factory mask is
    /// ORed into tickSpacing exactly as the official quoter specifies: 1<<20 for the
    /// original CL factory, 1<<19 for the newest, and 0 for legacyCLFactory2.
    pub async fn quote_slipstream_mixed_v3_exact_input_at(
        &self,
        quoter: &str,
        token_in: &str,
        token_out: &str,
        amount_in_raw: u128,
        tick_spacing: i32,
        factory_mask: i32,
        block: u64,
    ) -> Result<u128> {
        let encoded_spacing = tick_spacing | factory_mask;
        if encoded_spacing <= 0 || encoded_spacing > 8_388_607 { bail!("invalid encoded Slipstream tick spacing"); }
        let tuple = ParamType::Tuple(vec![
            ParamType::Address,
            ParamType::Address,
            ParamType::Uint(256),
            ParamType::Int(24),
            ParamType::Uint(160),
        ]);
        let selector = ethabi::short_signature("quoteExactInputSingleV3", &[tuple]);
        let mut calldata = selector.to_vec();
        calldata.extend(abi_encode(&[Token::Tuple(vec![
            Token::Address(parse_h160(token_in)?),
            Token::Address(parse_h160(token_out)?),
            Token::Uint(amount_in_raw.into()),
            Token::Int(U256::from(encoded_spacing as u32)),
            Token::Uint(U256::zero()),
        ])]));
        let raw = self.eth_call_at(quoter, &format!("0x{}", encode_hex_bytes(&calldata)), block).await?;
        decode_u128_word(&raw, 0)
    }

    pub async fn token0(&self, pair: &str) -> Result<String> {
        let raw = self.eth_call(pair, "0x0dfe1681").await?;
        decode_address_word(&raw, 0)
    }

    pub async fn token1(&self, pair: &str) -> Result<String> {
        let raw = self.eth_call(pair, "0xd21220a7").await?;
        decode_address_word(&raw, 0)
    }

    pub async fn reserves_at(&self, pair: &str, block: u64) -> Result<(u128, u128)> {
        // getReserves() -> 0x0902f1ac
        let raw = self.eth_call_at(pair, "0x0902f1ac", block).await?;
        let r0 = decode_u128_word(&raw, 0)?;
        let r1 = decode_u128_word(&raw, 1)?;
        Ok((r0, r1))
    }

    pub async fn v3_slot0_at(&self, pool: &str, block: u64) -> Result<(f64, i32)> {
        // slot0() -> 0x3850c7bd
        let raw = self.eth_call_at(pool, "0x3850c7bd", block).await?;
        let sqrt_price_x96 = decode_uint_word_f64(&raw, 0)?;
        let tick = decode_i24_word(&raw, 1)?;
        Ok((sqrt_price_x96, tick))
    }

    pub async fn v3_liquidity_at(&self, pool: &str, block: u64) -> Result<f64> {
        // liquidity() -> 0x1a686502
        let raw = self.eth_call_at(pool, "0x1a686502", block).await?;
        decode_uint_word_f64(&raw, 0)
    }

    /// Load every initialized tick in one V3 bitmap word through Uniswap's TickLens.
    /// This replaces the old bitmap -> N individual ticks() RPC waterfall with one eth_call.
    pub async fn v3_populated_ticks_in_word_at(
        &self,
        tick_lens: &str,
        pool: &str,
        word_pos: i16,
        block: u64,
    ) -> Result<Vec<(i32, i128, u128)>> {
        // getPopulatedTicksInWord(address,int16) -> 0x351fb478. V0.2 keeps the
        // TickLens address per V3 venue so Uniswap- and Pancake-style deployments
        // can use their own official periphery while sharing the same local math.
        let data = format!(
            "0x351fb478{}{}",
            encode_address_word(pool)?,
            encode_i16_word(word_pos),
        );
        let raw = self.eth_call_at(tick_lens, &data, block).await?;
        decode_tick_lens_ticks(&raw)
    }

    pub async fn quote_v3_exact_input_at(
        &self,
        quoter: &str,
        token_in: &str,
        token_out: &str,
        amount_in_raw: u128,
        fee_tier: u32,
        block: u64,
    ) -> Result<V3Quote> {
        // QuoterV2.quoteExactInputSingle((address,address,uint256,uint24,uint160))
        // selector 0xc6a5026a. sqrtPriceLimitX96 = 0 means no explicit limit.
        let data = format!(
            "0xc6a5026a{}{}{}{}{}",
            encode_address_word(token_in)?,
            encode_address_word(token_out)?,
            encode_u128_word(amount_in_raw),
            encode_u32_word(fee_tier),
            encode_u128_word(0),
        );
        let raw = self.eth_call_at(quoter, &data, block).await?;
        Ok(V3Quote {
            amount_out_raw: decode_u128_word(&raw, 0)?,
            initialized_ticks_crossed: decode_u32_word(&raw, 2)?,
            gas_estimate: decode_u128_word(&raw, 3)?,
        })
    }

    pub async fn quote_algebra_exact_input_at(
        &self,
        quoter: &str,
        token_in: &str,
        token_out: &str,
        amount_in_raw: u128,
        block: u64,
    ) -> Result<(u128, u16)> {
        // Algebra V1.9 Quoter.quoteExactInputSingle(address,address,uint256,uint160)
        // returns (amountOut, fee). limitSqrtPrice=0 means no explicit price limit.
        let selector = ethabi::short_signature(
            "quoteExactInputSingle",
            &[
                ParamType::Address,
                ParamType::Address,
                ParamType::Uint(256),
                ParamType::Uint(160),
            ],
        );
        let mut calldata = selector.to_vec();
        calldata.extend(abi_encode(&[
            Token::Address(parse_h160(token_in)?),
            Token::Address(parse_h160(token_out)?),
            Token::Uint(amount_in_raw.into()),
            Token::Uint(U256::zero()),
        ]));
        let raw = self
            .eth_call_at(
                quoter,
                &format!("0x{}", encode_hex_bytes(&calldata)),
                block,
            )
            .await?;
        let amount_out = decode_u128_word(&raw, 0)?;
        let fee_raw = decode_u32_word(&raw, 1)?;
        let fee =
            u16::try_from(fee_raw).map_err(|_| anyhow!("Algebra quote fee exceeds uint16"))?;
        Ok((amount_out, fee))
    }

    /// Query Curve's official RateProvider for every Curve pool that can quote an exact input.
    /// The RateProvider uses the MetaRegistry, so legacy and current Curve pool families are
    /// discovered behind one read-only interface.
    pub async fn curve_quotes_at(
        &self,
        rate_provider: &str,
        token_in: &str,
        token_out: &str,
        amount_in_raw: u128,
        block: u64,
    ) -> Result<Vec<CurvePoolQuote>> {
        let selector = ethabi::short_signature(
            "get_quotes",
            &[ParamType::Address, ParamType::Address, ParamType::Uint(256)],
        );
        let mut calldata = selector.to_vec();
        calldata.extend(abi_encode(&[
            Token::Address(parse_h160(token_in)?),
            Token::Address(parse_h160(token_out)?),
            Token::Uint(amount_in_raw.into()),
        ]));
        let raw = self
            .eth_call_at(
                rate_provider,
                &format!("0x{}", encode_hex_bytes(&calldata)),
                block,
            )
            .await?;
        let bytes = decode_hex_bytes(&raw)?;
        let tuple = ParamType::Tuple(vec![
            ParamType::Uint(256),
            ParamType::Uint(256),
            ParamType::Bool,
            ParamType::Uint(256),
            ParamType::Address,
            ParamType::Uint(256),
            ParamType::Uint(256),
            ParamType::Uint(8),
        ]);
        let decoded = abi_decode(&[ParamType::Array(Box::new(tuple))], &bytes)
            .context("failed to decode Curve RateProvider get_quotes response")?;
        let Some(Token::Array(items)) = decoded.into_iter().next() else {
            bail!("unexpected Curve RateProvider return type");
        };
        let mut out = Vec::with_capacity(items.len());
        for item in items {
            let Token::Tuple(fields) = item else {
                bail!("unexpected Curve quote tuple");
            };
            if fields.len() != 8 {
                bail!("unexpected Curve quote tuple length");
            }
            let as_u128 = |token: &Token, name: &str| -> Result<u128> {
                let Token::Uint(value) = token else {
                    bail!("Curve {name} is not uint")
                };
                if value.bits() > 128 {
                    bail!("Curve {name} exceeds u128")
                }
                Ok(value.low_u128())
            };
            let as_u32 = |token: &Token, name: &str| -> Result<u32> {
                let value = as_u128(token, name)?;
                u32::try_from(value).with_context(|| format!("Curve {name} exceeds u32"))
            };
            let source_index = as_u32(&fields[0], "source_index")?;
            let dest_index = as_u32(&fields[1], "dest_index")?;
            let Token::Bool(is_underlying) = &fields[2] else {
                bail!("Curve is_underlying is not bool");
            };
            let is_underlying = *is_underlying;
            let amount_out_raw = as_u128(&fields[3], "amount_out")?;
            let Token::Address(pool) = &fields[4] else {
                bail!("Curve pool is not address");
            };
            let source_balance_raw = as_u128(&fields[5], "source_balance")?;
            let dest_balance_raw = as_u128(&fields[6], "dest_balance")?;
            let pool_type = u8::try_from(as_u32(&fields[7], "pool_type")?)
                .context("Curve pool_type exceeds u8")?;
            out.push(CurvePoolQuote {
                source_index,
                dest_index,
                is_underlying,
                amount_out_raw,
                pool: format!("0x{}", encode_hex_bytes(pool.as_bytes())),
                source_balance_raw,
                dest_balance_raw,
                pool_type,
            });
        }
        Ok(out)
    }

    /// Replays a Curve RateProvider leg directly against the quoted pool at the same block.
    /// StableSwap pools use signed int128 indices (and optionally get_dy_underlying), while
    /// CryptoSwap pools use uint256 indices. The returned amount is the pool's own exact
    /// view result and is therefore authoritative for V0.3.2 route verification.
    pub async fn curve_direct_quote_at(
        &self,
        pool: &str,
        source_index: u32,
        dest_index: u32,
        is_underlying: bool,
        pool_type: u8,
        amount_in_raw: u128,
        block: u64,
    ) -> Result<u128> {
        let (name, params, args) = match pool_type {
            0 => {
                let name = if is_underlying {
                    "get_dy_underlying"
                } else {
                    "get_dy"
                };
                (
                    name,
                    vec![
                        ParamType::Int(128),
                        ParamType::Int(128),
                        ParamType::Uint(256),
                    ],
                    vec![
                        Token::Int(U256::from(source_index)),
                        Token::Int(U256::from(dest_index)),
                        Token::Uint(amount_in_raw.into()),
                    ],
                )
            }
            1 => (
                "get_dy",
                vec![
                    ParamType::Uint(256),
                    ParamType::Uint(256),
                    ParamType::Uint(256),
                ],
                vec![
                    Token::Uint(source_index.into()),
                    Token::Uint(dest_index.into()),
                    Token::Uint(amount_in_raw.into()),
                ],
            ),
            2 => bail!("Curve LLAMMA direct quotes are outside the V0.3.2 universe"),
            other => bail!("unsupported Curve pool_type {other}"),
        };

        let selector = ethabi::short_signature(name, &params);
        let mut calldata = selector.to_vec();
        calldata.extend(abi_encode(&args));
        let raw = self
            .eth_call_at(pool, &format!("0x{}", encode_hex_bytes(&calldata)), block)
            .await
            .with_context(|| format!(
                "Curve direct {name} failed pool={pool} i={source_index} j={dest_index} underlying={is_underlying}"
            ))?;
        decode_u128_word(&raw, 0).context("failed to decode direct Curve get_dy output")
    }

    /// Diagnostic-only Curve direct quote with explicit EVM gasPrice context.
    /// This intentionally mirrors curve_direct_quote_at but fixes is_underlying=false,
    /// matching the production V2/V3 executor policy that forbids exchange_underlying.
    pub async fn curve_direct_quote_at_with_tx_gas_price(
        &self,
        pool: &str,
        source_index: u32,
        dest_index: u32,
        pool_type: u8,
        amount_in_raw: u128,
        block: u64,
        from: &str,
        gas_price_wei: u128,
    ) -> Result<u128> {
        let (name, params, args) = match pool_type {
            0 => (
                "get_dy",
                vec![
                    ParamType::Int(128),
                    ParamType::Int(128),
                    ParamType::Uint(256),
                ],
                vec![
                    Token::Int(U256::from(source_index)),
                    Token::Int(U256::from(dest_index)),
                    Token::Uint(amount_in_raw.into()),
                ],
            ),
            1 => (
                "get_dy",
                vec![
                    ParamType::Uint(256),
                    ParamType::Uint(256),
                    ParamType::Uint(256),
                ],
                vec![
                    Token::Uint(source_index.into()),
                    Token::Uint(dest_index.into()),
                    Token::Uint(amount_in_raw.into()),
                ],
            ),
            2 => bail!("Curve LLAMMA direct quotes are outside the V3.0.4e diagnostic universe"),
            other => bail!("unsupported Curve pool_type {other}"),
        };

        let selector = ethabi::short_signature(name, &params);
        let mut calldata = selector.to_vec();
        calldata.extend(abi_encode(&args));
        let raw = self
            .eth_call_at_with_from_and_gas_price(
                from,
                pool,
                &format!("0x{}", encode_hex_bytes(&calldata)),
                block,
                gas_price_wei,
            )
            .await
            .with_context(|| format!(
                "Curve direct {name} gas-price probe failed pool={pool} i={source_index} j={dest_index} gasPrice={gas_price_wei}"
            ))?;
        decode_u128_word(&raw, 0).context("failed to decode gas-price-context Curve get_dy output")
    }

    /// Production-safety Curve direct quote with explicit EVM gasPrice context.
    /// Unlike the diagnostic variant above this leaves `from` unspecified, matching
    /// the scanner baseline except for GASPRICE itself.
    pub async fn curve_direct_quote_at_with_gas_price(
        &self,
        pool: &str,
        source_index: u32,
        dest_index: u32,
        is_underlying: bool,
        pool_type: u8,
        amount_in_raw: u128,
        block: u64,
        gas_price_wei: u128,
    ) -> Result<u128> {
        let (name, params, args) = match pool_type {
            0 => {
                let name = if is_underlying {
                    "get_dy_underlying"
                } else {
                    "get_dy"
                };
                (
                    name,
                    vec![
                        ParamType::Int(128),
                        ParamType::Int(128),
                        ParamType::Uint(256),
                    ],
                    vec![
                        Token::Int(U256::from(source_index)),
                        Token::Int(U256::from(dest_index)),
                        Token::Uint(amount_in_raw.into()),
                    ],
                )
            }
            1 => (
                "get_dy",
                vec![
                    ParamType::Uint(256),
                    ParamType::Uint(256),
                    ParamType::Uint(256),
                ],
                vec![
                    Token::Uint(source_index.into()),
                    Token::Uint(dest_index.into()),
                    Token::Uint(amount_in_raw.into()),
                ],
            ),
            2 => bail!("Curve LLAMMA direct quotes are outside the V3.0.4f safety universe"),
            other => bail!("unsupported Curve pool_type {other}"),
        };

        let selector = ethabi::short_signature(name, &params);
        let mut calldata = selector.to_vec();
        calldata.extend(abi_encode(&args));
        let raw = self
            .eth_call_at_with_gas_price(
                pool,
                &format!("0x{}", encode_hex_bytes(&calldata)),
                block,
                gas_price_wei,
            )
            .await
            .with_context(|| format!(
                "Curve direct {name} gas-price safety probe failed pool={pool} i={source_index} j={dest_index} underlying={is_underlying} gasPrice={gas_price_wei}"
            ))?;
        decode_u128_word(&raw, 0).context("failed to decode gas-price-context Curve get_dy output")
    }

    /// Returns the actual token stored at a Curve pool coin index. V0.3.2 uses
    /// this to ensure a supposedly direct spot route does not hide a lending/yield wrapper.
    pub async fn curve_pool_coin_at(&self, pool: &str, index: u32, block: u64) -> Result<String> {
        let selector = ethabi::short_signature("coins", &[ParamType::Uint(256)]);
        let mut calldata = selector.to_vec();
        calldata.extend(abi_encode(&[Token::Uint(index.into())]));
        let raw = self
            .eth_call_at(pool, &format!("0x{}", encode_hex_bytes(&calldata)), block)
            .await
            .with_context(|| format!("Curve coins({index}) failed pool={pool}"))?;
        decode_address_word(&raw, 0).context("failed to decode Curve pool coin address")
    }

    /// Resolve a Balancer V2 pool address to its bytes32 poolId when the SOR API
    /// returns an address instead of a full pool id.
    pub async fn balancer_v2_pool_id_at(&self, pool: &str, block: u64) -> Result<String> {
        let selector = ethabi::short_signature("getPoolId", &[]);
        let raw = self
            .eth_call_at(pool, &format!("0x{}", encode_hex_bytes(&selector)), block)
            .await
            .with_context(|| format!("Balancer V2 getPoolId failed pool={pool}"))?;
        let bytes = decode_hex_bytes(&raw)?;
        if bytes.len() < 32 {
            bail!("Balancer getPoolId returned {} bytes", bytes.len());
        }
        Ok(format!("0x{}", encode_hex_bytes(&bytes[..32])))
    }

    /// Same-block on-chain Balancer V2 dry run. queryBatchSwap executes the same
    /// pool swap hooks/balance math as batchSwap and returns the simulated Vault
    /// asset deltas without requiring token balances or approvals.
    ///
    /// Each returned tuple is (is_negative, magnitude). For GIVEN_IN swaps the
    /// final output token should have a negative Vault delta.
    pub async fn balancer_v2_query_batch_swap_at(
        &self,
        vault: &str,
        swaps: &[BalancerV2SwapStep],
        assets: &[String],
        block: u64,
    ) -> Result<Vec<(bool, u128)>> {
        if swaps.is_empty() || assets.len() < 2 {
            bail!("Balancer queryBatchSwap requires swaps and at least two assets");
        }
        let swap_param = ParamType::Tuple(vec![
            ParamType::FixedBytes(32),
            ParamType::Uint(256),
            ParamType::Uint(256),
            ParamType::Uint(256),
            ParamType::Bytes,
        ]);
        let funds_param = ParamType::Tuple(vec![
            ParamType::Address,
            ParamType::Bool,
            ParamType::Address,
            ParamType::Bool,
        ]);
        let selector = ethabi::short_signature(
            "queryBatchSwap",
            &[
                ParamType::Uint(8),
                ParamType::Array(Box::new(swap_param.clone())),
                ParamType::Array(Box::new(ParamType::Address)),
                funds_param.clone(),
            ],
        );

        let mut swap_tokens = Vec::with_capacity(swaps.len());
        for step in swaps {
            let pool_id = decode_hex_bytes(&step.pool_id)?;
            if pool_id.len() != 32 {
                bail!("invalid Balancer V2 poolId length: {}", step.pool_id);
            }
            swap_tokens.push(Token::Tuple(vec![
                Token::FixedBytes(pool_id),
                Token::Uint(U256::from(step.asset_in_index as u64)),
                Token::Uint(U256::from(step.asset_out_index as u64)),
                Token::Uint(step.amount_raw.into()),
                Token::Bytes(Vec::new()),
            ]));
        }
        let asset_tokens = assets
            .iter()
            .map(|a| parse_h160(a).map(Token::Address))
            .collect::<Result<Vec<_>>>()?;
        let zero = H160::zero();
        let args = vec![
            Token::Uint(U256::zero()), // GIVEN_IN
            Token::Array(swap_tokens),
            Token::Array(asset_tokens),
            Token::Tuple(vec![
                Token::Address(zero),
                Token::Bool(false),
                Token::Address(zero),
                Token::Bool(false),
            ]),
        ];
        let mut calldata = selector.to_vec();
        calldata.extend(abi_encode(&args));
        let raw = self
            .eth_call_at(vault, &format!("0x{}", encode_hex_bytes(&calldata)), block)
            .await
            .context("Balancer V2 queryBatchSwap eth_call failed")?;
        let bytes = decode_hex_bytes(&raw)?;
        let decoded = abi_decode(&[ParamType::Array(Box::new(ParamType::Int(256)))], &bytes)
            .context("failed to decode Balancer queryBatchSwap int256[]")?;
        let Token::Array(items) = &decoded[0] else {
            bail!("Balancer queryBatchSwap result is not an array");
        };
        items.iter().map(decode_signed_u256_magnitude).collect()
    }

    pub async fn chainlink_price_at(&self, feed: &str, decimals: u8, block: u64) -> Result<f64> {
        // latestRoundData() -> 0xfeaf968c. answer is word #1.
        let raw = self.eth_call_at(feed, "0xfeaf968c", block).await?;
        let answer = decode_u128_word(&raw, 1)?;
        Ok(answer as f64 / 10f64.powi(decimals as i32))
    }
}

fn decode_signed_u256_magnitude(token: &Token) -> Result<(bool, u128)> {
    let Token::Int(raw) = token else {
        bail!("expected int256 token");
    };
    let negative = raw.bit(255);
    let magnitude = if negative {
        (!*raw).overflowing_add(U256::one()).0
    } else {
        *raw
    };
    if magnitude > U256::from(u128::MAX) {
        bail!("signed int256 magnitude exceeds u128");
    }
    Ok((negative, magnitude.as_u128()))
}

fn parse_h160(address: &str) -> Result<H160> {
    let bytes = decode_hex_bytes(address)?;
    if bytes.len() != 20 {
        bail!("invalid address length: {address}");
    }
    Ok(H160::from_slice(&bytes))
}

fn decode_hex_bytes(raw: &str) -> Result<Vec<u8>> {
    let s = clean_hex(raw);
    if s.len() % 2 != 0 {
        bail!("hex string has odd length");
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    for i in (0..s.len()).step_by(2) {
        out.push(u8::from_str_radix(&s[i..i + 2], 16).context("invalid hex byte")?);
    }
    Ok(out)
}

fn encode_hex_bytes(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn encode_address_word(address: &str) -> Result<String> {
    let s = address.strip_prefix("0x").unwrap_or(address);
    if s.len() != 40 || !s.chars().all(|c| c.is_ascii_hexdigit()) {
        bail!("invalid address: {address}");
    }
    Ok(format!("{:0>64}", s.to_ascii_lowercase()))
}

fn encode_i16_word(value: i16) -> String {
    if value >= 0 {
        format!("{:064x}", value as u16)
    } else {
        let low = value as u16;
        format!("{}{:04x}", "f".repeat(60), low)
    }
}

fn encode_i24_word(value: i32) -> String {
    debug_assert!((-8_388_608..=8_388_607).contains(&value));
    if value >= 0 {
        format!("{:064x}", value as u32)
    } else {
        let low = ((1i64 << 24) + value as i64) as u32;
        format!("{}{:06x}", "f".repeat(58), low)
    }
}

fn encode_u32_word(value: u32) -> String {
    format!("{value:064x}")
}

fn encode_u128_word(value: u128) -> String {
    format!("{value:064x}")
}

fn clean_hex(raw: &str) -> &str {
    raw.strip_prefix("0x").unwrap_or(raw)
}

fn word(raw: &str, index: usize) -> Result<&str> {
    let s = clean_hex(raw);
    let start = index * 64;
    let end = start + 64;
    if s.len() < end {
        bail!(
            "ABI result too short: wanted word {index}, got {} hex chars",
            s.len()
        );
    }
    Ok(&s[start..end])
}

fn decode_address_word(raw: &str, index: usize) -> Result<String> {
    let w = word(raw, index)?;
    Ok(format!("0x{}", &w[24..64]))
}

fn decode_u128_word(raw: &str, index: usize) -> Result<u128> {
    let w = word(raw, index)?;
    if w[..32].chars().any(|c| c != '0') {
        bail!("uint256 word does not fit into u128 at ABI word {index}");
    }
    u128::from_str_radix(&w[32..64], 16).context("failed to decode uint word")
}

fn decode_u32_word(raw: &str, index: usize) -> Result<u32> {
    let w = word(raw, index)?;
    u32::from_str_radix(&w[56..64], 16).context("failed to decode uint32 word")
}

fn decode_i128_word(raw: &str, index: usize) -> Result<i128> {
    let w = word(raw, index)?;
    // int128 is sign-extended to 256 bits. The low 128 bits carry the two's-complement value.
    let low = u128::from_str_radix(&w[32..64], 16).context("failed to decode int128 word")?;
    Ok(low as i128)
}

fn decode_tick_lens_ticks(raw: &str) -> Result<Vec<(i32, i128, u128)>> {
    // ABI return type is a single dynamic array of static 3-word tuples:
    // (int24 tick, int128 liquidityNet, uint128 liquidityGross)[].
    let offset_bytes = usize::try_from(decode_u128_word(raw, 0)?)
        .context("TickLens ABI offset does not fit usize")?;
    if offset_bytes % 32 != 0 {
        bail!("invalid TickLens ABI array offset");
    }
    let array_word = offset_bytes / 32;
    let len = usize::try_from(decode_u128_word(raw, array_word)?)
        .context("TickLens array length does not fit usize")?;
    if len > 256 {
        bail!("TickLens returned impossible word population: {len}");
    }

    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        let base = array_word + 1 + i * 3;
        let tick = decode_i24_word(raw, base)?;
        let liquidity_net = decode_i128_word(raw, base + 1)?;
        let liquidity_gross = decode_u128_word(raw, base + 2)?;
        out.push((tick, liquidity_net, liquidity_gross));
    }
    Ok(out)
}

fn decode_i24_word(raw: &str, index: usize) -> Result<i32> {
    let w = word(raw, index)?;
    let raw24 = u32::from_str_radix(&w[58..64], 16).context("failed to decode int24 word")?;
    if raw24 & 0x80_0000 != 0 {
        Ok(raw24 as i32 - (1 << 24))
    } else {
        Ok(raw24 as i32)
    }
}

fn decode_uint_word_f64(raw: &str, index: usize) -> Result<f64> {
    let w = word(raw, index)?;
    let mut value = 0.0f64;
    for byte in w.as_bytes() {
        let digit = match byte {
            b'0'..=b'9' => (byte - b'0') as u8,
            b'a'..=b'f' => (byte - b'a' + 10) as u8,
            b'A'..=b'F' => (byte - b'A' + 10) as u8,
            _ => bail!("invalid hex digit in ABI word"),
        };
        value = value * 16.0 + digit as f64;
    }
    Ok(value)
}

fn parse_hex_u64(raw: &str) -> Result<u64> {
    u64::from_str_radix(clean_hex(raw), 16).context("failed to parse hex u64")
}

fn parse_hex_u128(raw: &str) -> Result<u128> {
    u128::from_str_radix(clean_hex(raw), 16).context("failed to parse hex u128")
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

fn parse_retry_after_ms(raw: &str) -> Option<u64> {
    // Retry-After may also be an HTTP date. For V0.2a we honor the common
    // delta-seconds form and fall back to exponential backoff otherwise.
    raw.trim()
        .parse::<u64>()
        .ok()
        .map(|seconds| seconds.saturating_mul(1000))
}

fn is_json_rpc_rate_limit(err: &Value) -> bool {
    err.get("code").and_then(Value::as_i64) == Some(429)
        || err
            .get("message")
            .and_then(Value::as_str)
            .map(|m| m.to_ascii_lowercase().contains("rate limit") || m.contains("429"))
            .unwrap_or(false)
}

fn safe_reqwest_error_kind(err: &reqwest::Error) -> &'static str {
    if err.is_timeout() {
        "timeout"
    } else if err.is_connect() {
        "connection error"
    } else if err.is_decode() {
        "response decode error"
    } else if err.is_body() {
        "response body error"
    } else if err.is_request() {
        "request construction/transport error"
    } else {
        "transport error"
    }
}

fn is_retryable_http(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

fn is_retryable_json_rpc_error(err: &Value) -> bool {
    let code = err.get("code").and_then(Value::as_i64).unwrap_or(0);
    if matches!(code, -32005 | -32016) {
        return true;
    }
    let msg = err
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_ascii_lowercase();
    [
        "rate",
        "too many",
        "timeout",
        "temporar",
        "unavailable",
        "busy",
        "capacity",
        "limit",
    ]
    .iter()
    .any(|needle| msg.contains(needle))
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_pacing_builder_sets_spacing_without_network_io() {
        let rpc = RpcClient::new(
            "http://127.0.0.1:1".to_string(),
            1,
            100,
            1000,
            1000,
        )
        .unwrap()
        .with_min_request_spacing_ms(350);
        assert_eq!(rpc.min_request_spacing_ms, 350);
    }

    #[test]
    fn encodes_address() {
        let a = "0x00000000000000000000000000000000000000ab";
        let w = encode_address_word(a).unwrap();
        assert_eq!(w.len(), 64);
        assert!(w.ends_with("00000000000000000000000000000000000000ab"));
    }

    #[test]
    fn decodes_address() {
        let raw = "0x0000000000000000000000001234567890abcdef1234567890abcdef12345678";
        assert_eq!(
            decode_address_word(raw, 0).unwrap(),
            "0x1234567890abcdef1234567890abcdef12345678"
        );
    }

    #[test]
    fn signed_word_encodings_are_sign_extended() {
        assert!(encode_i16_word(-1).chars().all(|c| c == 'f'));
        assert!(encode_i24_word(-1).chars().all(|c| c == 'f'));
        assert!(encode_i16_word(1).ends_with("0001"));
        assert!(encode_i24_word(1).ends_with("000001"));
    }

    #[test]
    fn decodes_tick_lens_tuple_array() {
        fn word_u(v: u128) -> String {
            format!("{v:064x}")
        }
        fn word_i128(v: i128) -> String {
            if v >= 0 {
                format!("{:064x}", v as u128)
            } else {
                let low = v as u128;
                format!("{}{:032x}", "f".repeat(32), low)
            }
        }
        let raw = format!(
            "0x{}{}{}{}{}{}{}{}",
            word_u(32),
            word_u(2),
            encode_i24_word(10),
            word_i128(-5),
            word_u(100),
            encode_i24_word(-20),
            word_i128(7),
            word_u(200),
        );
        let ticks = decode_tick_lens_ticks(&raw).unwrap();
        assert_eq!(ticks, vec![(10, -5, 100), (-20, 7, 200)]);
    }

    #[test]
    fn decodes_signed_int24() {
        let positive = format!("0x{:064x}", 12345u32);
        assert_eq!(decode_i24_word(&positive, 0).unwrap(), 12345);
        let neg_one = format!("0x{:064x}", (1u32 << 24) - 1);
        assert_eq!(decode_i24_word(&neg_one, 0).unwrap(), -1);
    }

    #[test]
    fn multicall3_abi_result_shape_round_trips() {
        let encoded = abi_encode(&[Token::Array(vec![
            Token::Tuple(vec![Token::Bool(true), Token::Bytes(vec![0x12, 0x34])]),
            Token::Tuple(vec![Token::Bool(false), Token::Bytes(Vec::new())]),
        ])]);
        let decoded = abi_decode(
            &[ParamType::Array(Box::new(ParamType::Tuple(vec![
                ParamType::Bool,
                ParamType::Bytes,
            ])))],
            &encoded,
        )
        .unwrap();
        let Token::Array(items) = &decoded[0] else {
            panic!("not array")
        };
        assert_eq!(items.len(), 2);
        let Token::Tuple(first) = &items[0] else {
            panic!("not tuple")
        };
        assert_eq!(first[0], Token::Bool(true));
        assert_eq!(first[1], Token::Bytes(vec![0x12, 0x34]));
    }

    #[test]
    fn curve_stableswap_selectors_match_canonical_abi() {
        let get_dy = ethabi::short_signature(
            "get_dy",
            &[
                ParamType::Int(128),
                ParamType::Int(128),
                ParamType::Uint(256),
            ],
        );
        let get_dy_underlying = ethabi::short_signature(
            "get_dy_underlying",
            &[
                ParamType::Int(128),
                ParamType::Int(128),
                ParamType::Uint(256),
            ],
        );
        assert_eq!(encode_hex_bytes(&get_dy), "5e0d443f");
        assert_eq!(encode_hex_bytes(&get_dy_underlying), "07211ef7");
    }

    #[test]
    fn decodes_balancer_signed_deltas() {
        assert_eq!(
            decode_signed_u256_magnitude(&Token::Int(U256::from(123u64))).unwrap(),
            (false, 123)
        );
        assert_eq!(
            decode_signed_u256_magnitude(&Token::Int(!U256::zero())).unwrap(),
            (true, 1)
        );
    }

    #[test]
    fn parses_multicall_address() {
        let address = parse_h160("0xcA11bde05977b3631167028862bE2a173976CA11").unwrap();
        assert_eq!(address.as_bytes().len(), 20);
    }
}
