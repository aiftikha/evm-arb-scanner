use std::{collections::{HashMap, HashSet}, fs, path::Path};

use anyhow::{anyhow, bail, Result};
use futures::{stream, StreamExt};
use serde::{Deserialize, Serialize};
use tiny_keccak::{Hasher, Keccak};
use tracing::{info, warn};

use crate::{
    rpc::{RpcClient, RpcLog},
    types::{DexConfig, DexKind, PoolDefinition, TokenConfig},
};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UniverseExpansionConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Historical factory-event window ending at the current head. Zero means genesis.
    #[serde(default = "default_lookback_blocks")]
    pub lookback_blocks: u64,
    /// Initial eth_getLogs chunk size. Failed ranges are bisected down to min_log_chunk_blocks.
    #[serde(default = "default_log_chunk_blocks")]
    pub log_chunk_blocks: u64,
    #[serde(default = "default_min_log_chunk_blocks")]
    pub min_log_chunk_blocks: u64,
    /// Breadth-first token-neighborhood expansion rounds from the explicitly configured seed tokens.
    #[serde(default = "default_frontier_rounds")]
    pub frontier_rounds: usize,
    /// Hard cap on auto-enrolled ERC-20s per chain. Seed tokens do not count against this cap.
    #[serde(default = "default_max_auto_tokens")]
    pub max_auto_tokens: usize,
    /// Hard cap on event-derived token pairs retained for cross-venue probing.
    #[serde(default = "default_max_event_pairs")]
    pub max_event_pairs: usize,
    /// Defensive cap on decoded creation logs per factory before neighborhood filtering.
    #[serde(default = "default_max_logs_per_factory")]
    pub max_logs_per_factory: usize,
    /// Case-insensitive symbol prefixes rejected from the auto-discovered intermediate-token set.
    /// Seed/configured tokens are never changed by this policy.
    #[serde(default)]
    pub auto_token_symbol_deny_prefixes: Vec<String>,
}

fn default_lookback_blocks() -> u64 { 0 }
fn default_log_chunk_blocks() -> u64 { 250_000 }
fn default_min_log_chunk_blocks() -> u64 { 2_000 }
fn default_frontier_rounds() -> usize { 2 }
fn default_max_auto_tokens() -> usize { 40 }
fn default_max_event_pairs() -> usize { 320 }
fn default_max_logs_per_factory() -> usize { 50_000 }

impl Default for UniverseExpansionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            lookback_blocks: default_lookback_blocks(),
            log_chunk_blocks: default_log_chunk_blocks(),
            min_log_chunk_blocks: default_min_log_chunk_blocks(),
            frontier_rounds: default_frontier_rounds(),
            max_auto_tokens: default_max_auto_tokens(),
            max_event_pairs: default_max_event_pairs(),
            max_logs_per_factory: default_max_logs_per_factory(),
            auto_token_symbol_deny_prefixes: Vec::new(),
        }
    }
}

impl UniverseExpansionConfig {
    pub fn validate(&self) -> Result<()> {
        if !self.enabled { return Ok(()); }
        if self.log_chunk_blocks == 0 { bail!("universe_expansion.log_chunk_blocks must be > 0"); }
        if self.min_log_chunk_blocks == 0 || self.min_log_chunk_blocks > self.log_chunk_blocks {
            bail!("universe_expansion requires 0 < min_log_chunk_blocks <= log_chunk_blocks");
        }
        if self.frontier_rounds == 0 || self.frontier_rounds > 4 {
            bail!("universe_expansion.frontier_rounds must be 1..=4");
        }
        if self.max_auto_tokens == 0 || self.max_auto_tokens > 256 {
            bail!("universe_expansion.max_auto_tokens must be 1..=256");
        }
        if self.max_event_pairs == 0 || self.max_event_pairs > 5_000 {
            bail!("universe_expansion.max_event_pairs must be 1..=5000");
        }
        if self.max_logs_per_factory == 0 {
            bail!("universe_expansion.max_logs_per_factory must be > 0");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UniverseExpansionStats {
    pub standard_factories_scanned: usize,
    pub factory_scan_failures: usize,
    pub creation_logs_decoded: usize,
    pub enumerable_pools_discovered: usize,
    pub auto_tokens_added: usize,
    pub event_pairs_retained: usize,
    pub event_pools_retained: usize,
    pub metadata_failures: usize,
    pub metadata_retried_from_checkpoint: usize,
    pub metadata_pending: usize,
    pub policy_filtered_tokens: usize,
}

#[derive(Debug, Clone)]
pub struct ExpandedUniverse {
    pub tokens: Vec<TokenConfig>,
    pub event_pools: Vec<PoolDefinition>,
    /// Canonical lowercase token-address pairs used for cross-venue factory probing.
    pub pair_keys: Vec<(String, String)>,
    pub stats: UniverseExpansionStats,
}

#[derive(Debug, Clone)]
struct RawPoolEvent {
    dex: DexConfig,
    pool_address: String,
    token0: String,
    token1: String,
    fee_tier: Option<u32>,
    tick_spacing: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PendingMetadataEntry {
    dex_name: String,
    pool_address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PendingMetadataFile {
    version: u32,
    chain: String,
    entries: Vec<PendingMetadataEntry>,
}

pub async fn expand_from_factory_events(
    chain_name: &str,
    seed_tokens: &[TokenConfig],
    dexes: &[DexConfig],
    rpc: &RpcClient,
    cfg: &UniverseExpansionConfig,
    discovery_concurrency: usize,
    multicall3: &str,
    multicall_max_calls: usize,
    pending_metadata_path: Option<&str>,
) -> Result<ExpandedUniverse> {
    if !cfg.enabled {
        return Ok(ExpandedUniverse {
            tokens: seed_tokens.to_vec(),
            event_pools: Vec::new(),
            pair_keys: seed_pair_keys(seed_tokens),
            stats: UniverseExpansionStats::default(),
        });
    }

    cfg.validate()?;
    let head = rpc.block_number().await?;
    let from_block = if cfg.lookback_blocks == 0 {
        0
    } else {
        head.saturating_sub(cfg.lookback_blocks)
    };

    let mut raw_events = Vec::new();
    let mut stats = UniverseExpansionStats::default();
    let mut unresolved_metadata = Vec::<PendingMetadataEntry>::new();

    // Retry pool metadata that a previous cold-cache discovery could not hydrate.
    // This checkpoint contains addresses only; no quote/state data is trusted from it.
    if let Some(path) = pending_metadata_path {
        let pending = load_pending_metadata(path, chain_name)?;
        stats.metadata_retried_from_checkpoint = pending.len();
        let mut by_dex = HashMap::<String, Vec<String>>::new();
        for entry in pending {
            by_dex.entry(entry.dex_name.to_ascii_lowercase()).or_default().push(entry.pool_address);
        }
        for (dex_name, pools) in by_dex {
            let Some(dex) = dexes.iter().find(|d| d.name.eq_ignore_ascii_case(&dex_name)) else { continue; };
            match hydrate_pool_addresses(rpc, dex, &pools, multicall3, head, multicall_max_calls).await {
                Ok((events, unresolved)) => {
                    raw_events.extend(events);
                    unresolved_metadata.extend(unresolved.into_iter().map(|pool_address| PendingMetadataEntry {
                        dex_name: dex.name.clone(), pool_address,
                    }));
                }
                Err(err) => {
                    warn!(chain = %chain_name, dex = %dex.name, error = %err,
                        "V3.4.2 pending metadata retry batch failed; keeping addresses for the next run");
                    unresolved_metadata.extend(pools.into_iter().map(|pool_address| PendingMetadataEntry {
                        dex_name: dex.name.clone(), pool_address,
                    }));
                }
            }
        }
    }

    // Free/public RPC plans can impose tiny eth_getLogs ranges. Before touching logs,
    // enumerate any factory that exposes an on-chain pool array. This is both cheaper
    // and more complete for V2, Solidly classic, and Slipstream factories, and the
    // resulting token neighborhood is later probed across every configured venue.
    let mut v2_enumerated = HashSet::<String>::new();
    let mut enumerated_pool_count = 0usize;
    for dex in dexes {
        let enumerable = match dex.kind {
            DexKind::V2 if !dex.syncswap_classic => true,
            DexKind::Solidly | DexKind::Slipstream => true,
            _ => false,
        };
        if !enumerable { continue; }
        match enumerate_factory_pools(
            rpc,
            dex,
            cfg.max_logs_per_factory,
            multicall3,
            head,
            multicall_max_calls,
        ).await {
            Ok((events, unresolved)) => {
                let count = events.len();
                enumerated_pool_count += count;
                stats.enumerable_pools_discovered += count;
                if dex.kind == DexKind::V2 {
                    v2_enumerated.insert(dex.name.to_ascii_lowercase());
                }
                raw_events.extend(events);
                unresolved_metadata.extend(unresolved.into_iter().map(|pool_address| PendingMetadataEntry {
                    dex_name: dex.name.clone(), pool_address,
                }));
                info!(chain = %chain_name, dex = %dex.name, factory = %dex.factory, pools = count,
                    "V3.4.2 batched enumerable factory pool scan complete");
            }
            Err(err) => {
                warn!(chain = %chain_name, dex = %dex.name, factory = %dex.factory, error = %err,
                    "V3.4.2 enumerable factory scan unavailable; continuing with other discovery sources");
            }
        }
    }

    for dex in dexes {
        let event_kind = match dex.kind {
            DexKind::V2 if !dex.syncswap_classic && !v2_enumerated.contains(&dex.name.to_ascii_lowercase()) => Some(DexKind::V2),
            DexKind::V3 => Some(DexKind::V3),
            _ => None,
        };
        let Some(kind) = event_kind else { continue; };
        stats.standard_factories_scanned += 1;
        let topic0 = match kind {
            DexKind::V2 => event_topic("PairCreated(address,address,address,uint256)"),
            DexKind::V3 => event_topic("PoolCreated(address,address,uint24,int24,address)"),
            _ => unreachable!(),
        };
        let logs = match fetch_factory_logs_adaptive(
            rpc,
            &dex.factory,
            &topic0,
            from_block,
            head,
            cfg.log_chunk_blocks,
            cfg.min_log_chunk_blocks,
            cfg.max_logs_per_factory,
        ).await {
            Ok(logs) => logs,
            Err(err) => {
                stats.factory_scan_failures += 1;
                warn!(chain = %chain_name, dex = %dex.name, factory = %dex.factory, error = %err,
                    "V3.4 factory creation-log scan failed; continuing with remaining factories and configured seed pairs");
                continue;
            }
        };
        let before = raw_events.len();
        for log in logs {
            match decode_creation_log(dex, kind, &log) {
                Ok(event) => raw_events.push(event),
                Err(err) => warn!(chain = %chain_name, dex = %dex.name, error = %err, "V3.4 skipped malformed factory creation log"),
            }
        }
        let added = raw_events.len() - before;
        stats.creation_logs_decoded += added;
        info!(chain = %chain_name, dex = %dex.name, from_block, head, decoded = added, "V3.4 standard factory creation-log scan complete");
    }

    let mut dedup = HashSet::new();
    raw_events.retain(|event| {
        dedup.insert(format!("{}:{}", event.dex.name.to_ascii_lowercase(), event.pool_address.to_ascii_lowercase()))
    });

    let seed_addresses = seed_tokens.iter()
        .map(|t| t.address.to_ascii_lowercase())
        .collect::<HashSet<_>>();
    let mut selected = seed_addresses.clone();
    let mut auto_order = Vec::<String>::new();

    for _ in 0..cfg.frontier_rounds {
        if auto_order.len() >= cfg.max_auto_tokens { break; }
        let mut scores = HashMap::<String, usize>::new();
        for event in &raw_events {
            let a = event.token0.to_ascii_lowercase();
            let b = event.token1.to_ascii_lowercase();
            let a_selected = selected.contains(&a);
            let b_selected = selected.contains(&b);
            if a_selected && !b_selected {
                *scores.entry(b).or_insert(0) += 1;
            } else if b_selected && !a_selected {
                *scores.entry(a).or_insert(0) += 1;
            }
        }
        if scores.is_empty() { break; }
        let mut ranked = scores.into_iter().collect::<Vec<_>>();
        ranked.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        let remaining = cfg.max_auto_tokens.saturating_sub(auto_order.len());
        let per_round_cap = (cfg.max_auto_tokens + cfg.frontier_rounds - 1) / cfg.frontier_rounds;
        let round_cap = remaining.min(per_round_cap.max(1));
        let mut added_this_round = 0usize;
        for (address, _) in ranked.into_iter().take(round_cap) {
            if selected.insert(address.clone()) {
                auto_order.push(address);
                added_this_round += 1;
            }
        }
        if added_this_round == 0 { break; }
    }

    let rpc_clone = rpc.clone();
    let metadata = stream::iter(auto_order.into_iter()).map(|address| {
        let rpc = rpc_clone.clone();
        async move {
            let decimals = rpc.erc20_decimals(&address).await?;
            let symbol = rpc.erc20_symbol(&address).await.unwrap_or_else(|_| {
                format!("AUTO_{}", address.trim_start_matches("0x").chars().take(6).collect::<String>())
            });
            Ok::<TokenConfig, anyhow::Error>(TokenConfig {
                symbol,
                address,
                decimals,
                usd_price: None,
                wrapped_native: false,
            })
        }
    }).buffer_unordered(discovery_concurrency.max(1)).collect::<Vec<_>>().await;

    let mut tokens = seed_tokens.to_vec();
    let mut valid_auto = HashSet::new();
    for result in metadata {
        match result {
            Ok(token) => {
                let symbol_lower = token.symbol.to_ascii_lowercase();
                let denied = cfg.auto_token_symbol_deny_prefixes.iter()
                    .any(|prefix| !prefix.trim().is_empty() && symbol_lower.starts_with(&prefix.to_ascii_lowercase()));
                if denied {
                    stats.policy_filtered_tokens += 1;
                    continue;
                }
                valid_auto.insert(token.address.to_ascii_lowercase());
                tokens.push(token);
            }
            Err(err) => {
                stats.metadata_failures += 1;
                warn!(chain = %chain_name, error = %err, "V3.4 auto-token metadata probe failed; token excluded");
            }
        }
    }
    stats.auto_tokens_added = valid_auto.len();

    let valid_addresses = tokens.iter().map(|t| t.address.to_ascii_lowercase()).collect::<HashSet<_>>();
    let token_by_address = tokens.iter().map(|t| (t.address.to_ascii_lowercase(), t)).collect::<HashMap<_, _>>();

    let mut pair_score = HashMap::<(String, String), usize>::new();
    for event in &raw_events {
        let pair = canonical_pair(&event.token0, &event.token1);
        if valid_addresses.contains(&pair.0) && valid_addresses.contains(&pair.1) {
            *pair_score.entry(pair).or_insert(0) += 1;
        }
    }
    for pair in seed_pair_keys(seed_tokens) {
        *pair_score.entry(pair).or_insert(0) += usize::MAX / 4;
    }
    let mut ranked_pairs = pair_score.into_iter().collect::<Vec<_>>();
    ranked_pairs.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    let pair_keys = ranked_pairs.into_iter().take(cfg.max_event_pairs).map(|v| v.0).collect::<Vec<_>>();
    let pair_set = pair_keys.iter().cloned().collect::<HashSet<_>>();
    stats.event_pairs_retained = pair_keys.len();

    let resolved_metadata_keys = raw_events.iter()
        .map(|event| format!("{}:{}", event.dex.name.to_ascii_lowercase(), event.pool_address.to_ascii_lowercase()))
        .collect::<HashSet<_>>();
    let mut event_pools = Vec::new();
    for event in raw_events {
        let pair = canonical_pair(&event.token0, &event.token1);
        if !pair_set.contains(&pair) { continue; }
        let Some(t0) = token_by_address.get(&event.token0.to_ascii_lowercase()) else { continue; };
        let Some(t1) = token_by_address.get(&event.token1.to_ascii_lowercase()) else { continue; };
        event_pools.push(PoolDefinition {
            dex: event.dex,
            pool_address: event.pool_address,
            token0: event.token0,
            token1: event.token1,
            token0_decimals: t0.decimals,
            token1_decimals: t1.decimals,
            fee_tier: event.fee_tier,
            tick_spacing: event.tick_spacing,
        });
    }
    stats.event_pools_retained = event_pools.len();
    unresolved_metadata.retain(|entry| {
        !resolved_metadata_keys.contains(&format!("{}:{}", entry.dex_name.to_ascii_lowercase(), entry.pool_address.to_ascii_lowercase()))
    });
    unresolved_metadata.sort_by(|a, b| a.dex_name.cmp(&b.dex_name).then_with(|| a.pool_address.cmp(&b.pool_address)));
    unresolved_metadata.dedup_by(|a, b| a.dex_name.eq_ignore_ascii_case(&b.dex_name) && a.pool_address.eq_ignore_ascii_case(&b.pool_address));
    stats.metadata_pending = unresolved_metadata.len();
    if let Some(path) = pending_metadata_path {
        save_pending_metadata(path, chain_name, &unresolved_metadata)?;
    }

    info!(
        chain = %chain_name,
        seed_tokens = seed_tokens.len(),
        auto_tokens = stats.auto_tokens_added,
        pairs = stats.event_pairs_retained,
        event_pools = stats.event_pools_retained,
        enumerable_pools = enumerated_pool_count,
        factory_scan_failures = stats.factory_scan_failures,
        metadata_failures = stats.metadata_failures,
        metadata_retried = stats.metadata_retried_from_checkpoint,
        metadata_pending = stats.metadata_pending,
        policy_filtered_tokens = stats.policy_filtered_tokens,
        "V3.4.2 event-driven token universe expansion complete"
    );

    Ok(ExpandedUniverse { tokens, event_pools, pair_keys, stats })
}

async fn enumerate_factory_pools(
    rpc: &RpcClient,
    dex: &DexConfig,
    max_pools: usize,
    multicall3: &str,
    block: u64,
    max_calls_per_batch: usize,
) -> Result<(Vec<RawPoolEvent>, Vec<String>)> {
    let (length_selector, item_selector) = match dex.kind {
        DexKind::V2 => ("0x574f2ba3", "0x1e3dd18b"), // allPairsLength(), allPairs(uint256)
        DexKind::Solidly | DexKind::Slipstream => ("0xefde4e64", "0x41d1de97"), // allPoolsLength(), allPools(uint256)
        _ => bail!("factory kind is not enumerable"),
    };

    let raw_len = rpc.eth_call_at(&dex.factory, length_selector, block).await?;
    let total = usize_from_word(&raw_len, 0)?;
    if total == 0 { return Ok((Vec::new(), Vec::new())); }
    if total > max_pools {
        bail!("factory {} exposes {} pools, above max_logs_per_factory={}; raise the cap deliberately before enumerating", dex.factory, total, max_pools);
    }

    // Enumerate addresses through Multicall3 rather than one JSON-RPC request per index.
    let address_calls = (0..total)
        .map(|index| (dex.factory.clone(), format!("{}{:064x}", item_selector, index)))
        .collect::<Vec<_>>();
    let address_rows = rpc
        .multicall_read_many_at(multicall3, &address_calls, block, max_calls_per_batch)
        .await?;
    let mut pools = Vec::with_capacity(total);
    for raw in address_rows.into_iter().flatten() {
        if let Ok(pool) = address_from_word(&raw, 0) {
            if !is_zero_address(&pool) { pools.push(pool); }
        }
    }
    pools.sort();
    pools.dedup();
    match hydrate_pool_addresses(rpc, dex, &pools, multicall3, block, max_calls_per_batch).await {
        Ok(result) => Ok(result),
        Err(err) => {
            warn!(dex = %dex.name, factory = %dex.factory, error = %err, pending = pools.len(),
                "V3.4.2 enumerable metadata batch failed after retries; retaining pool addresses in pending checkpoint");
            Ok((Vec::new(), pools))
        }
    }
}

async fn hydrate_pool_addresses(
    rpc: &RpcClient,
    dex: &DexConfig,
    pools: &[String],
    multicall3: &str,
    block: u64,
    max_calls_per_batch: usize,
) -> Result<(Vec<RawPoolEvent>, Vec<String>)> {
    if pools.is_empty() { return Ok((Vec::new(), Vec::new())); }
    let token0_calls = pools.iter().map(|pool| (pool.clone(), "0x0dfe1681".to_string())).collect::<Vec<_>>();
    let token1_calls = pools.iter().map(|pool| (pool.clone(), "0xd21220a7".to_string())).collect::<Vec<_>>();
    let token0_rows = rpc.multicall_read_many_at(multicall3, &token0_calls, block, max_calls_per_batch).await?;
    let token1_rows = rpc.multicall_read_many_at(multicall3, &token1_calls, block, max_calls_per_batch).await?;
    let aux_rows = match dex.kind {
        DexKind::Solidly => {
            let calls = pools.iter().map(|pool| (pool.clone(), "0x22be3de1".to_string())).collect::<Vec<_>>();
            Some(rpc.multicall_read_many_at(multicall3, &calls, block, max_calls_per_batch).await?)
        }
        DexKind::Slipstream => {
            let calls = pools.iter().map(|pool| (pool.clone(), "0xd0c93a7c".to_string())).collect::<Vec<_>>();
            Some(rpc.multicall_read_many_at(multicall3, &calls, block, max_calls_per_batch).await?)
        }
        DexKind::V2 => None,
        _ => unreachable!(),
    };

    let mut out = Vec::new();
    let mut unresolved = Vec::new();
    for idx in 0..pools.len() {
        let Some(raw0) = token0_rows.get(idx).and_then(|v| v.as_ref()) else {
            unresolved.push(pools[idx].clone());
            continue;
        };
        let Some(raw1) = token1_rows.get(idx).and_then(|v| v.as_ref()) else {
            unresolved.push(pools[idx].clone());
            continue;
        };
        let (Ok(token0), Ok(token1)) = (address_from_word(raw0, 0), address_from_word(raw1, 0)) else {
            unresolved.push(pools[idx].clone());
            continue;
        };
        if is_zero_address(&token0) || is_zero_address(&token1) || token0.eq_ignore_ascii_case(&token1) {
            continue;
        }
        let (fee_tier, tick_spacing) = match dex.kind {
            DexKind::Solidly => {
                let Some(raw) = aux_rows.as_ref().and_then(|rows| rows.get(idx)).and_then(|v| v.as_ref()) else {
                    unresolved.push(pools[idx].clone());
                    continue;
                };
                let Ok(stable) = bool_from_word(raw, 0) else {
                    unresolved.push(pools[idx].clone());
                    continue;
                };
                if stable != dex.solidly_stable { continue; }
                (None, None)
            }
            DexKind::Slipstream => {
                let Some(raw) = aux_rows.as_ref().and_then(|rows| rows.get(idx)).and_then(|v| v.as_ref()) else {
                    unresolved.push(pools[idx].clone());
                    continue;
                };
                let Ok(spacing) = i24_from_word(raw, 0) else {
                    unresolved.push(pools[idx].clone());
                    continue;
                };
                (None, Some(spacing))
            }
            DexKind::V2 => (None, None),
            _ => unreachable!(),
        };
        out.push(RawPoolEvent {
            dex: dex.clone(),
            pool_address: pools[idx].clone(),
            token0,
            token1,
            fee_tier,
            tick_spacing,
        });
    }
    Ok((out, unresolved))
}

fn load_pending_metadata(path: &str, chain_name: &str) -> Result<Vec<PendingMetadataEntry>> {
    let path = Path::new(path);
    if !path.exists() { return Ok(Vec::new()); }
    let raw = match fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(err) => {
            warn!(path = %path.display(), error = %err, "V3.4.2 pending metadata checkpoint unreadable; ignoring");
            return Ok(Vec::new());
        }
    };
    let file: PendingMetadataFile = match serde_json::from_str(&raw) {
        Ok(file) => file,
        Err(err) => {
            warn!(path = %path.display(), error = %err, "V3.4.2 pending metadata checkpoint incompatible; ignoring");
            return Ok(Vec::new());
        }
    };
    if file.version != 1 || !file.chain.eq_ignore_ascii_case(chain_name) { return Ok(Vec::new()); }
    Ok(file.entries)
}

fn save_pending_metadata(path: &str, chain_name: &str, entries: &[PendingMetadataEntry]) -> Result<()> {
    let path = Path::new(path);
    if entries.is_empty() {
        if path.exists() { let _ = fs::remove_file(path); }
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() { fs::create_dir_all(parent)?; }
    }
    let file = PendingMetadataFile { version: 1, chain: chain_name.to_string(), entries: entries.to_vec() };
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, serde_json::to_string_pretty(&file)?)?;
    if path.exists() { fs::remove_file(path)?; }
    fs::rename(tmp, path)?;
    Ok(())
}

async fn fetch_factory_logs_adaptive(
    rpc: &RpcClient,
    factory: &str,
    topic0: &str,
    from_block: u64,
    to_block: u64,
    preferred_chunk: u64,
    min_chunk: u64,
    max_logs: usize,
) -> Result<Vec<RpcLog>> {
    let mut pending = Vec::<(u64, u64)>::new();
    let mut start = from_block;
    while start <= to_block {
        let end = start.saturating_add(preferred_chunk.saturating_sub(1)).min(to_block);
        pending.push((start, end));
        if end == u64::MAX || end >= to_block { break; }
        start = end + 1;
    }

    let mut out = Vec::new();
    while let Some((lo, hi)) = pending.pop() {
        match rpc.get_logs(factory, topic0, lo, hi).await {
            Ok(mut logs) => {
                out.append(&mut logs);
                if out.len() > max_logs {
                    bail!("factory {factory} exceeded max_logs_per_factory={max_logs}; narrow lookback or raise cap");
                }
            }
            Err(err) => {
                let width = hi.saturating_sub(lo).saturating_add(1);
                let err_text = err.to_string();
                if let Some(provider_cap) = provider_log_range_cap(&err_text) {
                    // A cap below min_chunk makes a genesis/large-lookback crawl operationally
                    // unreasonable (Base free-tier can be 10 blocks). Fail this log source
                    // immediately; enumerable factories already seed the cross-venue graph.
                    if provider_cap < min_chunk {
                        return Err(anyhow!(
                            "provider eth_getLogs cap={provider_cap} blocks is below practical min_log_chunk_blocks={min_chunk}; historical event crawl skipped for factory {factory}"
                        ));
                    }
                    if provider_cap < width {
                        let mut chunk_start = lo;
                        let mut chunks = Vec::new();
                        while chunk_start <= hi {
                            let chunk_end = chunk_start
                                .saturating_add(provider_cap.saturating_sub(1))
                                .min(hi);
                            chunks.push((chunk_start, chunk_end));
                            if chunk_end >= hi { break; }
                            chunk_start = chunk_end + 1;
                        }
                        for range in chunks.into_iter().rev() {
                            pending.push(range);
                        }
                        continue;
                    }
                }
                if width <= min_chunk || lo >= hi {
                    return Err(anyhow!("eth_getLogs failed for factory {factory} range {lo}..={hi}: {err}"));
                }
                let mid = lo + (hi - lo) / 2;
                pending.push((mid + 1, hi));
                pending.push((lo, mid));
            }
        }
    }
    out.sort_by_key(|log| log.block_number);
    Ok(out)
}

fn provider_log_range_cap(message: &str) -> Option<u64> {
    let lower = message.to_ascii_lowercase();
    let marker = "up to a ";
    let start = lower.find(marker)? + marker.len();
    let tail = &lower[start..];
    let digits = tail.chars().take_while(|c| c.is_ascii_digit()).collect::<String>();
    if digits.is_empty() { return None; }
    let rest = &tail[digits.len()..];
    if !rest.starts_with(" block range") { return None; }
    digits.parse().ok()
}

fn usize_from_word(raw: &str, index: usize) -> Result<usize> {
    let word = word_at(raw, index)?;
    let value = u128::from_str_radix(word, 16)?;
    usize::try_from(value).map_err(|_| anyhow!("ABI uint256 does not fit usize"))
}

fn bool_from_word(raw: &str, index: usize) -> Result<bool> {
    let word = word_at(raw, index)?;
    match u8::from_str_radix(&word[62..64], 16)? {
        0 => Ok(false),
        1 => Ok(true),
        other => bail!("invalid ABI bool value {other}"),
    }
}

fn is_zero_address(address: &str) -> bool {
    address.trim_start_matches("0x").chars().all(|c| c == '0')
}

fn decode_creation_log(dex: &DexConfig, kind: DexKind, log: &RpcLog) -> Result<RawPoolEvent> {
    match kind {
        DexKind::V2 => {
            if log.topics.len() < 3 { bail!("PairCreated log has fewer than 3 topics"); }
            Ok(RawPoolEvent {
                dex: dex.clone(),
                pool_address: address_from_word(&log.data, 0)?,
                token0: address_from_topic(&log.topics[1])?,
                token1: address_from_topic(&log.topics[2])?,
                fee_tier: None,
                tick_spacing: None,
            })
        }
        DexKind::V3 => {
            if log.topics.len() < 4 { bail!("PoolCreated log has fewer than 4 topics"); }
            Ok(RawPoolEvent {
                dex: dex.clone(),
                pool_address: address_from_word(&log.data, 1)?,
                token0: address_from_topic(&log.topics[1])?,
                token1: address_from_topic(&log.topics[2])?,
                fee_tier: Some(u24_from_word(&log.topics[3])?),
                tick_spacing: Some(i24_from_word(&log.data, 0)?),
            })
        }
        _ => bail!("unsupported creation-log kind"),
    }
}

fn seed_pair_keys(tokens: &[TokenConfig]) -> Vec<(String, String)> {
    let mut pairs = Vec::new();
    for i in 0..tokens.len() {
        for j in (i + 1)..tokens.len() {
            pairs.push(canonical_pair(&tokens[i].address, &tokens[j].address));
        }
    }
    pairs
}

fn canonical_pair(a: &str, b: &str) -> (String, String) {
    let a = a.to_ascii_lowercase();
    let b = b.to_ascii_lowercase();
    if a < b { (a, b) } else { (b, a) }
}

fn event_topic(signature: &str) -> String {
    let mut hasher = Keccak::v256();
    hasher.update(signature.as_bytes());
    let mut out = [0u8; 32];
    hasher.finalize(&mut out);
    format!("0x{}", hex(&out))
}

fn address_from_topic(topic: &str) -> Result<String> {
    let raw = topic.strip_prefix("0x").unwrap_or(topic);
    if raw.len() != 64 || !raw.chars().all(|c| c.is_ascii_hexdigit()) {
        bail!("bad address topic");
    }
    Ok(format!("0x{}", &raw[24..]))
}

fn word_at(raw: &str, index: usize) -> Result<&str> {
    let raw = raw.strip_prefix("0x").unwrap_or(raw);
    let start = index.saturating_mul(64);
    let end = start.saturating_add(64);
    if raw.len() < end { bail!("event data too short for word {index}"); }
    Ok(&raw[start..end])
}

fn address_from_word(raw: &str, index: usize) -> Result<String> {
    let word = word_at(raw, index)?;
    Ok(format!("0x{}", &word[24..]))
}

fn u24_from_word(raw: &str) -> Result<u32> {
    let raw = raw.strip_prefix("0x").unwrap_or(raw);
    if raw.len() < 6 { bail!("uint24 word too short"); }
    u32::from_str_radix(&raw[raw.len() - 6..], 16).map_err(Into::into)
}

fn i24_from_word(raw: &str, index: usize) -> Result<i32> {
    let word = word_at(raw, index)?;
    let low = u32::from_str_radix(&word[58..64], 16)?;
    Ok(if low & 0x0080_0000 != 0 {
        low as i32 - (1 << 24)
    } else {
        low as i32
    })
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes { let _ = write!(&mut out, "{b:02x}"); }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_factory_topics_match_known_uniswap_signatures() {
        assert_eq!(
            event_topic("PairCreated(address,address,address,uint256)"),
            "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"
        );
        assert_eq!(
            event_topic("PoolCreated(address,address,uint24,int24,address)"),
            "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"
        );
    }

    #[test]
    fn provider_free_tier_log_cap_is_detected() {
        let msg = "Under the Free tier plan, you can make eth_getLogs requests with up to a 10 block range.";
        assert_eq!(provider_log_range_cap(msg), Some(10));
        assert_eq!(provider_log_range_cap("ordinary RPC error"), None);
    }

    #[test]
    fn signed_int24_event_word_decodes() {
        let neg_one = format!("0x{}ffffff", "f".repeat(58));
        assert_eq!(i24_from_word(&neg_one, 0).unwrap(), -1);
        let positive = format!("0x{:064x}", 60u32);
        assert_eq!(i24_from_word(&positive, 0).unwrap(), 60);
    }
}
