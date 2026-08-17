use std::{
    collections::{BTreeMap, HashMap, HashSet},
    env, fs,
    path::Path,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, bail, Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use rusqlite::{params, Connection};
use tiny_keccak::{Hasher, Keccak};
use tokio::sync::{watch, Mutex};
use tracing::{info, warn};

use crate::{
    amm::{geometric_grid, golden_section_max, v2_leg_output},
    rpc::RpcClient,
    types::{eq_addr, DexConfig, DexKind, PoolDefinition, PoolState, TokenConfig},
    universe::{expand_from_factory_events, UniverseExpansionConfig, UniverseExpansionStats},
    v3_math::concentrated_leg_output_current_interval,
};

const DEFAULT_CONFIG_PATH: &str = "config.v3_4.toml";

#[derive(Debug, Clone, Deserialize)]
pub struct BreadthConfig {
    #[serde(default = "default_summary_interval_secs")]
    pub summary_interval_secs: u64,
    #[serde(default = "default_top_candidates")]
    pub top_candidates: usize,
    #[serde(default = "default_min_trade_usd")]
    pub min_trade_usd: f64,
    #[serde(default = "default_max_trade_usd")]
    pub max_trade_usd: f64,
    #[serde(default = "default_min_pool_anchor_liquidity_usd")]
    pub min_pool_anchor_liquidity_usd: f64,
    #[serde(default = "default_exact_quote_budget")]
    pub exact_quote_budget_per_scan: usize,
    #[serde(default = "default_max_cycles_per_depth")]
    pub max_cycles_per_depth: usize,
    #[serde(default = "default_sizing_grid_points")]
    pub sizing_grid_points: usize,
    #[serde(default = "default_sizing_refine_iterations")]
    pub sizing_refine_iterations: usize,
    #[serde(default = "default_multicall3")]
    pub multicall3_address: String,
    #[serde(default = "default_multicall_max_calls")]
    pub multicall_max_calls: usize,
    #[serde(default = "default_output_dir")]
    pub output_dir: String,
    /// Prefix for summary/top/health artifacts. V3.3 configs default to v3_3; V3.4 uses v3_4.
    #[serde(default = "default_artifact_prefix")]
    pub artifact_prefix: String,
    /// Prefer local AMM-state sizing before exact RPC validation. Unsupported route shapes fall back safely.
    #[serde(default)]
    pub analytical_sizing_enabled: bool,
    #[serde(default = "default_analytical_sizing_iterations")]
    pub analytical_sizing_iterations: usize,
    #[serde(default = "default_analytical_validation_band_pct")]
    pub analytical_validation_band_pct: f64,
    pub chains: Vec<BreadthChainConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BreadthChainConfig {
    pub name: String,
    pub chain_id: u64,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_tier")]
    pub tier: u8,
    pub rpc_url_env: String,
    /// Optional Aave V3 Pool used only by the V3.3.4 historical economics harness.
    /// The breadth scanner itself never calls flashLoanSimple.
    #[serde(default)]
    pub aave_pool: Option<String>,
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    #[serde(default = "default_rpc_attempts")]
    pub rpc_max_attempts: u32,
    #[serde(default = "default_retry_base_ms")]
    pub rpc_retry_base_ms: u64,
    #[serde(default = "default_retry_max_ms")]
    pub rpc_retry_max_ms: u64,
    #[serde(default = "default_rpc_timeout_ms")]
    pub rpc_request_timeout_ms: u64,
    /// Minimum spacing between HTTP JSON-RPC requests made by this chain worker.
    /// Zero disables pacing. Useful for public endpoints that allow sustained traffic
    /// but reject short eth_call bursts.
    #[serde(default)]
    pub rpc_min_request_spacing_ms: u64,
    #[serde(default = "default_discovery_concurrency")]
    pub discovery_concurrency: usize,
    #[serde(default = "default_discovery_probe_delay_ms")]
    pub discovery_probe_delay_ms: u64,
    /// Discovery-only RPC pacing. This is applied to a clone sharing the same
    /// provider cooldown/request slot as the scan client, so cold-cache hydration
    /// cannot create an independent request burst.
    #[serde(default = "default_discovery_rpc_min_request_spacing_ms")]
    pub discovery_rpc_min_request_spacing_ms: u64,
    #[serde(default = "default_discovery_rpc_max_attempts")]
    pub discovery_rpc_max_attempts: u32,
    #[serde(default = "default_discovery_rpc_retry_max_ms")]
    pub discovery_rpc_retry_max_ms: u64,
    pub pool_cache_path: String,
    /// Per-chain persistent evidence store. Interesting exact-quoted routes are
    /// upserted here throughout the run so shutdown top-N truncation cannot erase them.
    pub opportunity_db_path: String,
    #[serde(default = "default_pool_cache_ttl_hours")]
    pub pool_cache_ttl_hours: u64,
    #[serde(default = "default_liquidity_refresh_blocks")]
    pub liquidity_refresh_blocks: u64,
    /// Optional chain-specific hot-pool floor. Thin Tier-3 ecosystems can use a
    /// lower floor without weakening the Base/Arbitrum filters.
    #[serde(default)]
    pub min_pool_anchor_liquidity_usd: Option<f64>,
    pub flash_loan_premium_bps: f64,
    /// Capital that may be used directly instead of paying a flash-loan premium.
    /// Zero preserves flash-only behavior. This is scanner economics only; breadth
    /// mode never transfers or funds assets.
    #[serde(default)]
    pub self_funded_capital_usd: f64,
    #[serde(default = "default_min_net_profit_usd")]
    pub min_net_profit_usd: f64,
    #[serde(default)]
    pub mev_bid_reserve_pct: f64,
    /// Optional V3.4 cold-start expansion from standard V2/V3 factory creation events.
    #[serde(default)]
    pub universe_expansion: UniverseExpansionConfig,
    pub fee_model: BreadthFeeModel,
    pub tokens: Vec<TokenConfig>,
    pub dexes: Vec<DexConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BreadthFeeModel {
    #[serde(default = "default_fee_quality")]
    pub quality: String,
    pub base_gas_units: u64,
    #[serde(default)]
    pub gas_units_per_extra_hop: u64,
    #[serde(default)]
    pub l1_data_fee_usd_proxy: f64,
    #[serde(default)]
    pub operator_fee_usd_proxy: f64,
}

fn default_true() -> bool { true }
fn default_tier() -> u8 { 3 }
fn default_summary_interval_secs() -> u64 { 60 }
fn default_top_candidates() -> usize { 40 }
fn default_min_trade_usd() -> f64 { 10.0 }
fn default_max_trade_usd() -> f64 { 10_000.0 }
fn default_min_pool_anchor_liquidity_usd() -> f64 { 25_000.0 }
fn default_exact_quote_budget() -> usize { 12 }
fn default_max_cycles_per_depth() -> usize { 50_000 }
fn default_sizing_grid_points() -> usize { 6 }
fn default_sizing_refine_iterations() -> usize { 2 }
fn default_multicall3() -> String { "0xcA11bde05977b3631167028862bE2a173976CA11".to_string() }
fn default_multicall_max_calls() -> usize { 80 }
fn default_output_dir() -> String { ".".to_string() }
fn default_artifact_prefix() -> String { "v3_3".to_string() }
fn default_analytical_sizing_iterations() -> usize { 24 }
fn default_analytical_validation_band_pct() -> f64 { 3.0 }
fn default_poll_interval_ms() -> u64 { 5_000 }
fn default_rpc_attempts() -> u32 { 4 }
fn default_retry_base_ms() -> u64 { 100 }
fn default_retry_max_ms() -> u64 { 1_500 }
fn default_rpc_timeout_ms() -> u64 { 6_000 }
fn default_discovery_concurrency() -> usize { 4 }
fn default_discovery_probe_delay_ms() -> u64 { 25 }
fn default_discovery_rpc_min_request_spacing_ms() -> u64 { 500 }
fn default_discovery_rpc_max_attempts() -> u32 { 8 }
fn default_discovery_rpc_retry_max_ms() -> u64 { 5_000 }
fn default_pool_cache_ttl_hours() -> u64 { 24 }
fn default_liquidity_refresh_blocks() -> u64 { 20 }
fn default_min_net_profit_usd() -> f64 { 1.0 }
fn default_fee_quality() -> String { "conservative_proxy".to_string() }

impl BreadthConfig {
    pub fn load(path: &str) -> Result<Self> {
        let actual = if path.trim().is_empty() { DEFAULT_CONFIG_PATH } else { path };
        let raw = fs::read_to_string(actual)
            .with_context(|| format!("failed to read V3.4 breadth config: {actual}"))?;
        let cfg: Self = toml::from_str(&raw).context("invalid V3.4 breadth TOML")?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<()> {
        if self.chains.is_empty() { bail!("V3.4 requires at least one chain"); }
        if self.summary_interval_secs == 0 { bail!("summary_interval_secs must be > 0"); }
        if self.top_candidates == 0 || self.top_candidates > 200 { bail!("top_candidates must be 1..=200"); }
        if self.min_trade_usd <= 0.0 || self.max_trade_usd <= self.min_trade_usd {
            bail!("require 0 < min_trade_usd < max_trade_usd");
        }
        if self.exact_quote_budget_per_scan == 0 { bail!("exact_quote_budget_per_scan must be > 0"); }
        if self.max_cycles_per_depth == 0 { bail!("max_cycles_per_depth must be > 0"); }
        if self.sizing_grid_points < 2 { bail!("sizing_grid_points must be >= 2"); }
        if self.sizing_refine_iterations > 8 { bail!("sizing_refine_iterations must be <= 8"); }
        if self.artifact_prefix.trim().is_empty() || !self.artifact_prefix.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            bail!("artifact_prefix must contain only letters, digits, or underscore");
        }
        if self.analytical_sizing_iterations < 8 || self.analytical_sizing_iterations > 80 {
            bail!("analytical_sizing_iterations must be 8..=80");
        }
        if !(0.1..=20.0).contains(&self.analytical_validation_band_pct) {
            bail!("analytical_validation_band_pct must be 0.1..=20");
        }
        validate_address(&self.multicall3_address)?;
        for chain in &self.chains {
            if chain.name.trim().is_empty() { bail!("chain name must not be empty"); }
            if chain.chain_id == 0 { bail!("chain {} has invalid chain_id", chain.name); }
            if chain.poll_interval_ms < 250 { bail!("chain {} poll_interval_ms must be >= 250", chain.name); }
            if chain.rpc_min_request_spacing_ms > 10_000 {
                bail!("chain {} rpc_min_request_spacing_ms must be <= 10000", chain.name);
            }
            if chain.discovery_rpc_min_request_spacing_ms > 10_000 {
                bail!("chain {} discovery_rpc_min_request_spacing_ms must be <= 10000", chain.name);
            }
            if chain.discovery_rpc_max_attempts == 0 || chain.discovery_rpc_max_attempts > 20 {
                bail!("chain {} discovery_rpc_max_attempts must be 1..=20", chain.name);
            }
            if chain.discovery_rpc_retry_max_ms < chain.rpc_retry_base_ms || chain.discovery_rpc_retry_max_ms > 60_000 {
                bail!("chain {} discovery_rpc_retry_max_ms must be >= rpc_retry_base_ms and <= 60000", chain.name);
            }
            if chain.tokens.len() < 2 { bail!("chain {} needs at least two tokens", chain.name); }
            if chain.dexes.is_empty() { bail!("chain {} needs at least one DEX", chain.name); }
            if chain.pool_cache_path.trim().is_empty() { bail!("chain {} pool_cache_path must not be empty", chain.name); }
            if chain.opportunity_db_path.trim().is_empty() { bail!("chain {} opportunity_db_path must not be empty", chain.name); }
            if let Some(pool) = chain.aave_pool.as_deref().filter(|v| !v.trim().is_empty()) { validate_address(pool)?; }
            if let Some(floor) = chain.min_pool_anchor_liquidity_usd {
                if !floor.is_finite() || floor < 0.0 {
                    bail!("chain {} min_pool_anchor_liquidity_usd must be finite and >= 0", chain.name);
                }
            }
            if !(0.0..1000.0).contains(&chain.flash_loan_premium_bps) {
                bail!("chain {} flash_loan_premium_bps out of range", chain.name);
            }
            if !chain.self_funded_capital_usd.is_finite() || !(0.0..=1_000_000.0).contains(&chain.self_funded_capital_usd) {
                bail!("chain {} self_funded_capital_usd must be 0..=1000000", chain.name);
            }
            if !(0.0..=100.0).contains(&chain.mev_bid_reserve_pct) {
                bail!("chain {} mev_bid_reserve_pct must be 0..=100", chain.name);
            }
            chain.universe_expansion.validate()
                .with_context(|| format!("invalid universe_expansion for {}", chain.name))?;
            for token in &chain.tokens { validate_address(&token.address)?; }
            for dex in &chain.dexes {
                validate_address(&dex.factory)?;
                match dex.kind {
                    DexKind::V2 => {
                        if dex.fee_bps.is_none() { bail!("V2 DEX {} on {} requires fee_bps", dex.name, chain.name); }
                        if dex.syncswap_classic && (dex.quoter_v2.is_some() || dex.tick_lens.is_some() || !dex.fee_tiers.is_empty()) {
                            bail!("SyncSwap Classic {} on {} uses factory/pool-level methods; do not configure V3 fields", dex.name, chain.name);
                        }
                    }
                    DexKind::V3 => {
                        let q = dex.quoter_v2.as_deref().ok_or_else(|| anyhow!("V3 DEX {} on {} requires quoter_v2", dex.name, chain.name))?;
                        validate_address(q)?;
                        if dex.fee_tiers.is_empty() { bail!("V3 DEX {} on {} requires fee_tiers", dex.name, chain.name); }
                    }
                    DexKind::Algebra => {
                        let q = dex.quoter_v2.as_deref().ok_or_else(|| anyhow!("Algebra DEX {} on {} requires quoter_v2", dex.name, chain.name))?;
                        validate_address(q)?;
                    }
                    DexKind::Solidly => {
                        if !dex.fee_tiers.is_empty() {
                            bail!("Solidly DEX {} on {} must not configure V3 fee_tiers", dex.name, chain.name);
                        }
                        if dex.quoter_v2.is_some() || dex.tick_lens.is_some() || dex.slipstream_factory_mask.is_some() {
                            bail!("Solidly DEX {} on {} uses pool-level exact quotes; do not configure V3/Slipstream quoter fields", dex.name, chain.name);
                        }
                        if let Some(fee) = dex.fee_bps {
                            if !(0.0..1000.0).contains(&fee) {
                                bail!("Solidly DEX {} on {} has invalid fallback fee_bps", dex.name, chain.name);
                            }
                        }
                    }
                    DexKind::Slipstream => {
                        let q = dex.quoter_v2.as_deref().ok_or_else(|| anyhow!("Slipstream DEX {} on {} requires quoter_v2", dex.name, chain.name))?;
                        validate_address(q)?;
                        if !dex.fee_tiers.is_empty() || dex.tick_lens.is_some() || dex.fee_bps.is_some() {
                            bail!("Slipstream DEX {} on {} discovers tick spacings/live fees dynamically; do not configure fee_tiers/tick_lens/fee_bps", dex.name, chain.name);
                        }
                        if let Some(mask) = dex.slipstream_factory_mask {
                            if !matches!(mask, 0 | 524288 | 1048576) {
                                bail!("Slipstream DEX {} on {} has invalid MixedRouteQuoterV3 factory mask {}", dex.name, chain.name, mask);
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct DepthMetrics {
    pub cycles_generated: u64,
    pub cycles_liquidity_eligible: u64,
    pub marginal_positive: u64,
    pub marginal_above_flash: u64,
    pub finite_gross_positive: u64,
    pub finite_after_flash_positive: u64,
    pub finite_after_financing_positive: u64,
    pub exact_quote_attempts: u64,
    pub exact_quote_successes: u64,
    pub exact_quote_failures: u64,
    pub sizing_routes: u64,
    pub sizing_evals: u64,
    pub analytical_sizing_routes: u64,
    pub analytical_sizing_fallbacks: u64,
    pub analytical_exact_validations: u64,
    pub sizing_improved: u64,
    pub sizing_net_positive: u64,
    pub unique_after_flash_positive: u64,
    pub unique_after_financing_positive: u64,
    pub unique_net_positive: u64,
    pub best_input_usd: Option<f64>,
    pub best_edge_bps: Option<f64>,
    pub best_gross_usd: Option<f64>,
    pub best_after_flash_usd: Option<f64>,
    pub best_after_financing_usd: Option<f64>,
    pub best_estimated_net_usd: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RpcHealth {
    pub requests: u64,
    pub retries: u64,
    pub failures: u64,
    pub rate_limits: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct CandidateRecord {
    pub opportunity_id: String,
    pub chain: String,
    pub block_number: u64,
    pub first_seen: String,
    pub last_seen: String,
    pub lifetime_ms: u64,
    pub observations: u64,
    pub route_depth: usize,
    pub start_token: String,
    pub token_path: Vec<String>,
    pub venues: Vec<String>,
    pub pools: Vec<String>,
    pub spot_edge_bps: f64,
    pub input_usd: f64,
    pub input_amount: f64,
    pub gross_profit_usd: f64,
    pub flash_fee_usd: f64,
    pub after_flash_usd: f64,
    pub funding_mode: String,
    pub financing_fee_usd: f64,
    pub profit_after_financing_usd: f64,
    pub mev_bid_reserve_usd: f64,
    pub estimated_execution_fee_usd: Option<f64>,
    pub estimated_net_usd: Option<f64>,
    pub fee_model_quality: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChainReport {
    pub chain: String,
    pub chain_id: u64,
    pub tier: u8,
    pub status: String,
    pub last_error: Option<String>,
    pub started_at: String,
    pub ended_at: Option<String>,
    pub scans: u64,
    pub last_scan_ms: u64,
    pub max_scan_ms: u64,
    pub last_block: Option<u64>,
    pub discovered_pools: usize,
    pub hot_pools: usize,
    pub configured_tokens: usize,
    pub auto_tokens: usize,
    pub event_pairs: usize,
    pub event_pools: usize,
    pub enumerable_pools: usize,
    pub factory_scan_failures: usize,
    pub metadata_failures: usize,
    pub metadata_retried_from_checkpoint: usize,
    pub metadata_pending: usize,
    pub policy_filtered_tokens: usize,
    pub self_funded_capital_usd: f64,
    pub adapters: Vec<String>,
    pub opportunity_db_path: String,
    pub native_usd: Option<f64>,
    pub cycle_budget_exhausted: bool,
    pub upper_bound_pruned_branches: u64,
    pub partial_snapshots: u64,
    pub empty_or_uninitialized_pools: u64,
    pub by_depth: BTreeMap<String, DepthMetrics>,
    pub by_venue_combination: BTreeMap<String, DepthMetrics>,
    pub best_candidate: Option<CandidateRecord>,
    pub rpc_health: RpcHealth,
}

impl ChainReport {
    fn new(chain: &BreadthChainConfig) -> Self {
        let mut by_depth = BTreeMap::new();
        by_depth.insert("2hop".to_string(), DepthMetrics::default());
        by_depth.insert("3hop".to_string(), DepthMetrics::default());
        by_depth.insert("4hop".to_string(), DepthMetrics::default());
        Self {
            chain: chain.name.clone(),
            chain_id: chain.chain_id,
            tier: chain.tier,
            status: "starting".to_string(),
            last_error: None,
            started_at: Utc::now().to_rfc3339(),
            ended_at: None,
            scans: 0,
            last_scan_ms: 0,
            max_scan_ms: 0,
            last_block: None,
            discovered_pools: 0,
            hot_pools: 0,
            configured_tokens: chain.tokens.len(),
            auto_tokens: 0,
            event_pairs: 0,
            event_pools: 0,
            enumerable_pools: 0,
            factory_scan_failures: 0,
            metadata_failures: 0,
            metadata_retried_from_checkpoint: 0,
            metadata_pending: 0,
            policy_filtered_tokens: 0,
            self_funded_capital_usd: chain.self_funded_capital_usd,
            adapters: chain.dexes.iter().map(|d| format!("{}:{:?}", d.name, d.kind)).collect(),
            opportunity_db_path: chain.opportunity_db_path.clone(),
            native_usd: None,
            cycle_budget_exhausted: false,
            upper_bound_pruned_branches: 0,
            partial_snapshots: 0,
            empty_or_uninitialized_pools: 0,
            by_depth,
            by_venue_combination: BTreeMap::new(),
            best_candidate: None,
            rpc_health: RpcHealth::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct FinalSummary {
    version: String,
    mode: String,
    started_at: String,
    ended_at: String,
    duration_seconds: u64,
    chains: BTreeMap<String, ChainReport>,
    global_best_candidates: Vec<CandidateRecord>,
}

#[derive(Debug, Clone, Serialize)]
struct ChainHealthSummary {
    status: String,
    last_error: Option<String>,
    scans: u64,
    last_scan_ms: u64,
    max_scan_ms: u64,
    rpc: RpcHealth,
}

#[derive(Debug, Clone, Serialize)]
struct HealthSummary {
    version: String,
    generated_at: String,
    chains: BTreeMap<String, ChainHealthSummary>,
}

#[derive(Debug, Clone)]
struct CycleEdge {
    pool_idx: usize,
    token_in: String,
    token_out: String,
}

#[derive(Debug, Clone)]
struct CycleCandidate {
    edges: Vec<CycleEdge>,
    start_addr: String,
    start_symbol: String,
    token_path: Vec<String>,
    spot_edge_bps: f64,
    venue_key: String,
    opportunity_id: String,
}

#[derive(Debug, Clone)]
struct ExactEvaluation {
    input_usd: f64,
    input_amount: f64,
    gross_profit_usd: f64,
    flash_fee_usd: f64,
    after_flash_usd: f64,
    funding_mode: &'static str,
    financing_fee_usd: f64,
    profit_after_financing_usd: f64,
    mev_bid_reserve_usd: f64,
    estimated_execution_fee_usd: Option<f64>,
    estimated_net_usd: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PoolCache {
    version: u32,
    universe_key: String,
    generated_unix: u64,
    #[serde(default)]
    tokens: Vec<TokenConfig>,
    #[serde(default)]
    expansion_stats: UniverseExpansionStats,
    pools: Vec<PoolCacheEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PoolCacheEntry {
    dex_name: String,
    pool_address: String,
    token0: String,
    token1: String,
    fee_tier: Option<u32>,
    #[serde(default)]
    tick_spacing: Option<i32>,
    #[serde(default)]
    fee_bps: Option<f64>,
}

#[derive(Debug, Clone)]
struct DiscoveredPoolUniverse {
    pools: Vec<PoolDefinition>,
    tokens: HashMap<String, TokenConfig>,
    expansion_stats: UniverseExpansionStats,
}

#[derive(Debug, Clone)]
enum DiscoveryProbe {
    V2 { dex: DexConfig, token_a: TokenConfig, token_b: TokenConfig },
    V3 { dex: DexConfig, token_a: TokenConfig, token_b: TokenConfig, fee_tier: u32 },
    Algebra { dex: DexConfig, token_a: TokenConfig, token_b: TokenConfig },
    Solidly { dex: DexConfig, token_a: TokenConfig, token_b: TokenConfig },
    Slipstream { dex: DexConfig, token_a: TokenConfig, token_b: TokenConfig, tick_spacing: i32 },
}

#[derive(Debug, Clone)]
struct DiscoveryHit {
    dex: DexConfig,
    pool_address: String,
    token0: String,
    token1: String,
    fee_tier: Option<u32>,
    tick_spacing: Option<i32>,
}

pub async fn run(config_path: &str, duration_seconds: Option<u64>) -> Result<()> {
    let cfg = BreadthConfig::load(config_path)?;
    fs::create_dir_all(&cfg.output_dir)
        .with_context(|| format!("failed to create V3.4 output directory {}", cfg.output_dir))?;

    let started = Instant::now();
    let started_at = Utc::now().to_rfc3339();
    info!(
        chains = cfg.chains.iter().filter(|c| c.enabled).count(),
        max_depth = 4,
        exact_budget = cfg.exact_quote_budget_per_scan,
        "SCANNER START: read-only bounded 2/3/4-hop multichain search with persistent evidence; signing and submission unavailable"
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let cfg = Arc::new(cfg);
    let mut handles = Vec::new();
    let mut report_handles: Vec<(String, Arc<Mutex<ChainReport>>)> = Vec::new();
    let mut lifetime_handles: Vec<Arc<Mutex<HashMap<String, CandidateRecord>>>> = Vec::new();

    for chain in cfg.chains.iter().filter(|c| c.enabled).cloned() {
        let report = Arc::new(Mutex::new(ChainReport::new(&chain)));
        let lifetimes = Arc::new(Mutex::new(HashMap::<String, CandidateRecord>::new()));
        report_handles.push((chain.name.clone(), report.clone()));
        lifetime_handles.push(lifetimes.clone());
        let global = cfg.clone();
        let rx = shutdown_rx.clone();
        handles.push(tokio::spawn(async move {
            if let Err(err) = run_chain_worker(global, chain, report.clone(), lifetimes, rx).await {
                let mut r = report.lock().await;
                r.status = "failed".to_string();
                r.last_error = Some(err.to_string());
                r.ended_at = Some(Utc::now().to_rfc3339());
                warn!(chain = %r.chain, error = %err, "V3.4 chain worker stopped; other chains continue");
            }
        }));
    }

    if handles.is_empty() { bail!("V3.4 has no enabled chains"); }

    match duration_seconds {
        Some(seconds) if seconds > 0 => {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(seconds)) => {
                    info!(seconds, "V3.4 requested run duration reached; requesting worker shutdown");
                }
                signal = tokio::signal::ctrl_c() => {
                    signal.context("failed waiting for Ctrl+C")?;
                    info!("V3.4 Ctrl+C received; requesting worker shutdown");
                }
            }
        }
        _ => {
            tokio::signal::ctrl_c().await.context("failed waiting for Ctrl+C")?;
            info!("V3.4 Ctrl+C received; requesting worker shutdown");
        }
    }

    let _ = shutdown_tx.send(true);
    for handle in handles {
        if let Err(err) = handle.await { warn!(error = %err, "V3.4 worker join failed"); }
    }
    info!("V3.4 workers stopped; writing final summaries");

    let mut chains = BTreeMap::new();
    for (name, report) in report_handles {
        let mut snapshot = report.lock().await.clone();
        if snapshot.ended_at.is_none() { snapshot.ended_at = Some(Utc::now().to_rfc3339()); }
        chains.insert(name, snapshot);
    }

    let mut all_candidates = Vec::new();
    for lifetimes in lifetime_handles {
        all_candidates.extend(lifetimes.lock().await.values().cloned());
    }
    all_candidates.sort_by(|a, b| candidate_score(b).total_cmp(&candidate_score(a)));
    all_candidates.truncate(cfg.top_candidates);

    let total_scans: u64 = chains.values().map(|report| report.scans).sum();
    let healthy_chain_count = chains.values().filter(|report| report.scans > 0).count();
    let summary = FinalSummary {
        version: env!("CARGO_PKG_VERSION").to_string(),
        mode: "read_only_multichain_scanner".to_string(),
        started_at,
        ended_at: Utc::now().to_rfc3339(),
        duration_seconds: started.elapsed().as_secs(),
        chains: chains.clone(),
        global_best_candidates: all_candidates.clone(),
    };
    let summary_file = format!("{}_summary.json", cfg.artifact_prefix);
    let top_file = format!("{}_top_candidates.json", cfg.artifact_prefix);
    let health_file = format!("{}_health.json", cfg.artifact_prefix);
    write_json(Path::new(&cfg.output_dir).join(&summary_file), &summary)?;
    write_json(Path::new(&cfg.output_dir).join(&top_file), &all_candidates)?;
    let health = HealthSummary {
        version: env!("CARGO_PKG_VERSION").to_string(),
        generated_at: Utc::now().to_rfc3339(),
        chains: chains
            .into_iter()
            .map(|(name, report)| {
                (
                    name,
                    ChainHealthSummary {
                        status: report.status,
                        last_error: report.last_error,
                        scans: report.scans,
                        last_scan_ms: report.last_scan_ms,
                        max_scan_ms: report.max_scan_ms,
                        rpc: report.rpc_health,
                    },
                )
            })
            .collect(),
    };
    write_json(Path::new(&cfg.output_dir).join(&health_file), &health)?;
    info!(
        summary = %summary_file,
        top = %top_file,
        health = %health_file,
        total_scans,
        healthy_chain_count,
        "scanner final files written"
    );
    if total_scans == 0 {
        bail!("scanner exited without completing a successful scan on any enabled chain; inspect the health artifact and RPC configuration");
    }
    Ok(())
}

async fn run_chain_worker(
    global: Arc<BreadthConfig>,
    chain: BreadthChainConfig,
    report: Arc<Mutex<ChainReport>>,
    lifetimes: Arc<Mutex<HashMap<String, CandidateRecord>>>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let rpc_url = env::var(&chain.rpc_url_env)
        .with_context(|| format!("{} is not set for {}", chain.rpc_url_env, chain.name))?;
    let rpc = RpcClient::new(
        rpc_url,
        chain.rpc_max_attempts,
        chain.rpc_retry_base_ms,
        chain.rpc_retry_max_ms,
        chain.rpc_request_timeout_ms,
    )?
    .with_min_request_spacing_ms(chain.rpc_min_request_spacing_ms);
    let actual_chain_id = rpc.chain_id().await.context("failed to read chain id")?;
    if actual_chain_id != chain.chain_id {
        bail!("{} RPC chain id mismatch: expected {}, got {}", chain.name, chain.chain_id, actual_chain_id);
    }

    let discovery_rpc = rpc
        .clone()
        .with_min_request_spacing_ms(
            chain.discovery_rpc_min_request_spacing_ms
                .max(chain.discovery_probe_delay_ms)
                .max(chain.rpc_min_request_spacing_ms),
        )
        .with_retry_policy(
            chain.discovery_rpc_max_attempts,
            chain.rpc_retry_base_ms,
            chain.discovery_rpc_retry_max_ms,
        );
    let universe = tokio::select! {
        result = load_or_discover_pools(
            &chain,
            &discovery_rpc,
            &global.multicall3_address,
            global.multicall_max_calls,
        ) => result?,
        changed = shutdown.changed() => {
            if changed.is_err() || *shutdown.borrow() {
                let mut r = report.lock().await;
                r.status = "stopped_during_discovery".to_string();
                r.ended_at = Some(Utc::now().to_rfc3339());
                return Ok(());
            }
            bail!("shutdown watch changed unexpectedly during discovery");
        }
    };
    let token_by_address = universe.tokens;
    let pools = universe.pools;
    let expansion_stats = universe.expansion_stats;
    if pools.is_empty() { bail!("{} discovered zero configured/expanded pools", chain.name); }
    let mut opportunity_db = init_opportunity_db(&chain.opportunity_db_path)
        .with_context(|| format!("{} failed to initialize opportunity DB {}", chain.name, chain.opportunity_db_path))?;

    {
        let mut r = report.lock().await;
        r.discovered_pools = pools.len();
        r.auto_tokens = expansion_stats.auto_tokens_added;
        r.event_pairs = expansion_stats.event_pairs_retained;
        r.event_pools = expansion_stats.event_pools_retained;
        r.enumerable_pools = expansion_stats.enumerable_pools_discovered;
        r.factory_scan_failures = expansion_stats.factory_scan_failures;
        r.metadata_failures = expansion_stats.metadata_failures;
        r.metadata_retried_from_checkpoint = expansion_stats.metadata_retried_from_checkpoint;
        r.metadata_pending = expansion_stats.metadata_pending;
        r.policy_filtered_tokens = expansion_stats.policy_filtered_tokens;
        r.status = "running".to_string();
    }

    let mut hot_addresses = HashSet::<String>::new();
    let mut last_liquidity_refresh_block = 0u64;
    let mut last_summary = Instant::now() - Duration::from_secs(global.summary_interval_secs);
    let mut seen_after_flash: HashMap<usize, HashSet<String>> = HashMap::new();
    let mut seen_after_financing: HashMap<usize, HashSet<String>> = HashMap::new();
    let mut seen_net: HashMap<usize, HashSet<String>> = HashMap::new();
    let mut warned_quote_failures = HashSet::<String>::new();
    let mut last_scan_error_logged: Option<String> = None;

    loop {
        if *shutdown.borrow() { break; }
        let scan_started = Instant::now();
        let before_stats = rpc.stats_snapshot();
        let scan_result = tokio::select! {
            result = scan_chain(
                &global,
                &chain,
                &rpc,
                &pools,
                &token_by_address,
                &mut hot_addresses,
                &mut last_liquidity_refresh_block,
                &report,
                &lifetimes,
                &mut opportunity_db,
                &mut seen_after_flash,
                &mut seen_after_financing,
                &mut seen_net,
                &mut warned_quote_failures,
            ) => result,
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { break; }
                continue;
            }
        };

        match scan_result {
            Err(err) => {
                let message = err.to_string();
                {
                    let mut r = report.lock().await;
                    r.last_error = Some(message.clone());
                }
                if last_scan_error_logged.as_deref() != Some(message.as_str()) {
                    warn!(chain = %chain.name, error = %err, "V3.4 chain scan failed; identical repeat errors suppressed until recovery/change");
                    last_scan_error_logged = Some(message);
                }
            }
            Ok(()) => {
                if last_scan_error_logged.take().is_some() {
                    let mut r = report.lock().await;
                    r.last_error = None;
                    info!(chain = %chain.name, "V3.4 chain scan recovered");
                }
            }
        }

        let after_stats = rpc.stats_snapshot();
        let scan_ms = scan_started.elapsed().as_millis().min(u64::MAX as u128) as u64;
        {
            let mut r = report.lock().await;
            r.rpc_health.requests = after_stats.requests;
            r.rpc_health.retries = after_stats.retries;
            r.rpc_health.failures = after_stats.failures;
            r.rpc_health.rate_limits = after_stats.rate_limits;
            r.last_scan_ms = scan_ms;
            r.max_scan_ms = r.max_scan_ms.max(scan_ms);
            let _ = before_stats;
        }

        if last_summary.elapsed() >= Duration::from_secs(global.summary_interval_secs) {
            log_chain_summary(&report).await;
            last_summary = Instant::now();
        }

        let elapsed = scan_started.elapsed();
        let target = Duration::from_millis(chain.poll_interval_ms);
        let sleep_for = target.saturating_sub(elapsed);
        tokio::select! {
            _ = tokio::time::sleep(sleep_for) => {}
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { break; }
            }
        }
    }

    let mut r = report.lock().await;
    r.status = "stopped".to_string();
    r.ended_at = Some(Utc::now().to_rfc3339());
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn scan_chain(
    global: &BreadthConfig,
    chain: &BreadthChainConfig,
    rpc: &RpcClient,
    pools: &[PoolDefinition],
    tokens: &HashMap<String, TokenConfig>,
    hot_addresses: &mut HashSet<String>,
    last_liquidity_refresh_block: &mut u64,
    report: &Arc<Mutex<ChainReport>>,
    lifetimes: &Arc<Mutex<HashMap<String, CandidateRecord>>>,
    opportunity_db: &mut Connection,
    seen_after_flash: &mut HashMap<usize, HashSet<String>>,
    seen_after_financing: &mut HashMap<usize, HashSet<String>>,
    seen_net: &mut HashMap<usize, HashSet<String>>,
    warned_quote_failures: &mut HashSet<String>,
) -> Result<()> {
    let block = rpc.block_number().await?;
    let gas_price_wei = rpc.gas_price_wei().await?;
    let refresh = hot_addresses.is_empty()
        || *last_liquidity_refresh_block == 0
        || block.saturating_sub(*last_liquidity_refresh_block) >= chain.liquidity_refresh_blocks;
    let targets = if refresh {
        pools.to_vec()
    } else {
        pools.iter().filter(|p| hot_addresses.contains(&p.pool_address.to_ascii_lowercase())).cloned().collect()
    };
    if targets.is_empty() { bail!("{} has zero state targets", chain.name); }

    let snapshot = rpc.pool_states_multicall_at(
        &global.multicall3_address,
        &targets,
        block,
        global.multicall_max_calls,
    ).await?;
    if !snapshot.complete {
        // Breadth mode may skip a broken/retired pool, but do not print the same warning
        // every few seconds for an overnight run. Count every degraded snapshot and emit
        // the diagnostic line once; the periodic summary carries the running counter.
        let first_partial = {
            let mut r = report.lock().await;
            let first = r.partial_snapshots == 0;
            r.partial_snapshots = r.partial_snapshots.saturating_add(1);
            first
        };
        if first_partial {
            warn!(
                chain = %chain.name,
                returned = snapshot.returned_calls,
                expected = snapshot.expected_calls,
                inner_failures = snapshot.inner_failures,
                decode_failures = snapshot.decode_failures,
                usable_states = snapshot.states.len(),
                "V3.4 first partial state snapshot; failed pools will be skipped and counted in aggregate telemetry"
            );
        }
    }
    {
        let mut r = report.lock().await;
        r.empty_or_uninitialized_pools = r.empty_or_uninitialized_pools
            .saturating_add(snapshot.empty_or_uninitialized_pools as u64);
    }
    if snapshot.states.len() < 2 {
        bail!("{} has fewer than two usable pool states (empty_or_uninitialized={})", chain.name, snapshot.empty_or_uninitialized_pools);
    }

    let native_usd = derive_native_usd(&snapshot.states, tokens);
    // Infer USD anchors through connected pool prices before hot-liquidity pruning.
    // Without this, an alt/alt middle pool (e.g. ARB/LINK) would be discarded even
    // when it is part of a perfectly valid stable/native-start 3- or 4-hop cycle.
    let inferred_usd = infer_token_usd_prices(&snapshot.states, tokens, native_usd);
    let states = if refresh {
        let liquidity_floor = chain.min_pool_anchor_liquidity_usd
            .unwrap_or(global.min_pool_anchor_liquidity_usd);
        let filtered = filter_states_by_anchor_liquidity(
            snapshot.states,
            &inferred_usd,
            liquidity_floor,
        );
        hot_addresses.clear();
        hot_addresses.extend(filtered.iter().map(|s| s.pool_address().to_ascii_lowercase()));
        *last_liquidity_refresh_block = block;
        filtered
    } else {
        snapshot.states
    };
    if states.len() < 2 {
        bail!("{} has fewer than two hot pools", chain.name);
    }

    let financeable_starts = chain.tokens.iter()
        .map(|t| t.address.to_ascii_lowercase())
        .collect::<HashSet<_>>();
    // If the minimum trade can be self-funded, do not discard sub-flash-hurdle
    // marginal edges. Those routes may be economically valid without Aave.
    let financing_hurdle_bps = if chain.self_funded_capital_usd >= global.min_trade_usd {
        0.0
    } else {
        chain.flash_loan_premium_bps
    };
    let (mut cycles, exhausted, generated, pruned_branches) = enumerate_cycles(
        &chain.name,
        &states,
        tokens,
        &inferred_usd,
        &financeable_starts,
        financing_hurdle_bps,
        global.max_cycles_per_depth,
    );
    // Rank exact-quote finalists by cheap local dollar economics rather than spot
    // bps alone. This prevents a deep 1-4 bps self-funded route from being starved
    // by a shallow high-bps route that can only absorb a few dollars.
    cycles.sort_by(|a, b| {
        let a_score = cycle_prefilter_score_usd(global, chain, &states, tokens, &inferred_usd, a);
        let b_score = cycle_prefilter_score_usd(global, chain, &states, tokens, &inferred_usd, b);
        b_score.total_cmp(&a_score).then_with(|| b.spot_edge_bps.total_cmp(&a.spot_edge_bps))
    });

    let observed_at = Utc::now().to_rfc3339();
    // Reserve exact-quote capacity across route depths so a busy 2-hop surface cannot
    // starve 3/4-hop survivors during the experiment we explicitly want to measure.
    let exact_quotas = exact_quote_quotas(
        &cycles,
        financing_hurdle_bps,
        global.exact_quote_budget_per_scan,
    );
    let mut exact_used_by_depth = HashMap::<usize, usize>::new();
    let mut new_best_lines = Vec::new();

    {
        let mut r = report.lock().await;
        r.scans = r.scans.saturating_add(1);
        r.last_block = Some(block);
        r.hot_pools = states.len();
        r.native_usd = native_usd;
        r.cycle_budget_exhausted |= exhausted;
        r.upper_bound_pruned_branches = r.upper_bound_pruned_branches.saturating_add(pruned_branches);
        for depth in 2..=4 {
            let m = r.by_depth.get_mut(&format!("{}hop", depth)).expect("depth metric exists");
            let n = generated.get(&depth).copied().unwrap_or(0) as u64;
            m.cycles_generated = m.cycles_generated.saturating_add(n);
            m.cycles_liquidity_eligible = m.cycles_liquidity_eligible.saturating_add(n);
        }
    }

    for cycle in &mut cycles {
        let depth = cycle.edges.len();
        let depth_key = format!("{}hop", depth);
        {
            let mut r = report.lock().await;
            {
                let m = r.by_depth.get_mut(&depth_key).expect("depth metric exists");
                update_best(&mut m.best_edge_bps, cycle.spot_edge_bps);
                if cycle.spot_edge_bps > 0.0 {
                    m.marginal_positive = m.marginal_positive.saturating_add(1);
                }
            }
            {
                let vm = r.by_venue_combination.entry(format!("{}:{}", depth_key, cycle.venue_key)).or_default();
                vm.cycles_generated = vm.cycles_generated.saturating_add(1);
                vm.cycles_liquidity_eligible = vm.cycles_liquidity_eligible.saturating_add(1);
                update_best(&mut vm.best_edge_bps, cycle.spot_edge_bps);
                if cycle.spot_edge_bps > 0.0 {
                    vm.marginal_positive = vm.marginal_positive.saturating_add(1);
                }
            }
        }
        if cycle.spot_edge_bps > chain.flash_loan_premium_bps {
            let mut r = report.lock().await;
            r.by_depth.get_mut(&depth_key).unwrap().marginal_above_flash += 1;
            r.by_venue_combination.entry(format!("{}:{}", depth_key, cycle.venue_key)).or_default().marginal_above_flash += 1;
        }
        if cycle.spot_edge_bps <= financing_hurdle_bps { continue; }
        let quota = exact_quotas.get(&depth).copied().unwrap_or(0);
        let used = exact_used_by_depth.entry(depth).or_insert(0);
        if *used >= quota { continue; }
        *used += 1;

        let start_price = inferred_usd.get(&cycle.start_addr.to_ascii_lowercase()).copied();
        let Some(start_price) = start_price.filter(|v| v.is_finite() && *v > 0.0) else { continue; };
        let start_amount = global.min_trade_usd / start_price;
        {
            let mut r = report.lock().await;
            r.by_depth.get_mut(&depth_key).unwrap().exact_quote_attempts += 1;
            r.by_venue_combination.entry(format!("{}:{}", depth_key, cycle.venue_key)).or_default().exact_quote_attempts += 1;
        }
        let finite_out = match quote_route_exact(rpc, &states, tokens, cycle, start_amount, block).await {
            Ok(v) => {
                let mut r = report.lock().await;
                r.by_depth.get_mut(&depth_key).unwrap().exact_quote_successes += 1;
                r.by_venue_combination.entry(format!("{}:{}", depth_key, cycle.venue_key)).or_default().exact_quote_successes += 1;
                v
            }
            Err(err) => {
                let mut r = report.lock().await;
                r.by_depth.get_mut(&depth_key).unwrap().exact_quote_failures += 1;
                r.by_venue_combination.entry(format!("{}:{}", depth_key, cycle.venue_key)).or_default().exact_quote_failures += 1;
                if warned_quote_failures.insert(cycle.opportunity_id.clone()) {
                    warn!(chain = %chain.name, block, id = %cycle.opportunity_id, error = %err, "V3.4 first exact quote anomaly for opportunity; repeats suppressed");
                }
                continue;
            }
        };

        let probe = evaluate_economics(global, chain, gas_price_wei, native_usd, depth, start_price, global.min_trade_usd, start_amount, finite_out);
        record_economic_metrics(report, &depth_key, &cycle.venue_key, &probe).await;
        if probe.gross_profit_usd > 0.0 && probe.profit_after_financing_usd <= 0.0 {
            let record = build_candidate_record(chain, &states, cycle, block, &observed_at, &probe);
            persist_candidate(opportunity_db, &record)?;
        }
        if probe.profit_after_financing_usd <= 0.0 { continue; }

        let first_after_flash = if probe.after_flash_usd > 0.0 {
            seen_after_flash.entry(depth).or_default().insert(cycle.opportunity_id.clone())
        } else { false };
        let first_after_financing = seen_after_financing
            .entry(depth)
            .or_default()
            .insert(cycle.opportunity_id.clone());

        let mut best = probe.clone();
        let probe_score = economics_score(&probe);
        let max_input_usd = global.max_trade_usd;
        {
            let mut r = report.lock().await;
            r.by_depth.get_mut(&depth_key).unwrap().sizing_routes += 1;
            r.by_venue_combination.entry(format!("{}:{}", depth_key, cycle.venue_key)).or_default().sizing_routes += 1;
        }

        let analytical_grid = analytical_size_grid_usd(
            global, chain, &states, tokens, cycle, start_price, max_input_usd,
        );
        let analytical_used = analytical_grid.is_some();
        let grid = analytical_grid.unwrap_or_else(|| {
            geometric_grid(global.min_trade_usd, max_input_usd, global.sizing_grid_points)
        });
        {
            let mut r = report.lock().await;
            {
                let m = r.by_depth.get_mut(&depth_key).unwrap();
                if analytical_used {
                    m.analytical_sizing_routes += 1;
                } else if global.analytical_sizing_enabled {
                    m.analytical_sizing_fallbacks += 1;
                }
            }
            {
                let vm = r.by_venue_combination.entry(format!("{}:{}", depth_key, cycle.venue_key)).or_default();
                if analytical_used {
                    vm.analytical_sizing_routes += 1;
                } else if global.analytical_sizing_enabled {
                    vm.analytical_sizing_fallbacks += 1;
                }
            }
        }
        let mut evaluated_sizes = vec![global.min_trade_usd];
        for input_usd in grid.into_iter().skip(1) {
            let amount_in = input_usd / start_price;
            let out = match quote_route_exact(rpc, &states, tokens, cycle, amount_in, block).await {
                Ok(v) => v,
                Err(_) => continue,
            };
            let e = evaluate_economics(global, chain, gas_price_wei, native_usd, depth, start_price, input_usd, amount_in, out);
            evaluated_sizes.push(input_usd);
            {
                let mut r = report.lock().await;
                {
                    let m = r.by_depth.get_mut(&depth_key).unwrap();
                    m.sizing_evals += 1;
                    if analytical_used { m.analytical_exact_validations += 1; }
                }
                {
                    let vm = r.by_venue_combination.entry(format!("{}:{}", depth_key, cycle.venue_key)).or_default();
                    vm.sizing_evals += 1;
                    if analytical_used { vm.analytical_exact_validations += 1; }
                }
            }
            if economics_score(&e) > economics_score(&best) { best = e; }
        }

        for _ in 0..global.sizing_refine_iterations {
            evaluated_sizes.sort_by(|a, b| a.total_cmp(b));
            let center = best.input_usd;
            let pos = evaluated_sizes.iter().position(|v| (*v - center).abs() <= f64::EPSILON).unwrap_or(0);
            let lo = if pos > 0 { evaluated_sizes[pos - 1] } else { global.min_trade_usd };
            let hi = if pos + 1 < evaluated_sizes.len() { evaluated_sizes[pos + 1] } else { max_input_usd };
            if hi <= lo * 1.01 { break; }
            let refine = (lo * hi).sqrt();
            if evaluated_sizes.iter().any(|v| (*v - refine).abs() / refine.max(1.0) < 1e-6) { break; }
            let amount_in = refine / start_price;
            if let Ok(out) = quote_route_exact(rpc, &states, tokens, cycle, amount_in, block).await {
                let e = evaluate_economics(global, chain, gas_price_wei, native_usd, depth, start_price, refine, amount_in, out);
                evaluated_sizes.push(refine);
                let mut r = report.lock().await;
                r.by_depth.get_mut(&depth_key).unwrap().sizing_evals += 1;
                r.by_venue_combination.entry(format!("{}:{}", depth_key, cycle.venue_key)).or_default().sizing_evals += 1;
                drop(r);
                if economics_score(&e) > economics_score(&best) { best = e; }
            }
        }

        {
            let mut r = report.lock().await;
            {
                let m = r.by_depth.get_mut(&depth_key).unwrap();
                if economics_score(&best) > probe_score { m.sizing_improved += 1; }
                if best.estimated_net_usd.unwrap_or(f64::NEG_INFINITY) > 0.0 { m.sizing_net_positive += 1; }
                update_best(&mut m.best_input_usd, best.input_usd);
                update_best(&mut m.best_gross_usd, best.gross_profit_usd);
                update_best(&mut m.best_after_flash_usd, best.after_flash_usd);
                update_best(&mut m.best_after_financing_usd, best.profit_after_financing_usd);
                if let Some(net) = best.estimated_net_usd { update_best(&mut m.best_estimated_net_usd, net); }
            }
            {
                let vm = r.by_venue_combination.entry(format!("{}:{}", depth_key, cycle.venue_key)).or_default();
                if economics_score(&best) > probe_score { vm.sizing_improved += 1; }
                if best.estimated_net_usd.unwrap_or(f64::NEG_INFINITY) > 0.0 { vm.sizing_net_positive += 1; }
                update_best(&mut vm.best_input_usd, best.input_usd);
                update_best(&mut vm.best_gross_usd, best.gross_profit_usd);
                update_best(&mut vm.best_after_flash_usd, best.after_flash_usd);
                update_best(&mut vm.best_after_financing_usd, best.profit_after_financing_usd);
                if let Some(net) = best.estimated_net_usd { update_best(&mut vm.best_estimated_net_usd, net); }
            }
        }

        let first_net_positive = if best.estimated_net_usd.unwrap_or(f64::NEG_INFINITY) > 0.0 {
            seen_net.entry(depth).or_default().insert(cycle.opportunity_id.clone())
        } else { false };

        let record = build_candidate_record(chain, &states, cycle, block, &observed_at, &best);
        let updated = update_lifetime(lifetimes, record).await;
        persist_candidate(opportunity_db, &updated)?;
        let mut emit = false;
        {
            let mut r = report.lock().await;
            let current_score = r.best_candidate.as_ref().map(candidate_score).unwrap_or(f64::NEG_INFINITY);
            if candidate_score(&updated) > current_score {
                r.best_candidate = Some(updated.clone());
                emit = true;
            }
            for d in 2..=4 {
                let key = format!("{}hop", d);
                if let Some(m) = r.by_depth.get_mut(&key) {
                    m.unique_after_flash_positive = seen_after_flash.get(&d).map(|s| s.len() as u64).unwrap_or(0);
                    m.unique_after_financing_positive = seen_after_financing.get(&d).map(|s| s.len() as u64).unwrap_or(0);
                    m.unique_net_positive = seen_net.get(&d).map(|s| s.len() as u64).unwrap_or(0);
                }
            }
        }
        let serious_net = updated.estimated_net_usd
            .map(|net| net >= chain.min_net_profit_usd)
            .unwrap_or(false);
        if emit || first_after_flash || first_after_financing || (first_net_positive && serious_net) {
            new_best_lines.push(updated);
        }
    }

    for c in new_best_lines {
        info!(
            chain = %c.chain,
            block = c.block_number,
            depth = c.route_depth,
            id = %c.opportunity_id,
            edge_bps = format!("{:.3}", c.spot_edge_bps),
            after_flash_usd = format!("{:.6}", c.after_flash_usd),
            funding = %c.funding_mode,
            after_financing_usd = format!("{:.6}", c.profit_after_financing_usd),
            estimated_net_usd = ?c.estimated_net_usd.map(|v| format!("{v:.6}")),
            input_usd = format!("{:.2}", c.input_usd),
            "V3.4 NEW_BEST"
        );
    }

    Ok(())
}

fn cycle_prefilter_score_usd(
    global: &BreadthConfig,
    chain: &BreadthChainConfig,
    states: &[PoolState],
    tokens: &HashMap<String, TokenConfig>,
    inferred_usd: &HashMap<String, f64>,
    cycle: &CycleCandidate,
) -> f64 {
    let Some(start_price) = inferred_usd
        .get(&cycle.start_addr.to_ascii_lowercase())
        .copied()
        .filter(|v| v.is_finite() && *v > 0.0)
    else {
        return cycle.spot_edge_bps * global.min_trade_usd / 10_000.0;
    };
    let cap = if chain.self_funded_capital_usd >= global.min_trade_usd {
        chain.self_funded_capital_usd.min(global.max_trade_usd)
    } else {
        global.min_trade_usd
    };
    let mid = (global.min_trade_usd * cap).sqrt();
    let mut best = f64::NEG_INFINITY;
    for input_usd in [global.min_trade_usd, mid, cap] {
        if input_usd <= 0.0 || !input_usd.is_finite() { continue; }
        let amount_in = input_usd / start_price;
        let Some(out) = quote_route_local_state(states, tokens, cycle, amount_in) else { continue; };
        let gross = (out - amount_in) * start_price;
        let financing_fee = if chain.self_funded_capital_usd > 0.0
            && input_usd <= chain.self_funded_capital_usd + 1e-9
        {
            0.0
        } else {
            input_usd * chain.flash_loan_premium_bps / 10_000.0
        };
        let score = gross - financing_fee;
        if score.is_finite() { best = best.max(score); }
    }
    if best.is_finite() {
        best
    } else {
        cycle.spot_edge_bps * global.min_trade_usd / 10_000.0
    }
}

fn exact_quote_quotas(
    cycles: &[CycleCandidate],
    flash_hurdle_bps: f64,
    total_budget: usize,
) -> HashMap<usize, usize> {
    let mut counts = (2usize..=4)
        .map(|depth| {
            let n = cycles.iter()
                .filter(|c| c.edges.len() == depth && c.spot_edge_bps > flash_hurdle_bps)
                .count();
            (depth, n)
        })
        .filter(|(_, n)| *n > 0)
        .collect::<Vec<_>>();
    let mut quotas = HashMap::new();
    if total_budget == 0 || counts.is_empty() { return quotas; }

    // Depth fairness first, then round-robin spare slots without assigning more
    // quota than a depth actually has survivors. With the default budget of 12 and
    // healthy survivor sets this converges to 4/4/4.
    counts.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    let mut remaining = total_budget;
    for (depth, _) in &counts {
        if remaining == 0 { break; }
        quotas.insert(*depth, 1);
        remaining -= 1;
    }
    while remaining > 0 {
        let mut progressed = false;
        for (depth, count) in &counts {
            if remaining == 0 { break; }
            let q = quotas.entry(*depth).or_insert(0);
            if *q < *count {
                *q += 1;
                remaining -= 1;
                progressed = true;
            }
        }
        if !progressed { break; }
    }
    quotas
}

async fn record_economic_metrics(report: &Arc<Mutex<ChainReport>>, depth_key: &str, venue_key: &str, e: &ExactEvaluation) {
    let mut r = report.lock().await;
    {
        let m = r.by_depth.get_mut(depth_key).unwrap();
        if e.gross_profit_usd > 0.0 { m.finite_gross_positive += 1; }
        if e.after_flash_usd > 0.0 { m.finite_after_flash_positive += 1; }
        if e.profit_after_financing_usd > 0.0 { m.finite_after_financing_positive += 1; }
        update_best(&mut m.best_gross_usd, e.gross_profit_usd);
        update_best(&mut m.best_after_flash_usd, e.after_flash_usd);
        update_best(&mut m.best_after_financing_usd, e.profit_after_financing_usd);
        if let Some(net) = e.estimated_net_usd { update_best(&mut m.best_estimated_net_usd, net); }
    }
    {
        let vm = r.by_venue_combination.entry(format!("{}:{}", depth_key, venue_key)).or_default();
        if e.gross_profit_usd > 0.0 { vm.finite_gross_positive += 1; }
        if e.after_flash_usd > 0.0 { vm.finite_after_flash_positive += 1; }
        if e.profit_after_financing_usd > 0.0 { vm.finite_after_financing_positive += 1; }
        update_best(&mut vm.best_gross_usd, e.gross_profit_usd);
        update_best(&mut vm.best_after_flash_usd, e.after_flash_usd);
        update_best(&mut vm.best_after_financing_usd, e.profit_after_financing_usd);
        if let Some(net) = e.estimated_net_usd { update_best(&mut vm.best_estimated_net_usd, net); }
    }
}

fn evaluate_economics(
    _global: &BreadthConfig,
    chain: &BreadthChainConfig,
    gas_price_wei: u128,
    native_usd: Option<f64>,
    depth: usize,
    start_price: f64,
    input_usd: f64,
    input_amount: f64,
    final_out: f64,
) -> ExactEvaluation {
    let gross_profit_usd = (final_out - input_amount) * start_price;
    let flash_fee_usd = input_usd * chain.flash_loan_premium_bps / 10_000.0;
    let after_flash_usd = gross_profit_usd - flash_fee_usd;
    let self_funded = chain.self_funded_capital_usd > 0.0
        && input_usd <= chain.self_funded_capital_usd + 1e-9;
    let funding_mode = if self_funded { "self" } else { "flash" };
    let financing_fee_usd = if self_funded { 0.0 } else { flash_fee_usd };
    let profit_after_financing_usd = gross_profit_usd - financing_fee_usd;
    let positive_after_financing = profit_after_financing_usd.max(0.0);
    let mev_bid_reserve_usd = positive_after_financing * chain.mev_bid_reserve_pct / 100.0;
    let estimated_execution_fee_usd = native_usd.map(|native| {
        let extra_hops = depth.saturating_sub(2) as u64;
        let gas_units = chain.fee_model.base_gas_units
            .saturating_add(chain.fee_model.gas_units_per_extra_hop.saturating_mul(extra_hops));
        gas_price_wei as f64 / 1e18 * gas_units as f64 * native
            + chain.fee_model.l1_data_fee_usd_proxy
            + chain.fee_model.operator_fee_usd_proxy
    });
    let estimated_net_usd = estimated_execution_fee_usd
        .map(|fee| profit_after_financing_usd - mev_bid_reserve_usd - fee);
    ExactEvaluation {
        input_usd,
        input_amount,
        gross_profit_usd,
        flash_fee_usd,
        after_flash_usd,
        funding_mode,
        financing_fee_usd,
        profit_after_financing_usd,
        mev_bid_reserve_usd,
        estimated_execution_fee_usd,
        estimated_net_usd,
    }
}

fn economics_score(e: &ExactEvaluation) -> f64 {
    e.estimated_net_usd.unwrap_or(e.profit_after_financing_usd)
}

fn analytical_size_grid_usd(
    global: &BreadthConfig,
    chain: &BreadthChainConfig,
    states: &[PoolState],
    tokens: &HashMap<String, TokenConfig>,
    cycle: &CycleCandidate,
    start_price: f64,
    max_input_usd: f64,
) -> Option<Vec<f64>> {
    if !global.analytical_sizing_enabled || start_price <= 0.0 || max_input_usd <= global.min_trade_usd {
        return None;
    }

    let local_after_financing = |input_usd: f64| -> Option<f64> {
        if !input_usd.is_finite() || input_usd <= 0.0 { return None; }
        let amount_in = input_usd / start_price;
        let amount_out = quote_route_local_state(states, tokens, cycle, amount_in)?;
        let gross_usd = (amount_out - amount_in) * start_price;
        let flash_usd = input_usd * chain.flash_loan_premium_bps / 10_000.0;
        let financing_usd = if chain.self_funded_capital_usd > 0.0
            && input_usd <= chain.self_funded_capital_usd + 1e-9 { 0.0 } else { flash_usd };
        let net = gross_usd - financing_usd;
        if net.is_finite() { Some(net) } else { None }
    };

    // The minimum trade must be locally representable or the route safely falls back
    // to exact RPC sizing. Concentrated-liquidity local math refuses to cross an
    // unknown initialized boundary rather than extrapolating liquidity.
    local_after_financing(global.min_trade_usd)?;

    let mut feasible_hi = max_input_usd;
    if local_after_financing(feasible_hi).is_none() {
        let mut lo = global.min_trade_usd;
        let mut hi = max_input_usd;
        for _ in 0..global.analytical_sizing_iterations.min(40) {
            if hi <= lo * 1.000_001 { break; }
            let mid = (lo * hi).sqrt();
            if local_after_financing(mid).is_some() { lo = mid; } else { hi = mid; }
        }
        feasible_hi = lo;
    }
    if feasible_hi <= global.min_trade_usd * 1.001 { return None; }

    let (opt, score) = golden_section_max(
        global.min_trade_usd,
        feasible_hi,
        global.analytical_sizing_iterations,
        |usd| local_after_financing(usd).unwrap_or(f64::NEG_INFINITY),
    );
    if !opt.is_finite() || !score.is_finite() { return None; }

    // If the local optimum is pinned to a concentrated-liquidity visibility boundary,
    // the true optimum may sit beyond it after a tick transition. Do not pretend the
    // local interval solved that route; use the existing exact fallback instead.
    if feasible_hi < max_input_usd * 0.999 && opt >= feasible_hi * 0.985 {
        return None;
    }

    let band = global.analytical_validation_band_pct / 100.0;
    let mut sizes = vec![
        global.min_trade_usd,
        (opt * (1.0 - band)).clamp(global.min_trade_usd, max_input_usd),
        opt.clamp(global.min_trade_usd, max_input_usd),
        (opt * (1.0 + band)).clamp(global.min_trade_usd, max_input_usd),
    ];
    if chain.self_funded_capital_usd > global.min_trade_usd
        && chain.self_funded_capital_usd < max_input_usd {
        let cap = chain.self_funded_capital_usd;
        sizes.push((cap * 0.99).max(global.min_trade_usd));
        sizes.push(cap);
        sizes.push((cap * 1.01).min(max_input_usd));
    }
    sizes.sort_by(|a, b| a.total_cmp(b));
    sizes.dedup_by(|a, b| (*a - *b).abs() / (*a).abs().max((*b).abs()).max(1.0) < 1e-6);
    Some(sizes)
}

fn quote_route_local_state(
    states: &[PoolState],
    tokens: &HashMap<String, TokenConfig>,
    cycle: &CycleCandidate,
    amount_in: f64,
) -> Option<f64> {
    let mut amount = amount_in;
    for edge in &cycle.edges {
        let state = states.get(edge.pool_idx)?;
        amount = match state.def().dex.kind {
            DexKind::V2 if !state.def().dex.syncswap_classic => {
                v2_leg_output(amount, state, &edge.token_in, &edge.token_out)?
            }
            DexKind::V3 | DexKind::Algebra | DexKind::Slipstream => {
                concentrated_leg_output_current_interval(
                    amount,
                    state,
                    &edge.token_in,
                    &edge.token_out,
                ).ok()?
            }
            // Stable Solidly and SyncSwap finite-size math remains contract-authoritative.
            // They safely use the exact RPC fallback rather than a guessed local invariant.
            DexKind::Solidly | DexKind::V2 => return None,
        };
        if !amount.is_finite() || amount <= 0.0 { return None; }
        if !tokens.contains_key(&edge.token_out.to_ascii_lowercase()) { return None; }
    }
    Some(amount)
}

async fn quote_route_exact(
    rpc: &RpcClient,
    states: &[PoolState],
    tokens: &HashMap<String, TokenConfig>,
    cycle: &CycleCandidate,
    amount_in: f64,
    block: u64,
) -> Result<f64> {
    let mut amount = amount_in;
    for edge in &cycle.edges {
        let state = states.get(edge.pool_idx).ok_or_else(|| anyhow!("bad pool index"))?;
        match state.def().dex.kind {
            DexKind::V2 => {
                if state.def().dex.syncswap_classic {
                    let token_in = tokens.get(&edge.token_in.to_ascii_lowercase()).ok_or_else(|| anyhow!("unknown token"))?;
                    let token_out = tokens.get(&edge.token_out.to_ascii_lowercase()).ok_or_else(|| anyhow!("unknown token"))?;
                    let raw_in = amount_to_raw(amount, token_in.decimals)?;
                    let raw_out = rpc.quote_syncswap_classic_exact_input_at(
                        state.pool_address(), &edge.token_in, raw_in, block,
                    ).await?;
                    amount = raw_to_amount(raw_out, token_out.decimals);
                } else {
                    amount = v2_leg_output(amount, state, &edge.token_in, &edge.token_out)
                        .ok_or_else(|| anyhow!("V2 exact quote failed"))?;
                }
            }
            DexKind::V3 => {
                let token_in = tokens.get(&edge.token_in.to_ascii_lowercase()).ok_or_else(|| anyhow!("unknown token"))?;
                let token_out = tokens.get(&edge.token_out.to_ascii_lowercase()).ok_or_else(|| anyhow!("unknown token"))?;
                let raw_in = amount_to_raw(amount, token_in.decimals)?;
                let quoter = state.def().dex.quoter_v2.as_deref().ok_or_else(|| anyhow!("missing V3 quoter"))?;
                let fee = state.def().fee_tier.ok_or_else(|| anyhow!("missing V3 fee tier"))?;
                let quote = rpc.quote_v3_exact_input_at(quoter, &edge.token_in, &edge.token_out, raw_in, fee, block).await?;
                amount = raw_to_amount(quote.amount_out_raw, token_out.decimals);
            }
            DexKind::Algebra => {
                let token_in = tokens.get(&edge.token_in.to_ascii_lowercase()).ok_or_else(|| anyhow!("unknown token"))?;
                let token_out = tokens.get(&edge.token_out.to_ascii_lowercase()).ok_or_else(|| anyhow!("unknown token"))?;
                let raw_in = amount_to_raw(amount, token_in.decimals)?;
                let quoter = state.def().dex.quoter_v2.as_deref().ok_or_else(|| anyhow!("missing Algebra quoter"))?;
                let (raw_out, _) = rpc.quote_algebra_exact_input_at(quoter, &edge.token_in, &edge.token_out, raw_in, block).await?;
                amount = raw_to_amount(raw_out, token_out.decimals);
            }
            DexKind::Solidly => {
                let token_in = tokens.get(&edge.token_in.to_ascii_lowercase()).ok_or_else(|| anyhow!("unknown token"))?;
                let token_out = tokens.get(&edge.token_out.to_ascii_lowercase()).ok_or_else(|| anyhow!("unknown token"))?;
                let raw_in = amount_to_raw(amount, token_in.decimals)?;
                let raw_out = rpc.quote_solidly_exact_input_at(
                    state.pool_address(),
                    &edge.token_in,
                    raw_in,
                    block,
                ).await?;
                amount = raw_to_amount(raw_out, token_out.decimals);
            }
            DexKind::Slipstream => {
                let token_in = tokens.get(&edge.token_in.to_ascii_lowercase()).ok_or_else(|| anyhow!("unknown token"))?;
                let token_out = tokens.get(&edge.token_out.to_ascii_lowercase()).ok_or_else(|| anyhow!("unknown token"))?;
                let raw_in = amount_to_raw(amount, token_in.decimals)?;
                let quoter = state.def().dex.quoter_v2.as_deref().ok_or_else(|| anyhow!("missing Slipstream quoter"))?;
                let spacing = state.def().tick_spacing.ok_or_else(|| anyhow!("missing Slipstream tick spacing"))?;
                let raw_out = if let Some(mask) = state.def().dex.slipstream_factory_mask {
                    rpc.quote_slipstream_mixed_v3_exact_input_at(
                        quoter, &edge.token_in, &edge.token_out, raw_in, spacing, mask, block,
                    ).await?
                } else {
                    rpc.quote_slipstream_exact_input_at(
                        quoter, &edge.token_in, &edge.token_out, raw_in, spacing, block,
                    ).await?
                };
                amount = raw_to_amount(raw_out, token_out.decimals);
            }
        }
        if !amount.is_finite() || amount <= 0.0 { bail!("non-positive exact route output"); }
    }
    Ok(amount)
}

fn enumerate_cycles(
    chain: &str,
    states: &[PoolState],
    tokens: &HashMap<String, TokenConfig>,
    usd_prices: &HashMap<String, f64>,
    financeable_starts: &HashSet<String>,
    flash_hurdle_bps: f64,
    max_per_depth: usize,
) -> (Vec<CycleCandidate>, bool, HashMap<usize, usize>, u64) {
    let mut adjacency: HashMap<String, Vec<(usize, String)>> = HashMap::new();
    let mut best_normalized_step = 1.0f64;
    for (idx, state) in states.iter().enumerate() {
        adjacency.entry(state.token0().to_ascii_lowercase()).or_default().push((idx, state.token1().to_string()));
        adjacency.entry(state.token1().to_ascii_lowercase()).or_default().push((idx, state.token0().to_string()));
        for (token_in, token_out) in [(state.token0(), state.token1()), (state.token1(), state.token0())] {
            if let Some(f) = normalized_edge_factor(state, token_in, token_out, usd_prices) {
                if f.is_finite() && f > best_normalized_step { best_normalized_step = f; }
            }
        }
    }

    // Any token with an inferred USD anchor can be a cycle start. Prefer explicit
    // USD anchors, then wrapped native, then inferred alts so equivalent rotations
    // deterministically keep the most financing-friendly start when possible.
    let mut starts = tokens.values()
        .filter(|t| financeable_starts.contains(&t.address.to_ascii_lowercase()))
        .filter(|t| usd_prices.get(&t.address.to_ascii_lowercase()).copied().map_or(false, |p| p.is_finite() && p > 0.0))
        .cloned().collect::<Vec<_>>();
    starts.sort_by(|a, b| {
        start_token_rank(a).cmp(&start_token_rank(b))
            .then_with(|| a.symbol.cmp(&b.symbol))
            .then_with(|| a.address.to_ascii_lowercase().cmp(&b.address.to_ascii_lowercase()))
    });
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    let mut generated: HashMap<usize, usize> = HashMap::new();
    let mut exhausted = false;
    let mut pruned_branches = 0u64;
    let hurdle_ratio = 1.0 + flash_hurdle_bps / 10_000.0;

    for start in starts {
        let start_key = start.address.to_ascii_lowercase();
        let mut used_pools = HashSet::new();
        let mut visited_tokens = HashSet::new();
        visited_tokens.insert(start_key.clone());
        let mut path = Vec::<CycleEdge>::new();
        dfs_cycles(
            chain,
            states,
            tokens,
            usd_prices,
            &adjacency,
            &start,
            &start_key,
            &start.address,
            &mut used_pools,
            &mut visited_tokens,
            &mut path,
            1.0,
            best_normalized_step.max(1.0),
            hurdle_ratio,
            &mut seen,
            &mut generated,
            max_per_depth,
            &mut exhausted,
            &mut pruned_branches,
            &mut out,
        );
    }
    (out, exhausted, generated, pruned_branches)
}

fn normalized_edge_factor(
    state: &PoolState,
    token_in: &str,
    token_out: &str,
    usd_prices: &HashMap<String, f64>,
) -> Option<f64> {
    let rate = state.marginal_rate(token_in, token_out)?;
    let p_in = usd_prices.get(&token_in.to_ascii_lowercase()).copied()?;
    let p_out = usd_prices.get(&token_out.to_ascii_lowercase()).copied()?;
    if p_in <= 0.0 || p_out <= 0.0 { return None; }
    let factor = rate * p_out / p_in;
    if factor.is_finite() && factor > 0.0 { Some(factor) } else { None }
}

#[allow(clippy::too_many_arguments)]
fn dfs_cycles(
    chain: &str,
    states: &[PoolState],
    tokens: &HashMap<String, TokenConfig>,
    usd_prices: &HashMap<String, f64>,
    adjacency: &HashMap<String, Vec<(usize, String)>>,
    start: &TokenConfig,
    start_key: &str,
    current: &str,
    used_pools: &mut HashSet<usize>,
    visited_tokens: &mut HashSet<String>,
    path: &mut Vec<CycleEdge>,
    partial_normalized_factor: f64,
    optimistic_step_factor: f64,
    hurdle_ratio: f64,
    seen: &mut HashSet<String>,
    generated: &mut HashMap<usize, usize>,
    max_per_depth: usize,
    exhausted: &mut bool,
    pruned_branches: &mut u64,
    out: &mut Vec<CycleCandidate>,
) {
    if path.len() >= 4 { return; }
    let current_key = current.to_ascii_lowercase();
    let Some(next_edges) = adjacency.get(&current_key) else { return; };
    for (pool_idx, token_out) in next_edges {
        if used_pools.contains(pool_idx) { continue; }
        let state = &states[*pool_idx];
        let Some(edge_factor) = normalized_edge_factor(state, current, token_out, usd_prices) else { continue; };
        let next_partial = partial_normalized_factor * edge_factor;
        let out_key = token_out.to_ascii_lowercase();
        let closes = out_key == start_key;
        let next_depth = path.len() + 1;
        if closes && next_depth >= 2 {
            if *generated.get(&next_depth).unwrap_or(&0) >= max_per_depth {
                *exhausted = true;
                continue;
            }
            let mut candidate_edges = path.clone();
            candidate_edges.push(CycleEdge { pool_idx: *pool_idx, token_in: current.to_string(), token_out: token_out.clone() });
            let dedupe = canonical_directed_cycle_key(chain, &candidate_edges, states);
            if !seen.insert(dedupe.clone()) { continue; }
            if !next_partial.is_finite() || next_partial <= 0.0 { continue; }
            *generated.entry(next_depth).or_insert(0) += 1;
            let venues = candidate_edges.iter().map(|edge| states[edge.pool_idx].def().dex.name.clone()).collect::<Vec<_>>();
            let mut token_path = vec![start.symbol.clone()];
            for edge in &candidate_edges {
                token_path.push(tokens.get(&edge.token_out.to_ascii_lowercase()).map(|t| t.symbol.clone()).unwrap_or_else(|| edge.token_out.clone()));
            }
            // USD normalization telescopes around a closed cycle, so this equals the
            // ordinary token-unit cycle return while being numerically well-conditioned.
            out.push(CycleCandidate {
                edges: candidate_edges,
                start_addr: start.address.clone(),
                start_symbol: start.symbol.clone(),
                token_path,
                spot_edge_bps: (next_partial - 1.0) * 10_000.0,
                venue_key: venues.join("->"),
                opportunity_id: stable_id(&dedupe),
            });
            continue;
        }
        if closes || next_depth >= 4 || visited_tokens.contains(&out_key) { continue; }

        // Safe branch-and-bound: every future edge is bounded by the largest observed
        // USD-normalized marginal edge in the current snapshot. If even filling all
        // remaining slots with that best edge cannot beat financing, this partial path
        // cannot produce a financing-plausible <=4-hop cycle and is skipped before
        // combinatorial expansion. The >=1 clamp makes the bound deliberately optimistic.
        let remaining_edges = 4usize.saturating_sub(next_depth);
        let optimistic_ceiling = next_partial * optimistic_step_factor.powi(remaining_edges as i32);
        if optimistic_ceiling <= hurdle_ratio {
            *pruned_branches = pruned_branches.saturating_add(1);
            continue;
        }

        used_pools.insert(*pool_idx);
        visited_tokens.insert(out_key.clone());
        path.push(CycleEdge { pool_idx: *pool_idx, token_in: current.to_string(), token_out: token_out.clone() });
        dfs_cycles(
            chain, states, tokens, usd_prices, adjacency, start, start_key, token_out,
            used_pools, visited_tokens, path, next_partial, optimistic_step_factor,
            hurdle_ratio, seen, generated, max_per_depth, exhausted, pruned_branches, out,
        );
        path.pop();
        visited_tokens.remove(&out_key);
        used_pools.remove(pool_idx);
    }
}

fn start_token_rank(token: &TokenConfig) -> u8 {
    if token.usd_price.is_some() { 0 }
    else if token.wrapped_native { 1 }
    else { 2 }
}

fn canonical_directed_cycle_key(chain: &str, edges: &[CycleEdge], states: &[PoolState]) -> String {
    if edges.is_empty() { return chain.to_ascii_lowercase(); }
    let labels = edges.iter().map(|edge| {
        format!(
            "{}>{}>{}",
            states[edge.pool_idx].pool_address().to_ascii_lowercase(),
            edge.token_in.to_ascii_lowercase(),
            edge.token_out.to_ascii_lowercase(),
        )
    }).collect::<Vec<_>>();
    let mut best: Option<String> = None;
    for offset in 0..labels.len() {
        let rotated = (0..labels.len())
            .map(|i| labels[(offset + i) % labels.len()].as_str())
            .collect::<Vec<_>>()
            .join("|");
        if best.as_ref().map_or(true, |current| rotated < *current) {
            best = Some(rotated);
        }
    }
    format!("{}|{}", chain.to_ascii_lowercase(), best.unwrap_or_default())
}

fn derive_native_usd(states: &[PoolState], tokens: &HashMap<String, TokenConfig>) -> Option<f64> {
    let native = tokens.values().find(|t| t.wrapped_native)?;
    let mut samples = Vec::new();
    for state in states {
        let other_addr = if eq_addr(state.token0(), &native.address) {
            Some(state.token1())
        } else if eq_addr(state.token1(), &native.address) {
            Some(state.token0())
        } else { None };
        let Some(other_addr) = other_addr else { continue; };
        let Some(other) = tokens.get(&other_addr.to_ascii_lowercase()) else { continue; };
        let Some(other_usd) = other.usd_price else { continue; };
        if other_usd <= 0.0 { continue; }
        if let Some(rate_after_fee) = state.marginal_rate(&native.address, other_addr) {
            let fee_mult = 1.0 - state.fee_bps() / 10_000.0;
            if fee_mult > 0.0 {
                let p = rate_after_fee / fee_mult * other_usd;
                if p.is_finite() && p > 10.0 && p < 100_000.0 { samples.push(p); }
            }
        }
    }
    if samples.is_empty() { return None; }
    samples.sort_by(|a, b| a.total_cmp(b));
    Some(samples[samples.len() / 2])
}

fn infer_token_usd_prices(
    states: &[PoolState],
    tokens: &HashMap<String, TokenConfig>,
    native_usd: Option<f64>,
) -> HashMap<String, f64> {
    let mut prices = HashMap::<String, f64>::new();
    for token in tokens.values() {
        if let Some(price) = token_usd(token, native_usd) {
            if price.is_finite() && price > 0.0 {
                prices.insert(token.address.to_ascii_lowercase(), price);
            }
        }
    }

    // Bounded propagation is enough because V3.4 searches paths no deeper than four legs.
    // Use medians across all available venue observations to reduce dependence on one pool.
    for _ in 0..4 {
        let mut samples = HashMap::<String, Vec<f64>>::new();
        for state in states {
            let k0 = state.token0().to_ascii_lowercase();
            let k1 = state.token1().to_ascii_lowercase();
            let p0 = prices.get(&k0).copied();
            let p1 = prices.get(&k1).copied();
            if p0.is_some() == p1.is_some() { continue; }

            let fee_mult = 1.0 - state.fee_bps() / 10_000.0;
            if !fee_mult.is_finite() || fee_mult <= 0.0 { continue; }
            let Some(rate_after_fee) = state.marginal_rate(state.token0(), state.token1()) else { continue; };
            let units1_per_0 = rate_after_fee / fee_mult;
            if !units1_per_0.is_finite() || units1_per_0 <= 0.0 { continue; }

            let (unknown, candidate) = match (p0, p1) {
                (Some(px0), None) => (k1, px0 / units1_per_0),
                (None, Some(px1)) => (k0, px1 * units1_per_0),
                _ => continue,
            };
            if candidate.is_finite() && candidate > 1e-10 && candidate < 1e9 {
                samples.entry(unknown).or_default().push(candidate);
            }
        }

        let mut added = 0usize;
        for (token, mut values) in samples {
            if prices.contains_key(&token) || values.is_empty() { continue; }
            values.sort_by(|a, b| a.total_cmp(b));
            let median = values[values.len() / 2];
            if median.is_finite() && median > 0.0 {
                prices.insert(token, median);
                added += 1;
            }
        }
        if added == 0 { break; }
    }
    prices
}

fn filter_states_by_anchor_liquidity(
    states: Vec<PoolState>,
    usd_prices: &HashMap<String, f64>,
    min_anchor_usd: f64,
) -> Vec<PoolState> {
    if min_anchor_usd <= 0.0 { return states; }
    states.into_iter().filter(|state| {
        pool_anchor_liquidity_usd(state, usd_prices)
            .map(|v| v >= min_anchor_usd)
            .unwrap_or(false)
    }).collect()
}

fn pool_anchor_liquidity_usd(
    state: &PoolState,
    usd_prices: &HashMap<String, f64>,
) -> Option<f64> {
    let p0 = usd_prices.get(&state.token0().to_ascii_lowercase()).copied();
    let p1 = usd_prices.get(&state.token1().to_ascii_lowercase()).copied();
    let (amount0, amount1) = match state {
        PoolState::V2(v2) => (v2.reserve0, v2.reserve1),
        PoolState::V3(v3) => {
            if v3.sqrt_price_x96 <= 0.0 || v3.liquidity <= 0.0 { return None; }
            let sqrt = v3.sqrt_price_x96 / 2f64.powi(96);
            if !sqrt.is_finite() || sqrt <= 0.0 { return None; }
            (
                v3.liquidity / sqrt / 10f64.powi(v3.def.token0_decimals as i32),
                v3.liquidity * sqrt / 10f64.powi(v3.def.token1_decimals as i32),
            )
        }
    };
    let v0 = p0.map(|p| amount0 * p).filter(|v| v.is_finite() && *v >= 0.0);
    let v1 = p1.map(|p| amount1 * p).filter(|v| v.is_finite() && *v >= 0.0);
    match (v0, v1) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

fn token_usd(token: &TokenConfig, native_usd: Option<f64>) -> Option<f64> {
    token.usd_price.or_else(|| token.wrapped_native.then_some(native_usd).flatten())
}

fn build_candidate_record(
    chain: &BreadthChainConfig,
    states: &[PoolState],
    cycle: &CycleCandidate,
    block: u64,
    observed_at: &str,
    e: &ExactEvaluation,
) -> CandidateRecord {
    CandidateRecord {
        opportunity_id: cycle.opportunity_id.clone(),
        chain: chain.name.clone(),
        block_number: block,
        first_seen: observed_at.to_string(),
        last_seen: observed_at.to_string(),
        lifetime_ms: 0,
        observations: 1,
        route_depth: cycle.edges.len(),
        start_token: cycle.start_symbol.clone(),
        token_path: cycle.token_path.clone(),
        venues: cycle.edges.iter().map(|edge| states[edge.pool_idx].def().dex.name.clone()).collect(),
        pools: cycle.edges.iter().map(|edge| states[edge.pool_idx].pool_address().to_string()).collect(),
        spot_edge_bps: cycle.spot_edge_bps,
        input_usd: e.input_usd,
        input_amount: e.input_amount,
        gross_profit_usd: e.gross_profit_usd,
        flash_fee_usd: e.flash_fee_usd,
        after_flash_usd: e.after_flash_usd,
        funding_mode: e.funding_mode.to_string(),
        financing_fee_usd: e.financing_fee_usd,
        profit_after_financing_usd: e.profit_after_financing_usd,
        mev_bid_reserve_usd: e.mev_bid_reserve_usd,
        estimated_execution_fee_usd: e.estimated_execution_fee_usd,
        estimated_net_usd: e.estimated_net_usd,
        fee_model_quality: chain.fee_model.quality.clone(),
    }
}

async fn update_lifetime(
    lifetimes: &Arc<Mutex<HashMap<String, CandidateRecord>>>,
    mut candidate: CandidateRecord,
) -> CandidateRecord {
    let mut map = lifetimes.lock().await;
    if let Some(existing) = map.get(&candidate.opportunity_id).cloned() {
        candidate.first_seen = existing.first_seen.clone();
        candidate.observations = existing.observations.saturating_add(1);
        candidate.lifetime_ms = parse_rfc3339_millis(&candidate.last_seen)
            .saturating_sub(parse_rfc3339_millis(&candidate.first_seen));
        if candidate_score(&existing) > candidate_score(&candidate) {
            let mut keep = existing;
            keep.last_seen = candidate.last_seen.clone();
            keep.observations = candidate.observations;
            keep.lifetime_ms = candidate.lifetime_ms;
            map.insert(keep.opportunity_id.clone(), keep.clone());
            return keep;
        }
    }
    map.insert(candidate.opportunity_id.clone(), candidate.clone());
    candidate
}

fn candidate_score(c: &CandidateRecord) -> f64 {
    c.estimated_net_usd.unwrap_or(c.profit_after_financing_usd)
}

fn parse_rfc3339_millis(s: &str) -> u64 {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .and_then(|dt| u64::try_from(dt.timestamp_millis()).ok())
        .unwrap_or(0)
}

async fn log_chain_summary(report: &Arc<Mutex<ChainReport>>) {
    let r = report.lock().await.clone();
    let d2 = r.by_depth.get("2hop").cloned().unwrap_or_default();
    let d3 = r.by_depth.get("3hop").cloned().unwrap_or_default();
    let d4 = r.by_depth.get("4hop").cloned().unwrap_or_default();
    let marginal_pos = d2.marginal_positive + d3.marginal_positive + d4.marginal_positive;
    let above_flash = d2.marginal_above_flash + d3.marginal_above_flash + d4.marginal_above_flash;
    let finite_after_flash = d2.finite_after_flash_positive + d3.finite_after_flash_positive + d4.finite_after_flash_positive;
    let finite_after_financing = d2.finite_after_financing_positive + d3.finite_after_financing_positive + d4.finite_after_financing_positive;
    let unique_net = d2.unique_net_positive + d3.unique_net_positive + d4.unique_net_positive;
    info!(
        chain = %r.chain,
        scans = r.scans,
        hot = r.hot_pools,
        routes2 = d2.cycles_generated,
        routes3 = d3.cycles_generated,
        routes4 = d4.cycles_generated,
        marginal_pos,
        above_flash,
        finite_after_flash,
        finite_after_financing,
        unique_net_positive = unique_net,
        best_edge_bps = ?r.best_candidate.as_ref().map(|c| format!("{:.3}", c.spot_edge_bps)),
        best_net = ?r.best_candidate.as_ref().and_then(|c| c.estimated_net_usd).map(|v| format!("{v:.6}")),
        rpc_failures = r.rpc_health.failures,
        rpc_rate_limits = r.rpc_health.rate_limits,
        last_scan_ms = r.last_scan_ms,
        partial_snapshots = r.partial_snapshots,
        empty_or_uninitialized = r.empty_or_uninitialized_pools,
        upper_bound_pruned = r.upper_bound_pruned_branches,
        "BREADTH_SUMMARY"
    );
}

async fn load_or_discover_pools(
    chain: &BreadthChainConfig,
    rpc: &RpcClient,
    multicall3: &str,
    multicall_max_calls: usize,
) -> Result<DiscoveredPoolUniverse> {
    if let Some(universe) = load_pool_cache(chain)? {
        info!(
            chain = %chain.name,
            pools = universe.pools.len(),
            tokens = universe.tokens.len(),
            cache = %chain.pool_cache_path,
            "V3.4 pool/token universe cache hit"
        );
        return Ok(universe);
    }
    let universe = discover_pools(chain, rpc, multicall3, multicall_max_calls).await?;
    save_pool_cache(chain, &universe)?;
    info!(
        chain = %chain.name,
        pools = universe.pools.len(),
        tokens = universe.tokens.len(),
        cache = %chain.pool_cache_path,
        "V3.4 pool/token universe discovery complete"
    );
    Ok(universe)
}

async fn discover_pools(
    chain: &BreadthChainConfig,
    rpc: &RpcClient,
    multicall3: &str,
    multicall_max_calls: usize,
) -> Result<DiscoveredPoolUniverse> {
    let pending_path = pending_metadata_path(chain);
    let expanded = expand_from_factory_events(
        &chain.name,
        &chain.tokens,
        &chain.dexes,
        rpc,
        &chain.universe_expansion,
        chain.discovery_concurrency,
        multicall3,
        multicall_max_calls,
        Some(&pending_path),
    ).await?;

    let token_map = expanded.tokens.iter().cloned()
        .map(|t| (t.address.to_ascii_lowercase(), t))
        .collect::<HashMap<_, _>>();

    let mut pair_tokens = Vec::<(TokenConfig, TokenConfig)>::new();
    for (a, b) in &expanded.pair_keys {
        let Some(token_a) = token_map.get(&a.to_ascii_lowercase()).cloned() else { continue; };
        let Some(token_b) = token_map.get(&b.to_ascii_lowercase()).cloned() else { continue; };
        pair_tokens.push((token_a, token_b));
    }

    let mut probes = Vec::new();
    for dex in &chain.dexes {
        let slipstream_spacings = if dex.kind == DexKind::Slipstream {
            match rpc.slipstream_tick_spacings(&dex.factory).await {
                Ok(spacings) if !spacings.is_empty() => Some(spacings),
                Ok(_) => {
                    warn!(chain = %chain.name, dex = %dex.name, "V3.4 Slipstream factory returned zero tick spacings; adapter skipped");
                    None
                }
                Err(err) => {
                    warn!(chain = %chain.name, dex = %dex.name, error = %err, "V3.4 Slipstream tick-spacing discovery failed; venue skipped while chain continues");
                    None
                }
            }
        } else { None };

        for (a, b) in &pair_tokens {
            match dex.kind {
                DexKind::V2 => probes.push(DiscoveryProbe::V2 { dex: dex.clone(), token_a: a.clone(), token_b: b.clone() }),
                DexKind::V3 => {
                    for fee_tier in &dex.fee_tiers {
                        probes.push(DiscoveryProbe::V3 { dex: dex.clone(), token_a: a.clone(), token_b: b.clone(), fee_tier: *fee_tier });
                    }
                }
                DexKind::Algebra => probes.push(DiscoveryProbe::Algebra { dex: dex.clone(), token_a: a.clone(), token_b: b.clone() }),
                DexKind::Solidly => probes.push(DiscoveryProbe::Solidly { dex: dex.clone(), token_a: a.clone(), token_b: b.clone() }),
                DexKind::Slipstream => {
                    if let Some(spacings) = &slipstream_spacings {
                        for tick_spacing in spacings {
                            probes.push(DiscoveryProbe::Slipstream {
                                dex: dex.clone(), token_a: a.clone(), token_b: b.clone(), tick_spacing: *tick_spacing,
                            });
                        }
                    }
                }
            }
        }
    }
    let probe_count = probes.len();
    let block = rpc.block_number().await?;
    let calls = probes
        .iter()
        .map(encode_discovery_probe_call)
        .collect::<Result<Vec<_>>>()?;
    let rows = rpc
        .multicall_read_many_at(multicall3, &calls, block, multicall_max_calls)
        .await
        .context("V3.4.2 batched cross-venue factory probing failed")?;
    if rows.len() != probes.len() {
        bail!("V3.4.2 discovery result count mismatch: probes={} results={}", probes.len(), rows.len());
    }

    // Event-discovered/enumerated pools are authoritative additions. Batched
    // cross-venue probes fill the same expanded pair graph on every adapter without
    // issuing one JSON-RPC request per pair/fee-tier.
    let mut pools = expanded.event_pools;
    let mut seen = HashSet::new();
    for pool in &pools {
        seen.insert(format!("{}:{}", pool.dex.name.to_ascii_lowercase(), pool.pool_address.to_ascii_lowercase()));
    }
    let mut failures = 0usize;
    for (probe, row) in probes.into_iter().zip(rows.into_iter()) {
        let Some(raw) = row else {
            failures = failures.saturating_add(1);
            continue;
        };
        match discovery_hit_from_multicall(probe, &raw) {
            Ok(Some(hit)) => {
                let key = format!("{}:{}", hit.dex.name.to_ascii_lowercase(), hit.pool_address.to_ascii_lowercase());
                if seen.insert(key) {
                    pools.push(make_definition(hit, &token_map)?);
                }
            }
            Ok(None) => {}
            Err(_) => failures = failures.saturating_add(1),
        }
    }
    info!(
        chain = %chain.name,
        pairs = pair_tokens.len(),
        probes = probe_count,
        rpc_multicall_batches = (probe_count + multicall_max_calls.max(1) - 1) / multicall_max_calls.max(1),
        discovered = pools.len(),
        failed_probes = failures,
        auto_tokens = expanded.stats.auto_tokens_added,
        event_pools = expanded.stats.event_pools_retained,
        "V3.4.2 batched expanded pair-graph factory discovery"
    );
    Ok(DiscoveredPoolUniverse {
        pools,
        tokens: token_map,
        expansion_stats: expanded.stats,
    })
}

fn encode_discovery_probe_call(probe: &DiscoveryProbe) -> Result<(String, String)> {
    let (dex, suffix) = match probe {
        DiscoveryProbe::V2 { dex, token_a, token_b } => {
            let (token0, token1) = canonical_token_addresses(&token_a.address, &token_b.address);
            let selector = if dex.syncswap_classic { function_selector("getPool(address,address)") } else { "e6a43905".to_string() };
            (dex, format!("{selector}{}{}", encode_address_word_local(&token0)?, encode_address_word_local(&token1)?))
        }
        DiscoveryProbe::V3 { dex, token_a, token_b, fee_tier } => {
            let (token0, token1) = canonical_token_addresses(&token_a.address, &token_b.address);
            (dex, format!("1698ee82{}{}{:064x}", encode_address_word_local(&token0)?, encode_address_word_local(&token1)?, *fee_tier))
        }
        DiscoveryProbe::Algebra { dex, token_a, token_b } => {
            let (token0, token1) = canonical_token_addresses(&token_a.address, &token_b.address);
            let selector = function_selector("poolByPair(address,address)");
            (dex, format!("{selector}{}{}", encode_address_word_local(&token0)?, encode_address_word_local(&token1)?))
        }
        DiscoveryProbe::Solidly { dex, token_a, token_b } => {
            let (token0, token1) = canonical_token_addresses(&token_a.address, &token_b.address);
            let selector = function_selector("getPool(address,address,bool)");
            let stable = if dex.solidly_stable { 1u8 } else { 0u8 };
            (dex, format!("{selector}{}{}{:064x}", encode_address_word_local(&token0)?, encode_address_word_local(&token1)?, stable))
        }
        DiscoveryProbe::Slipstream { dex, token_a, token_b, tick_spacing } => {
            if *tick_spacing <= 0 { bail!("Slipstream tick spacing must be positive"); }
            let (token0, token1) = canonical_token_addresses(&token_a.address, &token_b.address);
            let selector = function_selector("getPool(address,address,int24)");
            (dex, format!("{selector}{}{}{:064x}", encode_address_word_local(&token0)?, encode_address_word_local(&token1)?, *tick_spacing as u32))
        }
    };
    Ok((dex.factory.clone(), format!("0x{suffix}")))
}

fn discovery_hit_from_multicall(probe: DiscoveryProbe, raw: &str) -> Result<Option<DiscoveryHit>> {
    let pool = decode_address_word_local(raw)?;
    if is_zero_address(&pool) { return Ok(None); }
    let hit = match probe {
        DiscoveryProbe::V2 { dex, token_a, token_b } => {
            let (token0, token1) = canonical_token_addresses(&token_a.address, &token_b.address);
            DiscoveryHit { dex, pool_address: pool, token0, token1, fee_tier: None, tick_spacing: None }
        }
        DiscoveryProbe::V3 { dex, token_a, token_b, fee_tier } => {
            let (token0, token1) = canonical_token_addresses(&token_a.address, &token_b.address);
            DiscoveryHit { dex, pool_address: pool, token0, token1, fee_tier: Some(fee_tier), tick_spacing: None }
        }
        DiscoveryProbe::Algebra { dex, token_a, token_b } => {
            let (token0, token1) = canonical_token_addresses(&token_a.address, &token_b.address);
            DiscoveryHit { dex, pool_address: pool, token0, token1, fee_tier: None, tick_spacing: None }
        }
        DiscoveryProbe::Solidly { dex, token_a, token_b } => {
            let (token0, token1) = canonical_token_addresses(&token_a.address, &token_b.address);
            DiscoveryHit { dex, pool_address: pool, token0, token1, fee_tier: None, tick_spacing: None }
        }
        DiscoveryProbe::Slipstream { dex, token_a, token_b, tick_spacing } => {
            let (token0, token1) = canonical_token_addresses(&token_a.address, &token_b.address);
            DiscoveryHit { dex, pool_address: pool, token0, token1, fee_tier: None, tick_spacing: Some(tick_spacing) }
        }
    };
    Ok(Some(hit))
}

fn encode_address_word_local(address: &str) -> Result<String> {
    validate_address(address)?;
    Ok(format!("{:0>64}", address.trim_start_matches("0x").to_ascii_lowercase()))
}

fn decode_address_word_local(raw: &str) -> Result<String> {
    let raw = raw.trim_start_matches("0x");
    if raw.len() < 64 || !raw[..64].chars().all(|c| c.is_ascii_hexdigit()) {
        bail!("invalid ABI address return word");
    }
    Ok(format!("0x{}", &raw[24..64]))
}

fn function_selector(signature: &str) -> String {
    let mut hasher = Keccak::v256();
    let mut out = [0u8; 32];
    hasher.update(signature.as_bytes());
    hasher.finalize(&mut out);
    out[..4].iter().map(|b| format!("{b:02x}")).collect::<String>()
}

fn make_definition(hit: DiscoveryHit, tokens: &HashMap<String, TokenConfig>) -> Result<PoolDefinition> {
    let t0 = tokens.get(&hit.token0.to_ascii_lowercase()).ok_or_else(|| anyhow!("unapproved token0"))?;
    let t1 = tokens.get(&hit.token1.to_ascii_lowercase()).ok_or_else(|| anyhow!("unapproved token1"))?;
    Ok(PoolDefinition {
        dex: hit.dex,
        pool_address: hit.pool_address,
        token0: hit.token0,
        token1: hit.token1,
        token0_decimals: t0.decimals,
        token1_decimals: t1.decimals,
        fee_tier: hit.fee_tier,
        tick_spacing: hit.tick_spacing,
    })
}

fn pending_metadata_path(chain: &BreadthChainConfig) -> String {
    Path::new(&chain.pool_cache_path)
        .with_extension("pending_metadata.json")
        .to_string_lossy()
        .to_string()
}

fn load_pool_cache(chain: &BreadthChainConfig) -> Result<Option<DiscoveredPoolUniverse>> {
    let path = Path::new(&chain.pool_cache_path);
    if !path.exists() { return Ok(None); }
    let raw = fs::read_to_string(path)?;

    // Read only the lightweight JSON envelope first. Older cache schemas may not
    // contain fields required by the current PoolCache struct, so attempting a full
    // deserialize before checking `version` would incorrectly kill the chain worker.
    // A stale/incompatible cache must behave exactly like a cache miss and be rebuilt.
    let envelope: serde_json::Value = match serde_json::from_str(&raw) {
        Ok(value) => value,
        Err(err) => {
            warn!(
                chain = %chain.name,
                path = %chain.pool_cache_path,
                error = %err,
                "V3.4.2 unreadable pool cache; treating as stale and rebuilding"
            );
            return Ok(None);
        }
    };
    let cache_version = envelope.get("version").and_then(|v| v.as_u64()).unwrap_or(0);
    if cache_version != 6 {
        info!(
            chain = %chain.name,
            path = %chain.pool_cache_path,
            cache_version,
            expected_version = 6u64,
            "V3.4.2 stale pool cache schema; rebuilding"
        );
        return Ok(None);
    }

    let cache: PoolCache = match serde_json::from_value(envelope) {
        Ok(cache) => cache,
        Err(err) => {
            warn!(
                chain = %chain.name,
                path = %chain.pool_cache_path,
                error = %err,
                "V3.4.2 incompatible pool cache; treating as stale and rebuilding"
            );
            return Ok(None);
        }
    };
    if cache.universe_key != pool_universe_key(chain) { return Ok(None); }
    let ttl = chain.pool_cache_ttl_hours.saturating_mul(3600);
    if unix_seconds().saturating_sub(cache.generated_unix) > ttl { return Ok(None); }
    if cache.tokens.is_empty() { return Ok(None); }
    let tokens = cache.tokens.into_iter()
        .map(|t| (t.address.to_ascii_lowercase(), t))
        .collect::<HashMap<_, _>>();
    let mut out = Vec::new();
    for entry in cache.pools {
        let mut dex = chain.dexes.iter().find(|d| d.name.eq_ignore_ascii_case(&entry.dex_name)).cloned()
            .ok_or_else(|| anyhow!("cached DEX {} no longer configured", entry.dex_name))?;
        if dex.kind == DexKind::Solidly {
            dex.fee_bps = entry.fee_bps.or(dex.fee_bps);
        }
        out.push(make_definition(DiscoveryHit {
            dex,
            pool_address: entry.pool_address,
            token0: entry.token0,
            token1: entry.token1,
            fee_tier: entry.fee_tier,
            tick_spacing: entry.tick_spacing,
        }, &tokens)?);
    }
    if out.is_empty() {
        Ok(None)
    } else {
        Ok(Some(DiscoveredPoolUniverse {
            pools: out,
            tokens,
            expansion_stats: cache.expansion_stats,
        }))
    }
}

fn save_pool_cache(chain: &BreadthChainConfig, universe: &DiscoveredPoolUniverse) -> Result<()> {
    let path = Path::new(&chain.pool_cache_path);
    if let Some(parent) = path.parent() { if !parent.as_os_str().is_empty() { fs::create_dir_all(parent)?; } }
    let mut tokens = universe.tokens.values().cloned().collect::<Vec<_>>();
    tokens.sort_by(|a, b| a.address.to_ascii_lowercase().cmp(&b.address.to_ascii_lowercase()));
    let cache = PoolCache {
        version: 6,
        universe_key: pool_universe_key(chain),
        generated_unix: unix_seconds(),
        tokens,
        expansion_stats: universe.expansion_stats.clone(),
        pools: universe.pools.iter().map(|p| PoolCacheEntry {
            dex_name: p.dex.name.clone(),
            pool_address: p.pool_address.clone(),
            token0: p.token0.clone(),
            token1: p.token1.clone(),
            fee_tier: p.fee_tier,
            tick_spacing: p.tick_spacing,
            fee_bps: p.dex.fee_bps,
        }).collect(),
    };
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, serde_json::to_string_pretty(&cache)?)?;
    if path.exists() { fs::remove_file(path)?; }
    fs::rename(tmp, path)?;
    Ok(())
}

fn pool_universe_key(chain: &BreadthChainConfig) -> String {
    let mut parts = vec![format!("chain:{}:{}", chain.name, chain.chain_id)];
    for token in &chain.tokens {
        parts.push(format!("t:{}:{}:{}", token.symbol, token.address.to_ascii_lowercase(), token.decimals));
    }
    for dex in &chain.dexes {
        let mut tiers = dex.fee_tiers.clone(); tiers.sort_unstable();
        let mut dex_key = format!("d:{}:{:?}:{}:{:?}:{:?}:{:?}:stable={}:slipmask={:?}", dex.name, dex.kind, dex.factory.to_ascii_lowercase(), dex.fee_bps, dex.quoter_v2.as_ref().map(|q| q.to_ascii_lowercase()), tiers, dex.solidly_stable, dex.slipstream_factory_mask);
        if dex.syncswap_classic { dex_key.push_str(":syncswap=true"); }
        parts.push(dex_key);
    }
    let ux = &chain.universe_expansion;
    parts.push(format!(
        "ux:{}:{}:{}:{}:{}:{}:{}:{}",
        ux.enabled,
        ux.lookback_blocks,
        ux.log_chunk_blocks,
        ux.min_log_chunk_blocks,
        ux.frontier_rounds,
        ux.max_auto_tokens,
        ux.max_event_pairs,
        ux.max_logs_per_factory,
    ));
    if !ux.auto_token_symbol_deny_prefixes.is_empty() {
        let mut denied = ux.auto_token_symbol_deny_prefixes.clone();
        denied.sort();
        parts.push(format!("ux_deny:{}", denied.join(",").to_ascii_lowercase()));
    }
    parts.join("|")
}

fn init_opportunity_db(path: &str) -> Result<Connection> {
    let path = Path::new(path);
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() { fs::create_dir_all(parent)?; }
    }
    let conn = Connection::open(path)?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;\n\
         PRAGMA synchronous=NORMAL;\n\
         CREATE TABLE IF NOT EXISTS opportunities (\n\
           opportunity_id TEXT PRIMARY KEY,\n\
           chain TEXT NOT NULL,\n\
           route_depth INTEGER NOT NULL,\n\
           start_token TEXT NOT NULL,\n\
           token_path_json TEXT NOT NULL,\n\
           venues_json TEXT NOT NULL,\n\
           pools_json TEXT NOT NULL,\n\
           first_seen TEXT NOT NULL,\n\
           last_seen TEXT NOT NULL,\n\
           lifetime_ms INTEGER NOT NULL DEFAULT 0,\n\
           observations INTEGER NOT NULL DEFAULT 1,\n\
           best_block INTEGER NOT NULL,\n\
           best_spot_edge_bps REAL NOT NULL,\n\
           best_input_usd REAL NOT NULL,\n\
           best_input_amount REAL NOT NULL,\n\
           best_gross_profit_usd REAL NOT NULL,\n\
           best_flash_fee_usd REAL NOT NULL,\n\
           best_after_flash_usd REAL NOT NULL,\n\
           best_mev_bid_reserve_usd REAL NOT NULL,\n\
           best_estimated_execution_fee_usd REAL,\n\
           best_estimated_net_usd REAL,\n\
           best_score_usd REAL NOT NULL,\n\
           fee_model_quality TEXT NOT NULL\n\
         );\n\
         CREATE INDEX IF NOT EXISTS idx_opportunities_chain_depth ON opportunities(chain, route_depth);\n\
         CREATE INDEX IF NOT EXISTS idx_opportunities_score ON opportunities(best_score_usd DESC);"
    )?;
    Ok(conn)
}

fn persist_candidate(conn: &mut Connection, record: &CandidateRecord) -> Result<()> {
    let token_path_json = serde_json::to_string(&record.token_path)?;
    let venues_json = serde_json::to_string(&record.venues)?;
    let pools_json = serde_json::to_string(&record.pools)?;
    let score = candidate_score(record);
    conn.execute(
        "INSERT INTO opportunities (\n\
           opportunity_id, chain, route_depth, start_token, token_path_json, venues_json, pools_json,\n\
           first_seen, last_seen, lifetime_ms, observations, best_block, best_spot_edge_bps,\n\
           best_input_usd, best_input_amount, best_gross_profit_usd, best_flash_fee_usd,\n\
           best_after_flash_usd, best_mev_bid_reserve_usd, best_estimated_execution_fee_usd,\n\
           best_estimated_net_usd, best_score_usd, fee_model_quality\n\
         ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,1,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22)\n\
         ON CONFLICT(opportunity_id) DO UPDATE SET\n\
           last_seen=excluded.last_seen,\n\
           lifetime_ms=CAST(MAX(0.0, (julianday(excluded.last_seen)-julianday(opportunities.first_seen))*86400000.0) AS INTEGER),\n\
           observations=opportunities.observations+1,\n\
           best_block=CASE WHEN excluded.best_score_usd > opportunities.best_score_usd THEN excluded.best_block ELSE opportunities.best_block END,\n\
           best_spot_edge_bps=MAX(opportunities.best_spot_edge_bps, excluded.best_spot_edge_bps),\n\
           best_input_usd=CASE WHEN excluded.best_score_usd > opportunities.best_score_usd THEN excluded.best_input_usd ELSE opportunities.best_input_usd END,\n\
           best_input_amount=CASE WHEN excluded.best_score_usd > opportunities.best_score_usd THEN excluded.best_input_amount ELSE opportunities.best_input_amount END,\n\
           best_gross_profit_usd=MAX(opportunities.best_gross_profit_usd, excluded.best_gross_profit_usd),\n\
           best_flash_fee_usd=CASE WHEN excluded.best_score_usd > opportunities.best_score_usd THEN excluded.best_flash_fee_usd ELSE opportunities.best_flash_fee_usd END,\n\
           best_after_flash_usd=MAX(opportunities.best_after_flash_usd, excluded.best_after_flash_usd),\n\
           best_mev_bid_reserve_usd=CASE WHEN excluded.best_score_usd > opportunities.best_score_usd THEN excluded.best_mev_bid_reserve_usd ELSE opportunities.best_mev_bid_reserve_usd END,\n\
           best_estimated_execution_fee_usd=CASE WHEN excluded.best_score_usd > opportunities.best_score_usd THEN excluded.best_estimated_execution_fee_usd ELSE opportunities.best_estimated_execution_fee_usd END,\n\
           best_estimated_net_usd=CASE WHEN excluded.best_score_usd > opportunities.best_score_usd THEN excluded.best_estimated_net_usd ELSE opportunities.best_estimated_net_usd END,\n\
           best_score_usd=MAX(opportunities.best_score_usd, excluded.best_score_usd),\n\
           fee_model_quality=CASE WHEN excluded.best_score_usd > opportunities.best_score_usd THEN excluded.fee_model_quality ELSE opportunities.fee_model_quality END",
        params![
            record.opportunity_id,
            record.chain,
            record.route_depth as i64,
            record.start_token,
            token_path_json,
            venues_json,
            pools_json,
            record.first_seen,
            record.last_seen,
            record.lifetime_ms as i64,
            record.block_number as i64,
            record.spot_edge_bps,
            record.input_usd,
            record.input_amount,
            record.gross_profit_usd,
            record.flash_fee_usd,
            record.after_flash_usd,
            record.mev_bid_reserve_usd,
            record.estimated_execution_fee_usd,
            record.estimated_net_usd,
            score,
            record.fee_model_quality,
        ],
    )?;
    Ok(())
}

fn update_best(slot: &mut Option<f64>, value: f64) {
    if !value.is_finite() { return; }
    if slot.map(|v| value > v).unwrap_or(true) { *slot = Some(value); }
}

fn amount_to_raw(amount: f64, decimals: u8) -> Result<u128> {
    if !amount.is_finite() || amount <= 0.0 { bail!("invalid amount"); }
    let scaled = amount * 10f64.powi(decimals as i32);
    if !scaled.is_finite() || scaled <= 0.0 || scaled > u128::MAX as f64 { bail!("amount out of raw range"); }
    Ok(scaled.floor() as u128)
}

fn raw_to_amount(raw: u128, decimals: u8) -> f64 {
    raw as f64 / 10f64.powi(decimals as i32)
}

fn canonical_token_addresses(a: &str, b: &str) -> (String, String) {
    if a.trim_start_matches("0x").to_ascii_lowercase() <= b.trim_start_matches("0x").to_ascii_lowercase() {
        (a.to_string(), b.to_string())
    } else {
        (b.to_string(), a.to_string())
    }
}

fn stable_id(material: &str) -> String {
    let mut hasher = Keccak::v256();
    let mut out = [0u8; 32];
    hasher.update(material.as_bytes());
    hasher.finalize(&mut out);
    let mut s = String::with_capacity(18);
    s.push_str("0x");
    for b in &out[..8] { s.push_str(&format!("{b:02x}")); }
    s
}

fn validate_address(address: &str) -> Result<()> {
    let s = address.trim_start_matches("0x");
    if s.len() != 40 || !s.chars().all(|c| c.is_ascii_hexdigit()) {
        bail!("invalid EVM address: {address}");
    }
    Ok(())
}

fn is_zero_address(address: &str) -> bool {
    address.trim_start_matches("0x").chars().all(|c| c == '0')
}

fn unix_seconds() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

fn write_json(path: impl AsRef<Path>, value: &impl Serialize) -> Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed creating output directory {}", parent.display()))?;
        }
    }
    let json = serde_json::to_string_pretty(value)?;
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, json).with_context(|| format!("failed writing {}", tmp.display()))?;
    fs::rename(&tmp, path).with_context(|| {
        format!("failed atomically replacing {} with {}", path.display(), tmp.display())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{V2PoolState};

    fn token(symbol: &str, address: &str, usd: Option<f64>) -> TokenConfig {
        TokenConfig { symbol: symbol.to_string(), address: address.to_string(), decimals: 18, usd_price: usd, wrapped_native: false }
    }

    fn v2(name: &str, address: &str, a: &TokenConfig, b: &TokenConfig, r0: f64, r1: f64) -> PoolState {
        PoolState::V2(V2PoolState {
            def: PoolDefinition {
                dex: DexConfig { name: name.to_string(), kind: DexKind::V2, factory: "0x0000000000000000000000000000000000000001".to_string(), fee_bps: Some(0.0), quoter_v2: None, tick_lens: None, fee_tiers: vec![], solidly_stable: false, slipstream_factory_mask: None, syncswap_classic: false },
                pool_address: address.to_string(), token0: a.address.clone(), token1: b.address.clone(), token0_decimals: 18, token1_decimals: 18, fee_tier: None, tick_spacing: None,
            },
            reserve0: r0,
            reserve1: r1,
        })
    }

    #[test]
    fn bounded_graph_finds_two_three_four_hop_cycles() {
        let a = token("A", "0x0000000000000000000000000000000000000010", Some(1.0));
        let b = token("B", "0x0000000000000000000000000000000000000020", None);
        let c = token("C", "0x0000000000000000000000000000000000000030", None);
        let d = token("D", "0x0000000000000000000000000000000000000040", None);
        let tokens = [a.clone(), b.clone(), c.clone(), d.clone()].into_iter().map(|t| (t.address.to_ascii_lowercase(), t)).collect::<HashMap<_, _>>();
        let states = vec![
            v2("x", "0x0000000000000000000000000000000000000101", &a, &b, 100.0, 101.0),
            v2("y", "0x0000000000000000000000000000000000000102", &a, &b, 100.0, 99.0),
            v2("z", "0x0000000000000000000000000000000000000103", &b, &c, 100.0, 100.0),
            v2("q", "0x0000000000000000000000000000000000000104", &c, &a, 100.0, 100.0),
            v2("r", "0x0000000000000000000000000000000000000105", &c, &d, 100.0, 100.0),
            v2("s", "0x0000000000000000000000000000000000000106", &d, &a, 100.0, 100.0),
        ];
        let prices = infer_token_usd_prices(&states, &tokens, None);
        let starts = tokens.keys().cloned().collect::<HashSet<_>>();
        let (_, _, generated, _) = enumerate_cycles("test", &states, &tokens, &prices, &starts, 0.0, 1000);
        assert!(generated.get(&2).copied().unwrap_or(0) > 0);
        assert!(generated.get(&3).copied().unwrap_or(0) > 0);
        assert!(generated.get(&4).copied().unwrap_or(0) > 0);
    }

    #[test]
    fn cycle_rotations_are_deduplicated() {
        let a = token("A", "0x0000000000000000000000000000000000000010", Some(1.0));
        let b = token("B", "0x0000000000000000000000000000000000000020", Some(2.0));
        let c = token("C", "0x0000000000000000000000000000000000000030", Some(3.0));
        let tokens = [a.clone(), b.clone(), c.clone()].into_iter()
            .map(|t| (t.address.to_ascii_lowercase(), t)).collect::<HashMap<_, _>>();
        let states = vec![
            v2("ab", "0x0000000000000000000000000000000000000101", &a, &b, 100.0, 50.0),
            v2("bc", "0x0000000000000000000000000000000000000102", &b, &c, 100.0, 100.0),
            v2("ca", "0x0000000000000000000000000000000000000103", &c, &a, 100.0, 200.0),
        ];
        let prices = [
            (a.address.to_ascii_lowercase(), 1.0),
            (b.address.to_ascii_lowercase(), 2.0),
            (c.address.to_ascii_lowercase(), 3.0),
        ].into_iter().collect::<HashMap<_, _>>();
        let starts = tokens.keys().cloned().collect::<HashSet<_>>();
        let (cycles, _, generated, _) = enumerate_cycles("test", &states, &tokens, &prices, &starts, 0.0, 1000);
        // Two directed orientations exist. Rotating the same orientation to a different
        // start token must not create three copies of each one.
        assert_eq!(generated.get(&3).copied().unwrap_or(0), 2);
        assert_eq!(cycles.iter().filter(|c| c.edges.len() == 3).count(), 2);
    }

    #[test]
    fn stable_id_is_deterministic() {
        assert_eq!(stable_id("abc"), stable_id("abc"));
        assert_ne!(stable_id("abc"), stable_id("abcd"));
    }

    #[test]
    fn raw_amount_round_trip() {
        let raw = amount_to_raw(12.345678, 6).unwrap();
        assert_eq!(raw, 12_345_678);
        assert!((raw_to_amount(raw, 6) - 12.345678).abs() < 1e-9);
    }

    #[test]
    fn inferred_prices_keep_alt_alt_middle_pool_eligible() {
        let a = token("USD", "0x0000000000000000000000000000000000000010", Some(1.0));
        let b = token("B", "0x0000000000000000000000000000000000000020", None);
        let c = token("C", "0x0000000000000000000000000000000000000030", None);
        let tokens = [a.clone(), b.clone(), c.clone()].into_iter()
            .map(|t| (t.address.to_ascii_lowercase(), t)).collect::<HashMap<_, _>>();
        let states = vec![
            // 1 USD token = 0.5 B => B ~= $2.
            v2("ab", "0x0000000000000000000000000000000000000101", &a, &b, 100.0, 50.0),
            // 1 B = 4 C => C ~= $0.50.
            v2("bc", "0x0000000000000000000000000000000000000102", &b, &c, 100.0, 400.0),
        ];
        let prices = infer_token_usd_prices(&states, &tokens, None);
        assert!((prices[&b.address.to_ascii_lowercase()] - 2.0).abs() < 1e-9);
        assert!((prices[&c.address.to_ascii_lowercase()] - 0.5).abs() < 1e-9);
        let hot = filter_states_by_anchor_liquidity(states, &prices, 100.0);
        assert_eq!(hot.len(), 2);
    }

    #[test]
    fn exact_quote_budget_is_shared_across_depths() {
        fn fake_cycle(depth: usize, n: usize) -> CycleCandidate {
            CycleCandidate {
                edges: (0..depth).map(|i| CycleEdge {
                    pool_idx: i,
                    token_in: format!("in-{n}-{i}"),
                    token_out: format!("out-{n}-{i}"),
                }).collect(),
                start_addr: format!("0x{n:040x}"),
                start_symbol: "USD".to_string(),
                token_path: vec![],
                spot_edge_bps: 10.0,
                venue_key: "test".to_string(),
                opportunity_id: format!("id-{depth}-{n}"),
            }
        }
        let mut cycles = Vec::new();
        for depth in 2..=4 {
            for n in 0..10 { cycles.push(fake_cycle(depth, n)); }
        }
        let quotas = exact_quote_quotas(&cycles, 5.0, 6);
        assert_eq!(quotas.values().sum::<usize>(), 6);
        assert!(quotas.get(&2).copied().unwrap_or(0) >= 1);
        assert!(quotas.get(&3).copied().unwrap_or(0) >= 1);
        assert!(quotas.get(&4).copied().unwrap_or(0) >= 1);
    }
}
