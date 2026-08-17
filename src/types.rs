use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub rpc: RpcConfig,
    pub scanner: ScannerConfig,
    pub economics: EconomicsConfig,
    pub chainlink: ChainlinkConfig,
    #[serde(default)]
    pub external: ExternalVenueConfig,
    pub tokens: Vec<TokenConfig>,
    pub dexes: Vec<DexConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RpcConfig {
    pub url_env: String,
    #[serde(default = "default_rpc_max_attempts")]
    pub max_attempts: u32,
    #[serde(default = "default_rpc_retry_base_ms")]
    pub retry_base_ms: u64,
    #[serde(default = "default_rpc_retry_max_ms")]
    pub retry_max_ms: u64,
    #[serde(default = "default_rpc_request_timeout_ms")]
    pub request_timeout_ms: u64,
}

fn default_rpc_max_attempts() -> u32 {
    4
}
fn default_rpc_retry_base_ms() -> u64 {
    100
}
fn default_rpc_retry_max_ms() -> u64 {
    1500
}
fn default_rpc_request_timeout_ms() -> u64 {
    5000
}

#[derive(Debug, Clone, Deserialize)]
pub struct ScannerConfig {
    pub poll_interval_ms: u64,
    pub max_input_fraction_of_reserve: f64,
    pub max_trade_usd: f64,
    #[serde(default = "default_min_trade_usd")]
    pub min_trade_usd: f64,
    pub min_net_profit_usd: f64,
    #[serde(default = "default_min_spot_edge_bps")]
    pub min_spot_edge_bps: f64,
    pub database_path: String,
    #[serde(default = "default_concurrency")]
    pub rpc_concurrency: usize,
    /// Startup factory-discovery concurrency. Kept deliberately lower than the live
    /// scanner concurrency because discovery is not latency-sensitive and can burst
    /// hundreds of eth_call requests on a cold cache.
    #[serde(default = "default_discovery_concurrency")]
    pub discovery_concurrency: usize,
    /// Small per-probe pacing delay used only during cold-cache pool discovery.
    #[serde(default = "default_discovery_probe_delay_ms")]
    pub discovery_probe_delay_ms: u64,
    /// Persistent pool-universe cache. A matching fresh cache skips factory discovery.
    #[serde(default = "default_pool_cache_path")]
    pub pool_cache_path: String,
    /// Rediscover the configured pool universe after this many hours.
    #[serde(default = "default_pool_cache_ttl_hours")]
    pub pool_cache_ttl_hours: u64,
    #[serde(default = "default_quote_concurrency")]
    pub quote_concurrency: usize,
    #[serde(default = "default_v3_grid_points")]
    pub v3_quote_grid_points: usize,
    #[serde(default = "default_v3_refine_iterations")]
    pub v3_refine_iterations: usize,
    #[serde(default = "default_near_miss_count")]
    pub near_miss_count: usize,
    /// Number of V3 tick-bitmap words loaded on each side of the current word.
    /// One side + current + the other side = 3 words with the default value of 1.
    #[serde(default = "default_v3_tick_bitmap_words_each_side")]
    pub v3_tick_bitmap_words_each_side: i32,
    /// Guardrail for pathological/dense pools. If the loaded bitmap window contains
    /// more initialized ticks than this, local quoting is disabled for that pool.
    #[serde(default = "default_v3_max_initialized_ticks_per_pool")]
    pub v3_max_initialized_ticks_per_pool: usize,
    /// Highest-ranked V3 routes per block that are re-quoted through QuoterV2.
    /// Any locally-positive V3 route is validated regardless of this count.
    #[serde(default = "default_v3_validation_routes")]
    pub v3_validation_routes: usize,
    /// Refresh cached initialized-tick liquidity periodically even if the pool remains
    /// in the same bitmap word. QuoterV2 validation catches top-route drift between refreshes.
    #[serde(default = "default_v3_tick_cache_refresh_blocks")]
    pub v3_tick_cache_refresh_blocks: u64,
    /// Drop pools whose USD-anchored side is below this approximate notional depth.
    /// For V2 this uses reserves; for V3 it uses active-liquidity virtual reserves.
    #[serde(default = "default_min_pool_anchor_liquidity_usd")]
    pub min_pool_anchor_liquidity_usd: f64,
    /// Hard guardrail after marginal screening. Highest spot-edge routes survive first.
    #[serde(default = "default_max_candidate_routes")]
    pub max_candidate_routes: usize,
    /// Canonical Multicall3 deployment used to batch live pool-state reads into a few eth_call requests.
    #[serde(default = "default_multicall3_address")]
    pub multicall3_address: String,
    /// Maximum inner contract calls per Multicall3 aggregate3 request. Smaller chunks are friendlier to public RPC providers.
    #[serde(default = "default_multicall_max_calls")]
    pub multicall_max_calls: usize,
    /// Full discovered-universe liquidity refresh cadence. Between refreshes only the hot/eligible pool set is read.
    #[serde(default = "default_liquidity_refresh_blocks")]
    pub liquidity_refresh_blocks: u64,
}

fn default_concurrency() -> usize {
    24
}
fn default_discovery_concurrency() -> usize {
    4
}
fn default_discovery_probe_delay_ms() -> u64 {
    25
}
fn default_pool_cache_path() -> String {
    "data/pool_cache.json".to_string()
}
fn default_pool_cache_ttl_hours() -> u64 {
    24
}
fn default_quote_concurrency() -> usize {
    6
}
fn default_min_trade_usd() -> f64 {
    10.0
}
fn default_min_spot_edge_bps() -> f64 {
    8.0
}
fn default_v3_grid_points() -> usize {
    6
}
fn default_v3_refine_iterations() -> usize {
    4
}
fn default_near_miss_count() -> usize {
    5
}
fn default_v3_tick_bitmap_words_each_side() -> i32 {
    2
}
fn default_v3_max_initialized_ticks_per_pool() -> usize {
    1024
}
fn default_v3_validation_routes() -> usize {
    5
}
fn default_v3_tick_cache_refresh_blocks() -> u64 {
    20
}
fn default_min_pool_anchor_liquidity_usd() -> f64 {
    25_000.0
}
fn default_max_candidate_routes() -> usize {
    5_000
}
fn default_multicall3_address() -> String {
    "0xcA11bde05977b3631167028862bE2a173976CA11".to_string()
}
fn default_multicall_max_calls() -> usize {
    80
}
fn default_liquidity_refresh_blocks() -> u64 {
    20
}

#[derive(Debug, Clone, Deserialize)]
pub struct EconomicsConfig {
    pub flash_loan_premium_bps: f64,
    pub estimated_gas_units: u64,
    pub mev_bid_reserve_pct: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChainlinkConfig {
    pub native_usd_feed: String,
    pub feed_decimals: u8,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExternalVenueConfig {
    #[serde(default = "default_external_enabled")]
    pub enabled: bool,
    /// External venue probes are deliberately slower/candidate-discovery work.
    /// The core Multicall/local-math scanner still runs every block.
    #[serde(default = "default_external_probe_every_blocks")]
    pub probe_every_blocks: u64,
    /// Rotate through at most this many directed token pairs on each probe block.
    #[serde(default = "default_external_max_pairs_per_probe")]
    pub max_pairs_per_probe: usize,
    /// Keep only the strongest first-leg quotes before asking every venue for an exact reverse quote.
    #[serde(default = "default_external_first_leg_limit")]
    pub first_leg_limit: usize,
    #[serde(default = "default_external_concurrency")]
    pub concurrency: usize,
    #[serde(default = "default_curve_enabled")]
    pub curve_enabled: bool,
    #[serde(default = "default_curve_rate_provider")]
    pub curve_rate_provider: String,
    #[serde(default = "default_balancer_enabled")]
    pub balancer_enabled: bool,
    #[serde(default = "default_balancer_api_url")]
    pub balancer_api_url: String,
    #[serde(default = "default_external_http_timeout_ms")]
    pub http_timeout_ms: u64,
    /// Conservative gas uplift for Curve/Balancer-involved routes versus the core two-pool estimate.
    #[serde(default = "default_external_gas_multiplier")]
    pub gas_multiplier: f64,
    /// Number of logarithmically-spaced exact size samples for a verified gross-positive route.
    #[serde(default = "default_external_sizing_grid_points")]
    pub sizing_grid_points: usize,
    /// Number of local log-space refinement rounds around the best size sample.
    #[serde(default = "default_external_sizing_refine_iterations")]
    pub sizing_refine_iterations: usize,
    /// Bound exact sizing work per directed token pair on an external probe block.
    #[serde(default = "default_external_sizing_max_routes_per_pair")]
    pub sizing_max_routes_per_pair: usize,
}

fn default_external_enabled() -> bool {
    true
}
fn default_external_probe_every_blocks() -> u64 {
    5
}
fn default_external_max_pairs_per_probe() -> usize {
    4
}
fn default_external_first_leg_limit() -> usize {
    2
}
fn default_external_concurrency() -> usize {
    4
}
fn default_curve_enabled() -> bool {
    true
}
fn default_curve_rate_provider() -> String {
    "0xA834f3d23749233c9B61ba723588570A1cCA0Ed7".to_string()
}
fn default_balancer_enabled() -> bool {
    true
}
fn default_balancer_api_url() -> String {
    "https://api-v3.balancer.fi/".to_string()
}
fn default_external_http_timeout_ms() -> u64 {
    1500
}
fn default_external_gas_multiplier() -> f64 {
    1.35
}
fn default_external_sizing_grid_points() -> usize {
    10
}
fn default_external_sizing_refine_iterations() -> usize {
    3
}
fn default_external_sizing_max_routes_per_pair() -> usize {
    4
}

impl Default for ExternalVenueConfig {
    fn default() -> Self {
        Self {
            enabled: default_external_enabled(),
            probe_every_blocks: default_external_probe_every_blocks(),
            max_pairs_per_probe: default_external_max_pairs_per_probe(),
            first_leg_limit: default_external_first_leg_limit(),
            concurrency: default_external_concurrency(),
            curve_enabled: default_curve_enabled(),
            curve_rate_provider: default_curve_rate_provider(),
            balancer_enabled: default_balancer_enabled(),
            balancer_api_url: default_balancer_api_url(),
            http_timeout_ms: default_external_http_timeout_ms(),
            gas_multiplier: default_external_gas_multiplier(),
            sizing_grid_points: default_external_sizing_grid_points(),
            sizing_refine_iterations: default_external_sizing_refine_iterations(),
            sizing_max_routes_per_pair: default_external_sizing_max_routes_per_pair(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TokenConfig {
    pub symbol: String,
    pub address: String,
    pub decimals: u8,
    #[serde(default)]
    pub usd_price: Option<f64>,
    #[serde(default)]
    pub wrapped_native: bool,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DexKind {
    V2,
    V3,
    /// Algebra V1.9 concentrated-liquidity pools (Camelot AMMv3 on Arbitrum).
    /// Kept distinct from canonical Uniswap V3 so TickLens/local-V3 assumptions
    /// can never be applied accidentally.
    Algebra,
    /// Solidly-family classic pools (Aerodrome/Velodrome volatile or stable).
    /// These expose V2-like reserves but use factory-controlled dynamic fees and
    /// a pool-level exact getAmountOut quote. `solidly_stable` selects the stable
    /// invariant for breadth marginal screening. Legacy execution must never assume
    /// a Uniswap V2 router for these pools.
    Solidly,
    /// Aerodrome/Velodrome Slipstream concentrated-liquidity pools. They use
    /// tick-spacing keyed factories and dedicated quoters, so they remain distinct
    /// from canonical Uniswap V3 even though slot0/liquidity are V3-like.
    Slipstream,
}

impl Default for DexKind {
    fn default() -> Self {
        Self::V2
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct DexConfig {
    pub name: String,
    #[serde(default)]
    pub kind: DexKind,
    pub factory: String,
    #[serde(default)]
    pub fee_bps: Option<f64>,
    #[serde(default)]
    pub quoter_v2: Option<String>,
    /// TickLens compatible with this V3 deployment. Kept per venue because forks may
    /// deploy their own periphery even when the core pool interface is compatible.
    #[serde(default)]
    pub tick_lens: Option<String>,
    #[serde(default)]
    pub fee_tiers: Vec<u32>,
    /// Solidly classic pools are keyed by a stable/volatile boolean. False by default.
    #[serde(default)]
    pub solidly_stable: bool,
    /// For Slipstream entries quoted through MixedRouteQuoterV3, OR this factory
    /// selection bitmask into the raw tick spacing (0, 1<<20, or 1<<19). None means
    /// use the factory-specific standard QuoterV2 `quoteExactInputSingle` ABI.
    #[serde(default)]
    pub slipstream_factory_mask: Option<i32>,
    /// SyncSwap Classic pools use V2-style reserves, but discovery is through
    /// factory.getPool(tokenA,tokenB) and finite quotes must call the pool's
    /// getAmountOut(tokenIn,amountIn,sender). Kept as a V2 sub-adapter so the
    /// legacy executor never mistakes it for a UniswapV2Router-compatible venue.
    #[serde(default)]
    pub syncswap_classic: bool,
}

#[derive(Debug, Clone)]
pub struct PoolDefinition {
    pub dex: DexConfig,
    pub pool_address: String,
    pub token0: String,
    pub token1: String,
    pub token0_decimals: u8,
    pub token1_decimals: u8,
    pub fee_tier: Option<u32>,
    pub tick_spacing: Option<i32>,
}

impl PoolDefinition {
    pub fn fee_bps(&self) -> f64 {
        match self.dex.kind {
            DexKind::V2 => self.dex.fee_bps.unwrap_or(0.0),
            // Uniswap V3 fee tier is in hundredths of a basis point.
            DexKind::V3 => self.fee_tier.unwrap_or(0) as f64 / 100.0,
            // Algebra's live fee is populated into the per-state cloned DEX config
            // from globalState(); discovery/cache definitions intentionally leave it unset.
            DexKind::Algebra => self.dex.fee_bps.unwrap_or(0.0),
            // V3.3.1 refreshes the factory-controlled fee with each pool snapshot.
            DexKind::Solidly => self.dex.fee_bps.unwrap_or(0.0),
            // Slipstream's live swap fee is populated into the per-state cloned DEX
            // definition from ICLFactory.getSwapFee(pool).
            DexKind::Slipstream => self.dex.fee_bps.unwrap_or(0.0),
        }
    }

    pub fn label(&self) -> String {
        match self.dex.kind {
            DexKind::V2 => self.dex.name.clone(),
            DexKind::V3 => format!(
                "{}[{:.2}%]",
                self.dex.name,
                self.fee_tier.unwrap_or(0) as f64 / 10_000.0
            ),
            DexKind::Algebra => {
                if let Some(fee_bps) = self.dex.fee_bps {
                    format!("{}[Algebra {:.3}%]", self.dex.name, fee_bps / 100.0)
                } else {
                    format!("{}[Algebra]", self.dex.name)
                }
            }
            DexKind::Solidly => {
                let curve = if self.dex.solidly_stable { "stable" } else { "volatile" };
                if let Some(fee_bps) = self.dex.fee_bps {
                    format!("{}[{} {:.3}%]", self.dex.name, curve, fee_bps / 100.0)
                } else {
                    format!("{}[{}]", self.dex.name, curve)
                }
            }
            DexKind::Slipstream => {
                let spacing = self.tick_spacing.map(|v| v.to_string()).unwrap_or_else(|| "?".to_string());
                if let Some(fee_bps) = self.dex.fee_bps {
                    format!("{}[Slipstream ts={} {:.3}%]", self.dex.name, spacing, fee_bps / 100.0)
                } else {
                    format!("{}[Slipstream ts={}]", self.dex.name, spacing)
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct V2PoolState {
    pub def: PoolDefinition,
    pub reserve0: f64,
    pub reserve1: f64,
}

#[derive(Debug, Clone)]
pub struct V3Tick {
    pub tick: i32,
    /// Signed liquidityNet from IUniswapV3Pool.ticks(tick), represented as f64
    /// for the V0.1.2 local scanner math.
    pub liquidity_net: f64,
}

#[derive(Debug, Clone)]
pub struct V3TickCache {
    pub ticks: Vec<V3Tick>,
    /// Bitmap words whose TickLens result is already known, including empty words.
    /// Keeping empty words prevents repeated RPC reads when a route traverses sparse liquidity.
    pub loaded_words: Vec<i32>,
    /// Inclusive loaded bitmap-word bounds. V0.1.2d exposes only a contiguous known word range around the current tick.
    pub min_word: i32,
    pub max_word: i32,
    pub min_tick: i32,
    pub max_tick: i32,
}

#[derive(Debug, Clone)]
pub struct V3PoolState {
    pub def: PoolDefinition,
    /// Q64.96 sqrt price converted to f64. V0.1.2d uses this both for the cheap
    /// marginal screen and for the local tick-aware scanner quote.
    pub sqrt_price_x96: f64,
    pub tick: i32,
    /// Active raw V3 liquidity (uint128 converted to f64).
    pub liquidity: f64,
    /// Loaded only for V3 pools touched by marginally-interesting candidate routes.
    pub tick_cache: Option<V3TickCache>,
}

#[derive(Debug, Clone)]
pub enum PoolState {
    V2(V2PoolState),
    V3(V3PoolState),
}

impl PoolState {
    pub fn def(&self) -> &PoolDefinition {
        match self {
            Self::V2(s) => &s.def,
            Self::V3(s) => &s.def,
        }
    }

    pub fn is_v3(&self) -> bool {
        self.def().dex.kind == DexKind::V3
    }

    pub fn is_algebra(&self) -> bool {
        self.def().dex.kind == DexKind::Algebra
    }

    pub fn is_slipstream(&self) -> bool {
        self.def().dex.kind == DexKind::Slipstream
    }

    pub fn is_concentrated(&self) -> bool {
        self.is_v3() || self.is_algebra() || self.is_slipstream()
    }

    pub fn pool_address(&self) -> &str {
        &self.def().pool_address
    }
    pub fn token0(&self) -> &str {
        &self.def().token0
    }
    pub fn token1(&self) -> &str {
        &self.def().token1
    }
    pub fn fee_bps(&self) -> f64 {
        self.def().fee_bps()
    }
    pub fn label(&self) -> String {
        self.def().label()
    }

    /// Human-token marginal output rate after pool fee. This is only a prefilter.
    /// V0.2 sizes V3 routes locally, then QuoterV2-validates the highest-ranked routes.
    pub fn marginal_rate(&self, token_in: &str, token_out: &str) -> Option<f64> {
        let fee_multiplier = 1.0 - self.fee_bps() / 10_000.0;
        match self {
            Self::V2(s) => {
                let zero_for_one = if eq_addr(token_in, &s.def.token0) && eq_addr(token_out, &s.def.token1) {
                    true
                } else if eq_addr(token_in, &s.def.token1) && eq_addr(token_out, &s.def.token0) {
                    false
                } else {
                    return None;
                };

                // Aerodrome/Velodrome stable pools use x^3*y + y^3*x = k on
                // decimal-normalized balances, not x*y=k. The infinitesimal output
                // slope is -dx/dy (or its inverse), then the live swap fee is applied.
                if s.def.dex.kind == DexKind::Solidly && s.def.dex.solidly_stable {
                    // reserves are already in human-token units; Solidly's internal
                    // 1e18 normalization applies the same scale to both axes.
                    let x = s.reserve0;
                    let y = s.reserve1;
                    if !x.is_finite() || !y.is_finite() || x <= 0.0 || y <= 0.0 { return None; }
                    let dy_dx_norm = (3.0 * x * x * y + y * y * y) / (x * x * x + 3.0 * y * y * x);
                    if !dy_dx_norm.is_finite() || dy_dx_norm <= 0.0 { return None; }
                    let ratio = if zero_for_one { dy_dx_norm } else { 1.0 / dy_dx_norm };
                    return Some(ratio * fee_multiplier);
                }

                let ratio = if zero_for_one { s.reserve1 / s.reserve0 } else { s.reserve0 / s.reserve1 };
                Some(ratio * fee_multiplier)
            }
            Self::V3(s) => {
                if s.sqrt_price_x96 <= 0.0 {
                    return None;
                }
                let q96 = 2f64.powi(96);
                let raw_1_per_0 = (s.sqrt_price_x96 / q96).powi(2);
                let human_1_per_0 = raw_1_per_0
                    * 10f64.powi(s.def.token0_decimals as i32 - s.def.token1_decimals as i32);
                let ratio = if eq_addr(token_in, &s.def.token0) && eq_addr(token_out, &s.def.token1)
                {
                    human_1_per_0
                } else if eq_addr(token_in, &s.def.token1) && eq_addr(token_out, &s.def.token0) {
                    1.0 / human_1_per_0
                } else {
                    return None;
                };
                Some(ratio * fee_multiplier)
            }
        }
    }

    pub fn v2_reserve_in(&self, token_in: &str, token_out: &str) -> Option<f64> {
        let Self::V2(s) = self else {
            return None;
        };
        if eq_addr(token_in, &s.def.token0) && eq_addr(token_out, &s.def.token1) {
            Some(s.reserve0)
        } else if eq_addr(token_in, &s.def.token1) && eq_addr(token_out, &s.def.token0) {
            Some(s.reserve1)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub struct Opportunity {
    pub observed_at: String,
    pub block_number: u64,
    pub pair: String,
    pub start_token: String,
    pub mid_token: String,
    pub first_dex: String,
    pub second_dex: String,
    pub first_kind: String,
    pub second_kind: String,
    pub first_pool: String,
    pub second_pool: String,
    pub first_fee_bps: f64,
    pub second_fee_bps: f64,
    pub first_ticks_crossed: Option<u32>,
    pub second_ticks_crossed: Option<u32>,
    pub spot_edge_bps: f64,
    pub input_amount: f64,
    pub gross_profit_start: f64,
    pub gross_profit_usd: f64,
    pub flash_fee_usd: f64,
    pub gas_usd: f64,
    pub mev_bid_reserve_usd: f64,
    pub net_pre_bid_usd: f64,
    pub estimated_net_usd: f64,
    pub roi_bps: f64,
}

#[derive(Debug, Clone, Default)]
pub struct ScanMetrics {
    /// Pools discovered from all configured factories before live liquidity filtering.
    pub discovered_pool_count: usize,
    /// Pools removed by the coarse USD-anchored liquidity filter this block.
    pub liquidity_filtered_pool_count: usize,
    pub v2_pool_count: usize,
    pub v3_pool_count: usize,
    pub candidate_count: usize,
    pub marginal_positive_count: usize,
    pub post_swap_positive_count: usize,
    pub post_flash_positive_count: usize,
    pub post_gas_positive_count: usize,
    pub post_mev_positive_count: usize,
    pub qualifying_count: usize,
    /// QuoterV2 calls now mean validation/fallback calls, not every sizing sample.
    pub quote_calls: u64,
    pub quote_failures: u64,
    pub quote_rpc_ms: u128,
    pub local_quote_calls: u64,
    pub local_quote_failures: u64,
    pub tick_rpc_calls: u64,
    pub tick_rpc_ms: u128,
    pub v3_tick_cache_pools: usize,
    pub v3_tick_cache_hits: usize,
    pub v3_tick_cache_failures: usize,
    /// Number of directional cache-expansion rounds performed after the initial local quote pass.
    pub lazy_expand_rounds: usize,
    /// TickLens word calls made only for directional cache expansion. Included in tick_rpc_calls too.
    pub lazy_expand_rpc_calls: u64,
    /// Number of V3 pool caches widened by at least one bitmap word this scan.
    pub lazy_expand_pools: usize,
    /// Routes whose search upper bound was reduced by locally-known V3 liquidity capacity.
    pub capacity_limited_routes: usize,
    /// Capacity-limited routes whose optimum pressed the local-cache boundary and requested more state.
    pub capacity_expand_requests: usize,
    /// Unique V3 bitmap words retained in the persistent per-pool cache, including empty words.
    pub unique_tick_words_cached: usize,
    /// Expansion attempts that would have re-fetched a known word. Expected to remain zero.
    pub duplicate_word_fetches: u64,
    /// Unique V3 pool directions still considered locally executable this scan.
    pub v3_viable_directions: usize,
    /// Candidate V3 pool directions temporarily parked as too sparse for the minimum trade.
    pub v3_nonviable_directions: usize,
    /// Candidate routes skipped because one or more V3 legs were temporarily non-viable.
    pub skipped_nonviable_routes: usize,
    /// V3 leg quotes avoided by the temporary non-viable-direction cache.
    pub skipped_nonviable_quotes: u64,
    /// Newly parked V3 directions in this scan.
    pub nonviable_marked_this_scan: usize,
    pub validation_max_error_bps: f64,
    pub rpc_retries: u64,
    pub rpc_failures: u64,
    pub rpc_rate_limits: u64,
    pub state_fetch_ms: u128,
    pub route_eval_ms: u128,
    pub scan_total_ms: u128,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RpcStatsSnapshot {
    pub requests: u64,
    pub retries: u64,
    pub failures: u64,
    pub rate_limits: u64,
}

#[derive(Debug, Clone)]
pub struct V3Quote {
    pub amount_out_raw: u128,
    pub initialized_ticks_crossed: u32,
    pub gas_estimate: u128,
}

pub fn eq_addr(a: &str, b: &str) -> bool {
    a.eq_ignore_ascii_case(b)
}
