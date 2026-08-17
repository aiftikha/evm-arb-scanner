use anyhow::{anyhow, Result};

use crate::types::{eq_addr, DexKind, PoolState, V3PoolState};

const Q96: f64 = 79_228_162_514_264_337_593_543_950_336.0;
const MIN_TICK: i32 = -887_272;
const MAX_TICK: i32 = 887_272;

/// Fast local Uniswap V3 exact-input quote for scanner use.
///
/// The swap path is tick-aware and follows the V3 liquidity-transition rules,
/// but uses f64 arithmetic. QuoterV2 remains the final correctness oracle for
/// the highest-ranked routes before anything is treated as actionable.
pub fn v3_leg_output_local(
    amount_in_human: f64,
    pool: &PoolState,
    token_in: &str,
    token_out: &str,
) -> Result<(f64, u32)> {
    let PoolState::V3(state) = pool else {
        return Err(anyhow!("V3_MATH: local V3 quote called with non-V3 pool"));
    };
    if state.def.dex.kind != DexKind::V3 {
        return Err(anyhow!(
            "V3_MATH: canonical V3 math called with non-canonical pool"
        ));
    }
    local_quote_state(amount_in_human, state, token_in, token_out)
}

/// Conservative exact-input V3 quote that is valid only while the swap stays
/// inside the current tick-spacing interval. This deliberately avoids assuming
/// any unseen initialized-tick liquidity. It is used by the event-driven shadow
/// finite-size probe before a full tick-aware/Quoter validation exists.
pub fn v3_leg_output_current_tick(
    amount_in_human: f64,
    pool: &PoolState,
    token_in: &str,
    token_out: &str,
) -> Result<f64> {
    let PoolState::V3(state) = pool else {
        return Err(anyhow!(
            "V3_MATH: current-tick quote called with non-V3 pool"
        ));
    };
    if state.def.dex.kind != DexKind::V3 {
        return Err(anyhow!(
            "V3_MATH: canonical current-tick math called with non-canonical pool"
        ));
    }
    current_interval_quote_state(amount_in_human, state, token_in, token_out)
}

/// V3.4 venue-agnostic concentrated-liquidity finite quote inside the current
/// tick-spacing interval. Uniswap V3, Algebra V1.9 and Slipstream all share the
/// same active-liquidity x/y geometry between initialized ticks; this function
/// deliberately refuses to cross the next possible initialized boundary.
pub fn concentrated_leg_output_current_interval(
    amount_in_human: f64,
    pool: &PoolState,
    token_in: &str,
    token_out: &str,
) -> Result<f64> {
    let PoolState::V3(state) = pool else {
        return Err(anyhow!("V3_MATH: concentrated quote called with non-CL pool"));
    };
    if !matches!(state.def.dex.kind, DexKind::V3 | DexKind::Algebra | DexKind::Slipstream) {
        return Err(anyhow!("V3_MATH: concentrated quote called with non-CL DEX kind"));
    }
    current_interval_quote_state(amount_in_human, state, token_in, token_out)
}

fn current_interval_quote_state(
    amount_in_human: f64,
    state: &V3PoolState,
    token_in: &str,
    token_out: &str,
) -> Result<f64> {
    if amount_in_human <= 0.0 || !amount_in_human.is_finite() {
        return Err(anyhow!("V3_MATH: invalid current-interval input amount"));
    }
    if state.sqrt_price_x96 <= 0.0
        || !state.sqrt_price_x96.is_finite()
        || state.liquidity <= 0.0
        || !state.liquidity.is_finite()
    {
        return Err(anyhow!("V3_MATH: invalid current-interval pool state"));
    }

    let (zero_for_one, input_decimals, output_decimals) =
        direction_and_decimals(state, token_in, token_out)?;
    let amount_in_raw = amount_in_human * 10f64.powi(input_decimals as i32);
    if amount_in_raw <= 0.0 || !amount_in_raw.is_finite() {
        return Err(anyhow!("V3_MATH: invalid current-interval raw input"));
    }

    let fee_multiplier = 1.0 - state.def.fee_bps() / 10_000.0;
    if !(0.0..1.0).contains(&fee_multiplier) {
        return Err(anyhow!("V3_MATH: invalid current-interval fee"));
    }
    let effective_in = amount_in_raw * fee_multiplier;
    let sqrt_price = state.sqrt_price_x96 / Q96;
    let liquidity = state.liquidity;
    let spacing = state.def.tick_spacing
        .ok_or_else(|| anyhow!("V3_CURRENT_INTERVAL_NO_SPACING: tick spacing unavailable"))?
        .abs()
        .max(1);
    let compressed = state.tick.div_euclid(spacing);

    let amount_out_raw = if zero_for_one {
        let lower_tick = compressed.saturating_mul(spacing).clamp(MIN_TICK, MAX_TICK);
        let boundary = sqrt_ratio_at_tick(lower_tick);
        if relative_close(sqrt_price, boundary, 1e-13) {
            return Err(anyhow!(
                "V3_CURRENT_TICK_BOUNDARY: zero-for-one starts at spacing boundary"
            ));
        }
        let next = next_sqrt_from_amount0_in(sqrt_price, liquidity, effective_in);
        if !next.is_finite()
            || next <= 0.0
            || (next < boundary && !relative_close(next, boundary, 1e-12))
        {
            return Err(anyhow!(
                "V3_CURRENT_TICK_CROSS: quote would cross lower spacing boundary"
            ));
        }
        amount1_delta(liquidity, next, sqrt_price)
    } else {
        let upper_tick = compressed
            .saturating_add(1)
            .saturating_mul(spacing)
            .clamp(MIN_TICK, MAX_TICK);
        let boundary = sqrt_ratio_at_tick(upper_tick);
        let next = sqrt_price + effective_in / liquidity;
        if !next.is_finite()
            || next <= 0.0
            || (next > boundary && !relative_close(next, boundary, 1e-12))
        {
            return Err(anyhow!(
                "V3_CURRENT_TICK_CROSS: quote would cross upper spacing boundary"
            ));
        }
        amount0_delta(liquidity, sqrt_price, next)
    };

    if amount_out_raw <= 0.0 || !amount_out_raw.is_finite() {
        return Err(anyhow!("V3_MATH: current-interval quote produced no output"));
    }
    Ok(amount_out_raw / 10f64.powi(output_decimals as i32))
}

/// Conservative amount of token_in that can be simulated before the currently
/// loaded directional tick cache is exhausted. This is used only to cap the
/// search interval; it is not an execution quote.
pub fn v3_input_capacity_local(pool: &PoolState, token_in: &str, token_out: &str) -> Result<f64> {
    let PoolState::V3(state) = pool else {
        return Err(anyhow!("V3_MATH: capacity called with non-V3 pool"));
    };
    if state.def.dex.kind != DexKind::V3 {
        return Err(anyhow!(
            "V3_MATH: canonical V3 capacity called with non-canonical pool"
        ));
    }
    input_capacity_state(state, token_in, token_out)
}

fn direction_and_decimals(
    state: &V3PoolState,
    token_in: &str,
    token_out: &str,
) -> Result<(bool, u8, u8)> {
    if eq_addr(token_in, &state.def.token0) && eq_addr(token_out, &state.def.token1) {
        Ok((true, state.def.token0_decimals, state.def.token1_decimals))
    } else if eq_addr(token_in, &state.def.token1) && eq_addr(token_out, &state.def.token0) {
        Ok((false, state.def.token1_decimals, state.def.token0_decimals))
    } else {
        Err(anyhow!("V3_MATH: tokens do not match V3 pool"))
    }
}

fn local_quote_state(
    amount_in_human: f64,
    state: &V3PoolState,
    token_in: &str,
    token_out: &str,
) -> Result<(f64, u32)> {
    if amount_in_human <= 0.0 || !amount_in_human.is_finite() {
        return Err(anyhow!("V3_MATH: invalid V3 input amount"));
    }
    let cache = state
        .tick_cache
        .as_ref()
        .ok_or_else(|| anyhow!("V3_NO_CACHE: local tick cache unavailable"))?;

    let (zero_for_one, input_decimals, output_decimals) =
        direction_and_decimals(state, token_in, token_out)?;

    let mut amount_remaining = amount_in_human * 10f64.powi(input_decimals as i32);
    if !amount_remaining.is_finite() || amount_remaining <= 0.0 {
        return Err(anyhow!("V3_MATH: raw input is invalid"));
    }

    let mut sqrt_price = state.sqrt_price_x96 / Q96;
    let mut liquidity = state.liquidity;
    let mut current_tick = state.tick;
    let fee_fraction = state.def.fee_bps() / 10_000.0;
    let fee_multiplier = 1.0 - fee_fraction;
    if !(0.0..1.0).contains(&fee_multiplier) {
        return Err(anyhow!("V3_MATH: invalid V3 fee"));
    }

    let mut amount_out_raw = 0.0f64;
    let mut ticks_crossed = 0u32;
    let max_steps = cache.ticks.len().saturating_add(8).max(12);

    for _ in 0..max_steps {
        if amount_remaining <= 1e-9 {
            break;
        }
        if liquidity < 0.0 || !liquidity.is_finite() || sqrt_price <= 0.0 {
            return Err(anyhow!("V3_MATH: invalid local liquidity/price"));
        }

        let next = next_initialized_tick(cache, current_tick, zero_for_one);
        let (target_tick, liquidity_net, initialized) = if let Some(tick) = next {
            (tick.tick, tick.liquidity_net, true)
        } else {
            let boundary = if zero_for_one {
                cache.min_tick
            } else {
                cache.max_tick
            };
            if (zero_for_one && current_tick <= boundary)
                || (!zero_for_one && current_tick >= boundary)
            {
                return Err(anyhow!(
                    "V3_OUTSIDE_CACHE: quote reached loaded tick boundary"
                ));
            }
            (boundary, 0.0, false)
        };

        let target_sqrt = sqrt_ratio_at_tick(target_tick);
        if !target_sqrt.is_finite() || target_sqrt <= 0.0 {
            return Err(anyhow!("V3_MATH: invalid target sqrt price"));
        }

        // slot0.tick may sit exactly on an initialized boundary. Cross it with
        // zero input before evaluating the next price range.
        if relative_close(sqrt_price, target_sqrt, 1e-14) {
            if initialized {
                liquidity = apply_liquidity_net(liquidity, liquidity_net, zero_for_one)?;
                ticks_crossed = ticks_crossed.saturating_add(1);
                current_tick = if zero_for_one {
                    target_tick - 1
                } else {
                    target_tick
                };
                sqrt_price = target_sqrt;
                continue;
            }
            return Err(anyhow!("V3_OUTSIDE_CACHE: stalled at loaded tick boundary"));
        }

        if zero_for_one && target_sqrt >= sqrt_price {
            return Err(anyhow!("V3_MATH: invalid lower tick target"));
        }
        if !zero_for_one && target_sqrt <= sqrt_price {
            return Err(anyhow!("V3_MATH: invalid upper tick target"));
        }

        // A gap can legitimately have zero active liquidity. Walk to the next
        // initialized tick at zero token flow and resume after crossing it.
        if liquidity == 0.0 {
            if !initialized {
                if target_tick <= MIN_TICK || target_tick >= MAX_TICK {
                    return Err(anyhow!(
                        "V3_LIQUIDITY_EXHAUSTED: no active liquidity before protocol boundary"
                    ));
                }
                return Err(anyhow!(
                    "V3_OUTSIDE_CACHE: zero-liquidity gap continues beyond loaded words"
                ));
            }
            sqrt_price = target_sqrt;
            liquidity = apply_liquidity_net(liquidity, liquidity_net, zero_for_one)?;
            ticks_crossed = ticks_crossed.saturating_add(1);
            current_tick = if zero_for_one {
                target_tick - 1
            } else {
                target_tick
            };
            continue;
        }

        let effective_needed = if zero_for_one {
            amount0_delta(liquidity, target_sqrt, sqrt_price)
        } else {
            amount1_delta(liquidity, sqrt_price, target_sqrt)
        };
        if !effective_needed.is_finite() || effective_needed < 0.0 {
            return Err(anyhow!("V3_MATH: invalid step input"));
        }
        let gross_needed = effective_needed / fee_multiplier;

        if amount_remaining + gross_needed.abs() * 1e-12 >= gross_needed {
            amount_remaining = (amount_remaining - gross_needed).max(0.0);
            amount_out_raw += if zero_for_one {
                amount1_delta(liquidity, target_sqrt, sqrt_price)
            } else {
                amount0_delta(liquidity, sqrt_price, target_sqrt)
            };
            sqrt_price = target_sqrt;

            if initialized {
                liquidity = apply_liquidity_net(liquidity, liquidity_net, zero_for_one)?;
                ticks_crossed = ticks_crossed.saturating_add(1);
                current_tick = if zero_for_one {
                    target_tick - 1
                } else {
                    target_tick
                };
            } else if amount_remaining > 1e-9 {
                return Err(anyhow!(
                    "V3_OUTSIDE_CACHE: quote requires another bitmap word"
                ));
            }
        } else {
            let effective_in = amount_remaining * fee_multiplier;
            let next_sqrt = if zero_for_one {
                next_sqrt_from_amount0_in(sqrt_price, liquidity, effective_in)
            } else {
                sqrt_price + effective_in / liquidity
            };
            if !next_sqrt.is_finite() || next_sqrt <= 0.0 {
                return Err(anyhow!("V3_MATH: invalid partial-step price"));
            }
            amount_out_raw += if zero_for_one {
                amount1_delta(liquidity, next_sqrt, sqrt_price)
            } else {
                amount0_delta(liquidity, sqrt_price, next_sqrt)
            };
            amount_remaining = 0.0;
        }
    }

    if amount_remaining > 1e-6 {
        return Err(anyhow!(
            "V3_OUTSIDE_CACHE: quote exceeded local step budget"
        ));
    }
    if !amount_out_raw.is_finite() || amount_out_raw <= 0.0 {
        return Err(anyhow!(
            "V3_LIQUIDITY_EXHAUSTED: local quote produced no output"
        ));
    }

    Ok((
        amount_out_raw / 10f64.powi(output_decimals as i32),
        ticks_crossed,
    ))
}

fn input_capacity_state(state: &V3PoolState, token_in: &str, token_out: &str) -> Result<f64> {
    let cache = state
        .tick_cache
        .as_ref()
        .ok_or_else(|| anyhow!("V3_NO_CACHE: local tick cache unavailable"))?;
    let (zero_for_one, input_decimals, _) = direction_and_decimals(state, token_in, token_out)?;

    let fee_multiplier = 1.0 - state.def.fee_bps() / 10_000.0;
    if !(0.0..1.0).contains(&fee_multiplier) {
        return Err(anyhow!("V3_MATH: invalid V3 fee"));
    }

    let mut sqrt_price = state.sqrt_price_x96 / Q96;
    let mut liquidity = state.liquidity;
    let mut current_tick = state.tick;
    let mut gross_capacity_raw = 0.0f64;
    let max_steps = cache.ticks.len().saturating_add(8).max(12);

    for _ in 0..max_steps {
        if liquidity < 0.0 || !liquidity.is_finite() || sqrt_price <= 0.0 {
            return Err(anyhow!(
                "V3_MATH: invalid local liquidity/price while sizing"
            ));
        }

        let next = next_initialized_tick(cache, current_tick, zero_for_one);
        let (target_tick, liquidity_net, initialized) = if let Some(tick) = next {
            (tick.tick, tick.liquidity_net, true)
        } else {
            (
                if zero_for_one {
                    cache.min_tick
                } else {
                    cache.max_tick
                },
                0.0,
                false,
            )
        };
        let target_sqrt = sqrt_ratio_at_tick(target_tick);

        if relative_close(sqrt_price, target_sqrt, 1e-14) {
            if initialized {
                liquidity = apply_liquidity_net(liquidity, liquidity_net, zero_for_one)?;
                current_tick = if zero_for_one {
                    target_tick - 1
                } else {
                    target_tick
                };
                sqrt_price = target_sqrt;
                continue;
            }
            break;
        }

        if liquidity > 0.0 {
            let effective_needed = if zero_for_one {
                amount0_delta(liquidity, target_sqrt, sqrt_price)
            } else {
                amount1_delta(liquidity, sqrt_price, target_sqrt)
            };
            if !effective_needed.is_finite() || effective_needed < 0.0 {
                return Err(anyhow!("V3_MATH: invalid capacity step input"));
            }
            gross_capacity_raw += effective_needed / fee_multiplier;
        }

        sqrt_price = target_sqrt;
        if initialized {
            liquidity = apply_liquidity_net(liquidity, liquidity_net, zero_for_one)?;
            current_tick = if zero_for_one {
                target_tick - 1
            } else {
                target_tick
            };
        } else {
            break;
        }
    }

    if !gross_capacity_raw.is_finite() || gross_capacity_raw <= 0.0 {
        return Err(anyhow!(
            "V3_LIQUIDITY_EXHAUSTED: no input capacity in loaded direction"
        ));
    }

    Ok(gross_capacity_raw / 10f64.powi(input_decimals as i32))
}

fn next_initialized_tick<'a>(
    cache: &'a crate::types::V3TickCache,
    current_tick: i32,
    zero_for_one: bool,
) -> Option<&'a crate::types::V3Tick> {
    if zero_for_one {
        cache.ticks.iter().rev().find(|t| t.tick <= current_tick)
    } else {
        cache.ticks.iter().find(|t| t.tick > current_tick)
    }
}

fn apply_liquidity_net(liquidity: f64, liquidity_net: f64, zero_for_one: bool) -> Result<f64> {
    let delta = if zero_for_one {
        -liquidity_net
    } else {
        liquidity_net
    };
    let next = liquidity + delta;
    if !next.is_finite() {
        return Err(anyhow!("V3_MATH: non-finite liquidity while crossing tick"));
    }

    // uint128/int128 values can be ~1e20+, while the scanner deliberately uses
    // f64. An exact on-chain transition to zero can therefore appear as a small
    // negative absolute number after conversion. Clamp only tiny *relative*
    // round-off; a material negative value is still a real local-state error.
    let scale = liquidity.abs().max(liquidity_net.abs()).max(1.0);
    let roundoff = scale * 1e-12;
    if next < 0.0 && next.abs() <= roundoff {
        return Ok(0.0);
    }
    if next < 0.0 {
        return Err(anyhow!(
            "V3_LIQUIDITY_UNDERFLOW: liquidity became materially negative while crossing tick"
        ));
    }
    Ok(if next.abs() <= roundoff { 0.0 } else { next })
}

#[inline]
fn amount0_delta(liquidity: f64, sqrt_a: f64, sqrt_b: f64) -> f64 {
    let (lo, hi) = if sqrt_a <= sqrt_b {
        (sqrt_a, sqrt_b)
    } else {
        (sqrt_b, sqrt_a)
    };
    liquidity * (hi - lo) / (hi * lo)
}

#[inline]
fn amount1_delta(liquidity: f64, sqrt_a: f64, sqrt_b: f64) -> f64 {
    (liquidity * (sqrt_b - sqrt_a)).abs()
}

#[inline]
fn next_sqrt_from_amount0_in(current: f64, liquidity: f64, amount0_in: f64) -> f64 {
    liquidity * current / (liquidity + amount0_in * current)
}

pub fn sqrt_ratio_at_tick(tick: i32) -> f64 {
    let t = tick.clamp(MIN_TICK, MAX_TICK);
    1.0001_f64.powf(t as f64 / 2.0)
}

#[inline]
fn relative_close(a: f64, b: f64, eps: f64) -> bool {
    (a - b).abs() <= eps * a.abs().max(b.abs()).max(1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_zero_is_one() {
        assert!((sqrt_ratio_at_tick(0) - 1.0).abs() < 1e-15);
    }

    #[test]
    fn tick_prices_are_monotonic() {
        assert!(sqrt_ratio_at_tick(-100) < sqrt_ratio_at_tick(0));
        assert!(sqrt_ratio_at_tick(100) > sqrt_ratio_at_tick(0));
    }

    #[test]
    fn amount_deltas_are_positive() {
        let l = 1_000_000.0;
        let a = 0.99;
        let b = 1.01;
        assert!(amount0_delta(l, a, b) > 0.0);
        assert!(amount1_delta(l, a, b) > 0.0);
    }

    #[test]
    fn crossing_can_legitimately_leave_zero_liquidity() {
        let zero = apply_liquidity_net(100.0, -100.0, false).unwrap();
        assert_eq!(zero, 0.0);
        let active_again = apply_liquidity_net(zero, 250.0, false).unwrap();
        assert_eq!(active_again, 250.0);
    }
}
