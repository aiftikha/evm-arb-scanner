use crate::types::{eq_addr, PoolState};

pub fn amount_out(amount_in: f64, reserve_in: f64, reserve_out: f64, fee_bps: f64) -> f64 {
    if amount_in <= 0.0 || reserve_in <= 0.0 || reserve_out <= 0.0 {
        return 0.0;
    }
    let fee_multiplier = 1.0 - fee_bps / 10_000.0;
    let effective_in = amount_in * fee_multiplier;
    reserve_out * effective_in / (reserve_in + effective_in)
}

pub fn v2_leg_output(
    amount_in: f64,
    pool: &PoolState,
    token_in: &str,
    token_out: &str,
) -> Option<f64> {
    let PoolState::V2(s) = pool else {
        return None;
    };
    let (reserve_in, reserve_out) =
        if eq_addr(token_in, &s.def.token0) && eq_addr(token_out, &s.def.token1) {
            (s.reserve0, s.reserve1)
        } else if eq_addr(token_in, &s.def.token1) && eq_addr(token_out, &s.def.token0) {
            (s.reserve1, s.reserve0)
        } else {
            return None;
        };
    Some(amount_out(
        amount_in,
        reserve_in,
        reserve_out,
        s.def.fee_bps(),
    ))
}

pub fn v2_round_trip_output(
    amount_in: f64,
    first: &PoolState,
    second: &PoolState,
    start_token: &str,
    mid_token: &str,
) -> Option<f64> {
    let mid = v2_leg_output(amount_in, first, start_token, mid_token)?;
    v2_leg_output(mid, second, mid_token, start_token)
}

pub fn golden_section_max<F>(mut lo: f64, mut hi: f64, iterations: usize, f: F) -> (f64, f64)
where
    F: Fn(f64) -> f64,
{
    const PHI: f64 = 1.618_033_988_749_895;
    let mut c = hi - (hi - lo) / PHI;
    let mut d = lo + (hi - lo) / PHI;
    let mut fc = f(c);
    let mut fd = f(d);

    for _ in 0..iterations {
        if fc > fd {
            hi = d;
            d = c;
            fd = fc;
            c = hi - (hi - lo) / PHI;
            fc = f(c);
        } else {
            lo = c;
            c = d;
            fc = fd;
            d = lo + (hi - lo) / PHI;
            fd = f(d);
        }
    }

    let x = (lo + hi) / 2.0;
    (x, f(x))
}

pub fn geometric_grid(lo: f64, hi: f64, points: usize) -> Vec<f64> {
    if points <= 1 || hi <= lo {
        return vec![lo];
    }
    let ratio = (hi / lo).powf(1.0 / (points - 1) as f64);
    (0..points)
        .map(|i| {
            if i + 1 == points {
                hi
            } else {
                lo * ratio.powi(i as i32)
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constant_product_output_is_positive() {
        let out = amount_out(1_000.0, 1_000_000.0, 500_000.0, 30.0);
        assert!(out > 0.0);
        assert!(out < 500.0);
    }

    #[test]
    fn optimizer_finds_parabola_peak() {
        let (x, y) = golden_section_max(0.0, 10.0, 80, |x| -(x - 3.0).powi(2) + 9.0);
        assert!((x - 3.0).abs() < 1e-5);
        assert!((y - 9.0).abs() < 1e-5);
    }

    #[test]
    fn geometric_grid_hits_bounds() {
        let xs = geometric_grid(10.0, 10_000.0, 4);
        assert_eq!(xs.len(), 4);
        assert!((xs[0] - 10.0).abs() < 1e-9);
        assert!((xs[3] - 10_000.0).abs() < 1e-9);
    }
}
