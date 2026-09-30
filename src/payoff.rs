//! Combined multi-leg payoff math, ported from the frontend's
//! `lib/payoff.ts`. Pure functions over caller-supplied legs — no
//! pricing or persistence here, just the P&L arithmetic.

use axum::http::StatusCode;
use axum::response::Json;
use serde::{Deserialize, Serialize};

use crate::error::AppError;

#[derive(Debug, Clone, Deserialize)]
pub struct PricedLeg {
    pub option_type: String,   // "call" | "put"
    pub position_type: String, // "long" | "short" (long == frontend's "buy")
    pub strike: f64,
    pub contracts: f64,
    pub premium: f64,
}

/// Net P&L across all legs at a given spot price at expiry.
pub fn combined_pnl(legs: &[PricedLeg], spot_at_expiry: f64) -> f64 {
    legs.iter().fold(0.0, |total, leg| {
        let intrinsic = if leg.option_type == "call" {
            (spot_at_expiry - leg.strike).max(0.0)
        } else {
            (leg.strike - spot_at_expiry).max(0.0)
        };
        let per_contract = if leg.position_type == "long" {
            intrinsic - leg.premium
        } else {
            leg.premium - intrinsic
        };
        total + per_contract * leg.contracts
    })
}

#[derive(Debug, Serialize)]
pub struct PayoffPoint {
    pub spot: f64,
    pub pnl: f64,
}

/// Series of {spot, pnl} points across a spot range, for charting the
/// combined curve.
pub fn combined_payoff_series(
    legs: &[PricedLeg],
    lo_spot: f64,
    hi_spot: f64,
    steps: u32,
) -> Vec<PayoffPoint> {
    let range = hi_spot - lo_spot;
    (0..=steps)
        .map(|i| {
            let spot = lo_spot + (range * i as f64) / steps as f64;
            PayoffPoint {
                spot,
                pnl: combined_pnl(legs, spot),
            }
        })
        .collect()
}

/// Positive = net debit paid to enter; negative = net credit received.
pub fn net_premium(legs: &[PricedLeg]) -> f64 {
    legs.iter().fold(0.0, |total, leg| {
        let signed = if leg.position_type == "long" {
            leg.premium
        } else {
            -leg.premium
        };
        total + signed * leg.contracts
    })
}

/// Find the spot prices where the combined P&L crosses zero, by
/// root-finding on `combined_pnl`. Scans a bracket around the strikes
/// and bisects each sign change, so callers (e.g. the strategy builder)
/// get breakevens without reimplementing the search.
pub fn breakevens(legs: &[PricedLeg]) -> Vec<f64> {
    if legs.is_empty() {
        return Vec::new();
    }
    let min_strike = legs
        .iter()
        .map(|l| l.strike)
        .fold(f64::INFINITY, f64::min);
    let max_strike = legs
        .iter()
        .map(|l| l.strike)
        .fold(f64::NEG_INFINITY, f64::max);
    // Bracket wide enough to capture breakevens outside the strike range
    // (e.g. a short strangle's upside breakeven sits above the call strike).
    let span = (max_strike - min_strike).max(1.0);
    let lo = min_strike - span;
    let hi = max_strike + span;
    let steps = 2000u32;

    let mut roots = Vec::new();
    let mut prev_spot = lo;
    let mut prev_pnl = combined_pnl(legs, lo);
    for i in 1..=steps {
        let spot = lo + (hi - lo) * i as f64 / steps as f64;
        let pnl = combined_pnl(legs, spot);
        if prev_pnl == 0.0 {
            roots.push(prev_spot);
        } else if (prev_pnl < 0.0) != (pnl < 0.0) {
            // Bisect the sign change for a tight root estimate.
            let (mut a, mut b) = (prev_spot, spot);
            let (mut fa, _) = (prev_pnl, pnl);
            for _ in 0..60 {
                let mid = 0.5 * (a + b);
                let fm = combined_pnl(legs, mid);
                if (fa < 0.0) != (fm < 0.0) {
                    b = mid;
                } else {
                    a = mid;
                    fa = fm;
                }
            }
            roots.push(0.5 * (a + b));
        }
        prev_spot = spot;
        prev_pnl = pnl;
    }
    roots
}

#[derive(Deserialize)]
pub struct PayoffRequest {
    pub legs: Vec<PricedLeg>,
    pub lo_spot: f64,
    pub hi_spot: f64,
    #[serde(default = "default_steps")]
    pub steps: u32,
}

fn default_steps() -> u32 {
    200
}

#[derive(Serialize)]
pub struct PayoffResponse {
    pub points: Vec<PayoffPoint>,
    pub net_premium: f64,
}

/// Stateless P&L math over caller-supplied legs (no pricing lookup, no
/// auth) — the frontend's strategy builder already has each leg's
/// premium from a prior /api/v1/price call before it needs this.
pub async fn post_payoff(Json(req): Json<PayoffRequest>) -> Result<Json<PayoffResponse>, AppError> {
    if req.legs.is_empty() {
        return Err(AppError::new(
            StatusCode::BAD_REQUEST,
            "legs must not be empty",
        ));
    }
    if req.hi_spot <= req.lo_spot {
        return Err(AppError::new(
            StatusCode::BAD_REQUEST,
            "hi_spot must be greater than lo_spot",
        ));
    }
    if req.steps == 0 {
        return Err(AppError::new(
            StatusCode::BAD_REQUEST,
            "steps must be positive",
        ));
    }

    let points = combined_payoff_series(&req.legs, req.lo_spot, req.hi_spot, req.steps);
    let net_premium = net_premium(&req.legs);
    Ok(Json(PayoffResponse {
        points,
        net_premium,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leg(
        option_type: &str,
        position_type: &str,
        strike: f64,
        contracts: f64,
        premium: f64,
    ) -> PricedLeg {
        PricedLeg {
            option_type: option_type.to_string(),
            position_type: position_type.to_string(),
            strike,
            contracts,
            premium,
        }
    }

    #[test]
    fn long_straddle_is_symmetric_around_strike() {
        // Long call + long put, same strike/premium: PnL at strike-d and
        // strike+d should be identical by symmetry.
        let legs = vec![
            leg("call", "long", 100.0, 1.0, 5.0),
            leg("put", "long", 100.0, 1.0, 5.0),
        ];
        let below = combined_pnl(&legs, 90.0);
        let above = combined_pnl(&legs, 110.0);
        assert!((below - above).abs() < 1e-9);
        // Max loss at expiry, exactly at the strike, is the total premium paid.
        assert_eq!(combined_pnl(&legs, 100.0), -10.0);
    }

    #[test]
    fn net_premium_nets_long_and_short_legs() {
        let legs = vec![
            leg("call", "long", 100.0, 1.0, 5.0),
            leg("call", "short", 110.0, 1.0, 2.0),
        ];
        // Debit of 5 paid, credit of 2 received -> net debit of 3.
        assert_eq!(net_premium(&legs), 3.0);
    }

    #[test]
    fn series_endpoints_match_direct_calls() {
        let legs = vec![leg("put", "short", 100.0, 2.0, 4.0)];
        let series = combined_payoff_series(&legs, 80.0, 120.0, 4);
        assert_eq!(series.len(), 5);
        assert_eq!(series.first().unwrap().pnl, combined_pnl(&legs, 80.0));
        assert_eq!(series.last().unwrap().pnl, combined_pnl(&legs, 120.0));
    }

    #[test]
    fn breakevens_of_long_straddle_are_strike_plus_minus_premium() {
        let legs = vec![
            leg("call", "long", 100.0, 1.0, 5.0),
            leg("put", "long", 100.0, 1.0, 5.0),
        ];
        let mut roots = breakevens(&legs);
        roots.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(roots.len(), 2);
        assert!((roots[0] - 90.0).abs() < 1e-3);
        assert!((roots[1] - 110.0).abs() < 1e-3);
    }

    #[test]
    fn breakevens_of_short_strangle_are_outside_strikes() {
        let legs = vec![
            leg("put", "short", 95.0, 1.0, 3.0),
            leg("call", "short", 105.0, 1.0, 3.0),
        ];
        let mut roots = breakevens(&legs);
        roots.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(roots.len(), 2);
        assert!((roots[0] - 89.0).abs() < 1e-3);
        assert!((roots[1] - 111.0).abs() < 1e-3);
    }

    #[test]
    fn breakevens_empty_for_always_profitable_structure() {
        // A long call bought below intrinsic value is profitable at every
        // expiry spot, so there is no breakeven.
        let legs = vec![leg("call", "long", 100.0, 1.0, 0.0)];
        assert!(breakevens(&legs).is_empty());
    }
}
