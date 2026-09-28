use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::Json;
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, Transaction};

use crate::auth::AuthUser;
use crate::collateral::collateral_required;
use crate::error::{db_error, AppError, AppJson, AppQuery};
use crate::models::{Account, Position};
use crate::readmodels::{apply_position_closed, apply_position_opened};
use crate::{black_scholes, smile_vol, AppState, BSInputs, BSResult};

pub async fn get_account(
    State(state): State<AppState>,
    AuthUser(wallet_address): AuthUser,
) -> Result<Json<Account>, AppError> {
    // Verify/login already creates this row, but stay defensive in case a
    // session outlives some future account-deletion path.
    sqlx::query(
        "INSERT INTO accounts (wallet_address) VALUES (?) ON CONFLICT(wallet_address) DO NOTHING",
    )
    .bind(&wallet_address)
    .execute(&state.db)
    .await
    .map_err(|e| db_error("create or confirm account", e))?;

    let account: Account = sqlx::query_as("SELECT * FROM accounts WHERE wallet_address = ?")
        .bind(&wallet_address)
        .fetch_one(&state.db)
        .await
        .map_err(|e| db_error("load account", e))?;

    Ok(Json(account))
}

pub const DEFAULT_LIST_LIMIT: i64 = 50;
pub const MAX_LIST_LIMIT: i64 = 200;

#[derive(Deserialize)]
pub struct ListPositionsQuery {
    /// "open" | "closed" | "rolled" — omit to return every status.
    pub status: Option<String>,
    /// Restrict to the legs of one multi-leg strategy — omit for everything.
    pub strategy_id: Option<String>,
    /// Defaults to DEFAULT_LIST_LIMIT, capped at MAX_LIST_LIMIT regardless
    /// of what the caller asks for.
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

/// Response shape is still a bare JSON array (unchanged, since the
/// frontend already consumes it that way) — total count and whether more
/// pages exist ride along as `X-Total-Count`/`X-Has-More` response
/// headers instead, the same pattern GitHub's API uses for exactly this
/// reason: it lets pagination metadata arrive without breaking existing
/// callers that expect the body to just be the list.
pub async fn list_positions(
    State(state): State<AppState>,
    AuthUser(wallet_address): AuthUser,
    AppQuery(q): AppQuery<ListPositionsQuery>,
) -> Result<(HeaderMap, Json<Vec<Position>>), AppError> {
    let limit = q
        .limit
        .unwrap_or(DEFAULT_LIST_LIMIT)
        .clamp(1, MAX_LIST_LIMIT);
    let offset = q.offset.unwrap_or(0).max(0);

    // `? IS NULL OR column = ?` lets one query handle all four
    // status/strategy_id filter combinations without branching SQL.
    let positions: Vec<Position> = sqlx::query_as(
        "SELECT * FROM positions
            WHERE wallet_address = ?
              AND (? IS NULL OR status = ?)
              AND (? IS NULL OR strategy_id = ?)
         ORDER BY opened_at DESC
         LIMIT ? OFFSET ?",
    )
    .bind(&wallet_address)
    .bind(&q.status)
    .bind(&q.status)
    .bind(&q.strategy_id)
    .bind(&q.strategy_id)
    .bind(limit)
    .bind(offset)
    .fetch_all(&state.db)
    .await
    .map_err(|e| db_error("list positions", e))?;

    let total: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM positions
            WHERE wallet_address = ?
              AND (? IS NULL OR status = ?)
              AND (? IS NULL OR strategy_id = ?)",
    )
    .bind(&wallet_address)
    .bind(&q.status)
    .bind(&q.status)
    .bind(&q.strategy_id)
    .bind(&q.strategy_id)
    .fetch_one(&state.db)
    .await
    .map_err(|e| db_error("count positions", e))?;

    let has_more = offset + (positions.len() as i64) < total;
    let mut headers = HeaderMap::new();
    headers.insert("x-total-count", HeaderValue::from(total));
    headers.insert(
        "x-has-more",
        HeaderValue::from_static(if has_more { "true" } else { "false" }),
    );

    Ok((headers, Json(positions)))
}

#[derive(Deserialize)]
pub struct OpenPositionRequest {
    pub underlying: String,
    pub strike: f64,
    pub expiry_days: f64,
    pub option_type: String,   // "call" | "put"
    pub position_type: String, // "long" | "short"
    pub contracts: f64,
}

/// Prices and inserts a new position, debiting/crediting the account and
/// locking collateral as needed, all within the caller's transaction.
/// Shared by the open handler and (once it exists) the roll handler, so
/// rolling a position doesn't need to duplicate this logic.
pub(crate) async fn open_position_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    state: &AppState,
    wallet_address: &str,
    req: &OpenPositionRequest,
    strategy_id: Option<&str>,
) -> Result<Position, AppError> {
    if req.contracts <= 0.0 || req.strike <= 0.0 || req.expiry_days <= 0.0 {
        return Err(AppError::new(
            StatusCode::BAD_REQUEST,
            "contracts, strike, and expiry_days must all be positive",
        ));
    }
    if req.option_type != "call" && req.option_type != "put" {
        return Err(AppError::new(
            StatusCode::BAD_REQUEST,
            "option_type must be \"call\" or \"put\"",
        ));
    }
    if req.position_type != "long" && req.position_type != "short" {
        return Err(AppError::new(
            StatusCode::BAD_REQUEST,
            "position_type must be \"long\" or \"short\"",
        ));
    }

    let (spot, base_vol) = {
        let prices = state.spot_prices.lock().unwrap();
        let vols = state.vol_surface.lock().unwrap();
        let not_found = || {
            AppError::new(
                StatusCode::NOT_FOUND,
                format!("unknown underlying \"{}\"", req.underlying),
            )
        };
        let spot = *prices.get(&req.underlying).ok_or_else(not_found)?;
        let vol = *vols.get(&req.underlying).ok_or_else(not_found)?;
        (spot, vol)
    };

    let vol = smile_vol(base_vol, req.strike / spot);
    let t = req.expiry_days / 365.0;
    let is_call = req.option_type == "call";
    let entry_premium = black_scholes(&BSInputs {
        spot,
        strike: req.strike,
        vol,
        t,
        r: 0.05,
        is_call,
    })
    .premium;

    let is_short = req.position_type == "short";
    let collateral = if is_short {
        collateral_required(&req.option_type, req.contracts, req.strike, spot)
    } else {
        0.0
    };
    let cash_delta = if is_short {
        entry_premium * req.contracts // premium received
    } else {
        -entry_premium * req.contracts // premium paid
    };

    let account: Account = sqlx::query_as("SELECT * FROM accounts WHERE wallet_address = ?")
        .bind(wallet_address)
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| db_error("load account", e))?;

    let new_balance = account.balance + cash_delta;
    let new_collateral_locked = account.collateral_locked + collateral;
    // Available buying power must stay non-negative: cash on hand minus
    // whatever's locked as collateral (across all positions, not just
    // this one) must cover this trade's premium debit/collateral.
    if new_balance - new_collateral_locked < 0.0 {
        return Err(AppError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "insufficient buying power: this trade's premium/collateral would exceed balance minus locked collateral",
        ));
    }

    sqlx::query("UPDATE accounts SET balance = ?, collateral_locked = ? WHERE wallet_address = ?")
        .bind(new_balance)
        .bind(new_collateral_locked)
        .bind(wallet_address)
        .execute(&mut **tx)
        .await
        .map_err(|e| db_error("update account balance", e))?;

    let id = uuid::Uuid::new_v4().to_string();
    sqlx::query(
        "INSERT INTO positions
            (id, wallet_address, underlying, strike, expiry_days, option_type,
             position_type, contracts, entry_premium, entry_spot, collateral, status, strategy_id)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?)",
    )
    .bind(&id)
    .bind(wallet_address)
    .bind(&req.underlying)
    .bind(req.strike)
    .bind(req.expiry_days)
    .bind(&req.option_type)
    .bind(&req.position_type)
    .bind(req.contracts)
    .bind(entry_premium)
    .bind(spot)
    .bind(collateral)
    .bind(strategy_id)
    .execute(&mut **tx)
    .await
    .map_err(|e| db_error("insert position", e))?;

    let position: Position = sqlx::query_as("SELECT * FROM positions WHERE id = ?")
        .bind(&id)
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| db_error("load the position just opened", e))?;

    // Read-model maintenance runs inside the same transaction as the
    // insert, so the CQRS summaries can never miss an open (or record one
    // that later rolls back).
    apply_position_opened(&mut **tx, &position)
        .await
        .map_err(|e| db_error("update strategy read model after open", e))?;

    Ok(position)
}

pub async fn open_position(
    State(state): State<AppState>,
    AuthUser(wallet_address): AuthUser,
    AppJson(req): AppJson<OpenPositionRequest>,
) -> Result<Json<Position>, AppError> {
    let mut tx = state
        .db
        .begin()
        .await
        .map_err(|e| db_error("begin open-position transaction", e))?;
    let position = open_position_in_tx(&mut tx, &state, &wallet_address, &req, None).await?;
    tx.commit()
        .await
        .map_err(|e| db_error("commit open-position transaction", e))?;
    Ok(Json(position))
}

/// Reprices an open position at the current spot/vol and settles it:
/// releases any locked collateral, applies the closing cash flow to the
/// account, and marks the row closed. Shared by the close and roll
/// handlers so both settle a position the same way.
///
/// Simplification: this reprices with the *same* time-to-expiry the
/// position was opened with rather than tracking real elapsed time
/// against an absolute expiry timestamp — fine for a paper-trading demo,
/// but not a real theta decay model.
pub(crate) async fn close_position_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    state: &AppState,
    wallet_address: &str,
    position_id: &str,
) -> Result<Position, AppError> {
    let position: Position = sqlx::query_as(
        "SELECT * FROM positions WHERE id = ? AND wallet_address = ? AND status = 'open'",
    )
    .bind(position_id)
    .bind(wallet_address)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|e| db_error("look up position", e))?
    .ok_or_else(|| {
        AppError::new(
            StatusCode::NOT_FOUND,
            "no open position with that id for this wallet",
        )
    })?;

    let (spot, base_vol) = {
        let prices = state.spot_prices.lock().unwrap();
        let vols = state.vol_surface.lock().unwrap();
        let not_found = || {
            AppError::new(
                StatusCode::NOT_FOUND,
                format!("unknown underlying \"{}\"", position.underlying),
            )
        };
        let spot = *prices.get(&position.underlying).ok_or_else(not_found)?;
        let vol = *vols.get(&position.underlying).ok_or_else(not_found)?;
        (spot, vol)
    };

    let vol = smile_vol(base_vol, position.strike / spot);
    let t = position.expiry_days / 365.0;
    let is_call = position.option_type == "call";
    let close_premium = black_scholes(&BSInputs {
        spot,
        strike: position.strike,
        vol,
        t,
        r: 0.05,
        is_call,
    })
    .premium;

    let is_short = position.position_type == "short";
    let realized_pnl = if is_short {
        (position.entry_premium - close_premium) * position.contracts
    } else {
        (close_premium - position.entry_premium) * position.contracts
    };
    let cash_delta = if is_short {
        -close_premium * position.contracts // buy to close
    } else {
        close_premium * position.contracts // sell to close
    };

    let account: Account = sqlx::query_as("SELECT * FROM accounts WHERE wallet_address = ?")
        .bind(wallet_address)
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| db_error("load account", e))?;

    let new_balance = account.balance + cash_delta;
    let new_collateral_locked = account.collateral_locked - position.collateral;

    sqlx::query("UPDATE accounts SET balance = ?, collateral_locked = ? WHERE wallet_address = ?")
        .bind(new_balance)
        .bind(new_collateral_locked)
        .bind(wallet_address)
        .execute(&mut **tx)
        .await
        .map_err(|e| db_error("update account balance", e))?;

    sqlx::query(
        "UPDATE positions
            SET status = 'closed', close_premium = ?, close_spot = ?, realized_pnl = ?,
                closed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
         WHERE id = ?",
    )
    .bind(close_premium)
    .bind(spot)
    .bind(realized_pnl)
    .bind(position_id)
    .execute(&mut **tx)
    .await
    .map_err(|e| db_error("mark position closed", e))?;

    let closed: Position = sqlx::query_as("SELECT * FROM positions WHERE id = ?")
        .bind(position_id)
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| db_error("load the position just closed", e))?;

    // Same transaction as the status update: the realized P&L and trade
    // counts land in the read models atomically with the close itself.
    apply_position_closed(&mut **tx, &closed)
        .await
        .map_err(|e| db_error("update position read models after close", e))?;

    Ok(closed)
}

pub async fn close_position(
    State(state): State<AppState>,
    AuthUser(wallet_address): AuthUser,
    Path(id): Path<String>,
) -> Result<Json<Position>, AppError> {
    let mut tx = state
        .db
        .begin()
        .await
        .map_err(|e| db_error("begin close-position transaction", e))?;
    let closed = close_position_in_tx(&mut tx, &state, &wallet_address, &id).await?;
    tx.commit()
        .await
        .map_err(|e| db_error("commit close-position transaction", e))?;
    Ok(Json(closed))
}

#[derive(Deserialize)]
pub struct RollPositionRequest {
    pub new_strike: f64,
    pub new_expiry_days: f64,
}

#[derive(serde::Serialize)]
pub struct RollResult {
    pub closed: Position,
    pub opened: Position,
}

/// Closes the given position and immediately opens its replacement (same
/// underlying/option_type/position_type/contracts, new strike and expiry)
/// as one atomic transaction — either both happen or neither does.
pub async fn roll_position(
    State(state): State<AppState>,
    AuthUser(wallet_address): AuthUser,
    Path(id): Path<String>,
    AppJson(req): AppJson<RollPositionRequest>,
) -> Result<Json<RollResult>, AppError> {
    let mut tx = state
        .db
        .begin()
        .await
        .map_err(|e| db_error("begin roll transaction", e))?;

    let mut closed = close_position_in_tx(&mut tx, &state, &wallet_address, &id).await?;
    // close_position_in_tx always marks the row 'closed'; a roll is
    // specifically a close-and-reopen, so relabel it 'rolled' to keep
    // /api/v1/history's ledger distinguishable from a plain close.
    sqlx::query("UPDATE positions SET status = 'rolled' WHERE id = ?")
        .bind(&closed.id)
        .execute(&mut *tx)
        .await
        .map_err(|e| db_error("mark position rolled", e))?;
    closed.status = "rolled".to_string();

    let open_req = OpenPositionRequest {
        underlying: closed.underlying.clone(),
        strike: req.new_strike,
        expiry_days: req.new_expiry_days,
        option_type: closed.option_type.clone(),
        position_type: closed.position_type.clone(),
        contracts: closed.contracts,
    };
    // Preserve strategy grouping across a roll: the replacement leg
    // belongs to the same multi-leg strategy as the one it replaced.
    let opened = open_position_in_tx(
        &mut tx,
        &state,
        &wallet_address,
        &open_req,
        closed.strategy_id.as_deref(),
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| db_error("commit roll transaction", e))?;
    Ok(Json(RollResult { closed, opened }))
}

#[derive(Serialize, Default)]
pub struct AggregateGreeks {
    pub delta: f64,
    pub gamma: f64,
    pub theta: f64,
    pub vega: f64,
}

/// Reprices a position at today's spot/vol (not the entry-time values
/// stored on the row). `None` if the underlying has been delisted since
/// the position was opened — shared by portfolio greeks and strategy
/// unrealized-P&L, both of which need to skip that case the same way.
pub(crate) fn current_bs_result(state: &AppState, p: &Position) -> Option<BSResult> {
    let (spot, base_vol) = {
        let prices = state.spot_prices.lock().unwrap();
        let vols = state.vol_surface.lock().unwrap();
        match (prices.get(&p.underlying), vols.get(&p.underlying)) {
            (Some(&s), Some(&v)) => (s, v),
            _ => return None,
        }
    };

    let vol = smile_vol(base_vol, p.strike / spot);
    let t = p.expiry_days / 365.0;
    let is_call = p.option_type == "call";
    Some(black_scholes(&BSInputs {
        spot,
        strike: p.strike,
        vol,
        t,
        r: 0.05,
        is_call,
    }))
}

/// Sums each open position's current Greeks, flipping sign for short
/// positions — ported from the frontend's aggregateGreeks().
pub async fn get_portfolio_greeks(
    State(state): State<AppState>,
    AuthUser(wallet_address): AuthUser,
) -> Result<Json<AggregateGreeks>, AppError> {
    let open_positions: Vec<Position> =
        sqlx::query_as("SELECT * FROM positions WHERE wallet_address = ? AND status = 'open'")
            .bind(&wallet_address)
            .fetch_all(&state.db)
            .await
            .map_err(|e| db_error("load open positions for greeks", e))?;

    let mut totals = AggregateGreeks::default();
    for p in &open_positions {
        let Some(result) = current_bs_result(&state, p) else {
            continue; // underlying delisted since this position was opened
        };

        let sign = if p.position_type == "short" {
            -1.0
        } else {
            1.0
        };
        totals.delta += sign * result.delta * p.contracts;
        totals.gamma += sign * result.gamma * p.contracts;
        totals.theta += sign * result.theta * p.contracts;
        totals.vega += sign * result.vega * p.contracts;
    }

    Ok(Json(totals))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Not reachable through the HTTP API at all (nothing lets a caller
    /// delist an underlying), so this calls get_portfolio_greeks directly
    /// as a plain function rather than through TestApp/the router — the
    /// only way to actually exercise the `continue` branch this test is
    /// aimed at.
    #[tokio::test]
    async fn get_portfolio_greeks_skips_a_position_in_a_delisted_underlying() {
        let db_path =
            std::env::temp_dir().join(format!("zenith-positions-test-{}.db", uuid::Uuid::new_v4()));
        let pool = crate::db::init_pool(&format!("sqlite://{}", db_path.display())).await;
        let state = AppState::new(pool);

        sqlx::query("INSERT INTO accounts (wallet_address) VALUES ('GTEST')")
            .execute(&state.db)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO positions
                (id, wallet_address, underlying, strike, expiry_days, option_type,
                 position_type, contracts, entry_premium, entry_spot, status)
             VALUES ('p1', 'GTEST', 'RETIRED', 100, 30, 'call', 'long', 1, 5, 100, 'open')",
        )
        .execute(&state.db)
        .await
        .unwrap();

        // Never listed in spot_prices/vol_surface at all — same situation
        // as an underlying that existed when the position opened and was
        // delisted since.
        assert!(!state.spot_prices.lock().unwrap().contains_key("RETIRED"));

        let greeks = get_portfolio_greeks(State(state.clone()), AuthUser("GTEST".into()))
            .await
            .unwrap()
            .0;
        assert_eq!(greeks.delta, 0.0);
        assert_eq!(greeks.gamma, 0.0);
        assert_eq!(greeks.theta, 0.0);
        assert_eq!(greeks.vega, 0.0);

        state.db.close().await;
        let _ = std::fs::remove_file(&db_path);
    }
}
