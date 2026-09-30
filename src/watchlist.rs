use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Json;
use serde::Deserialize;

use crate::auth::AuthUser;
use crate::error::{db_error, AppError, AppJson};
use crate::models::WatchlistItem;
use crate::AppState;

pub async fn get_watchlist(
    State(state): State<AppState>,
    AuthUser(wallet_address): AuthUser,
) -> Result<Json<Vec<WatchlistItem>>, AppError> {
    // `wallet_address!`/`underlying!` override the composite-PK nullable quirk.
    let items: Vec<WatchlistItem> = sqlx::query_as!(
        WatchlistItem,
        "SELECT wallet_address AS \"wallet_address!\", underlying AS \"underlying!\", added_at FROM watchlist WHERE wallet_address = ? ORDER BY added_at DESC",
        &wallet_address
    )
    .fetch_all(&state.db)
    .await
    .map_err(|e| db_error("load watchlist", e))?;

    Ok(Json(items))
}

#[derive(Deserialize)]
pub struct AddWatchlistRequest {
    pub underlying: String,
}

pub async fn add_watchlist(
    State(state): State<AppState>,
    AuthUser(wallet_address): AuthUser,
    AppJson(req): AppJson<AddWatchlistRequest>,
) -> Result<StatusCode, AppError> {
    if !state
        .spot_prices
        .lock()
        .unwrap()
        .contains_key(&req.underlying)
    {
        return Err(AppError::new(
            StatusCode::NOT_FOUND,
            format!("unknown underlying \"{}\"", req.underlying),
        ));
    }

    sqlx::query!(
        "INSERT INTO watchlist (wallet_address, underlying) VALUES (?, ?) ON CONFLICT(wallet_address, underlying) DO NOTHING",
        &wallet_address,
        &req.underlying
    )
    .execute(&state.db)
    .await
    .map_err(|e| db_error("add watchlist item", e))?;

    Ok(StatusCode::CREATED)
}

pub async fn remove_watchlist(
    State(state): State<AppState>,
    AuthUser(wallet_address): AuthUser,
    Path(underlying): Path<String>,
) -> Result<StatusCode, AppError> {
    let result = sqlx::query!(
        "DELETE FROM watchlist WHERE wallet_address = ? AND underlying = ?",
        &wallet_address,
        &underlying
    )
    .execute(&state.db)
    .await
    .map_err(|e| db_error("remove watchlist item", e))?;

    if result.rows_affected() == 0 {
        return Err(AppError::new(
            StatusCode::NOT_FOUND,
            format!("\"{underlying}\" is not on this wallet's watchlist"),
        ));
    }

    Ok(StatusCode::NO_CONTENT)
}
