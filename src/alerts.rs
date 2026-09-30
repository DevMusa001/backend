use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Json;
use serde::Deserialize;

use crate::auth::AuthUser;
use crate::error::{db_error, AppError, AppJson};
use crate::models::Alert;
use crate::outbox::{self, AlertTriggeredPayload, DomainEvent, EVENT_VERSION};
use crate::AppState;

pub async fn get_alerts(
    State(state): State<AppState>,
    AuthUser(wallet_address): AuthUser,
) -> Result<Json<Vec<Alert>>, AppError> {
    // `id!` overrides the TEXT-PK nullable quirk; `triggered: bool` overrides
    // the INTEGER column to decode as the model's bool.
    let alerts: Vec<Alert> = sqlx::query_as!(
        Alert,
        "SELECT id AS \"id!\", wallet_address, underlying, condition, target_price, triggered AS \"triggered: bool\", created_at, triggered_at FROM alerts WHERE wallet_address = ? ORDER BY created_at DESC",
        &wallet_address
    )
    .fetch_all(&state.db)
    .await
    .map_err(|e| db_error("load alerts", e))?;

    Ok(Json(alerts))
}

#[derive(Deserialize)]
pub struct CreateAlertRequest {
    pub underlying: String,
    pub condition: String, // "above" | "below"
    pub target_price: f64,
}

pub async fn create_alert(
    State(state): State<AppState>,
    AuthUser(wallet_address): AuthUser,
    AppJson(req): AppJson<CreateAlertRequest>,
) -> Result<Json<Alert>, AppError> {
    if req.condition != "above" && req.condition != "below" {
        return Err(AppError::new(
            StatusCode::BAD_REQUEST,
            "condition must be \"above\" or \"below\"",
        ));
    }
    if req.target_price <= 0.0 {
        return Err(AppError::new(
            StatusCode::BAD_REQUEST,
            "target_price must be positive",
        ));
    }
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

    let id = uuid::Uuid::new_v4().to_string();
    sqlx::query!(
        "INSERT INTO alerts (id, wallet_address, underlying, condition, target_price) VALUES (?, ?, ?, ?, ?)",
        &id,
        &wallet_address,
        &req.underlying,
        &req.condition,
        req.target_price
    )
    .execute(&state.db)
    .await
    .map_err(|e| db_error("create alert", e))?;

    let alert: Alert = sqlx::query_as!(
        Alert,
        "SELECT id AS \"id!\", wallet_address, underlying, condition, target_price, triggered AS \"triggered: bool\", created_at, triggered_at FROM alerts WHERE id = ?",
        &id
    )
    .fetch_one(&state.db)
    .await
    .map_err(|e| db_error("load the alert just created", e))?;

    Ok(Json(alert))
}

pub async fn delete_alert(
    State(state): State<AppState>,
    AuthUser(wallet_address): AuthUser,
    Path(id): Path<String>,
) -> Result<StatusCode, AppError> {
    let result = sqlx::query!("DELETE FROM alerts WHERE id = ? AND wallet_address = ?", &id, &wallet_address)
        .execute(&state.db)
        .await
        .map_err(|e| db_error("delete alert", e))?;

    if result.rows_affected() == 0 {
        return Err(AppError::new(
            StatusCode::NOT_FOUND,
            "no alert with that id for this wallet",
        ));
    }

    Ok(StatusCode::NO_CONTENT)
}

/// Checks every untriggered alert against the current spot price for its
/// underlying and flips `triggered` once the condition is met. Returns the
/// total number of alerts fired across every underlying this pass —
/// pulled out of the loop below so a test can assert on it directly
/// instead of only through log lines on a live 10-second timer.
///
/// Each trigger and its `AlertTriggered` outbox event commit atomically:
/// the UPDATE ... RETURNING yields the ids of the rows that fired, and the
/// events are written into the outbox inside the same transaction, so an
/// alert whose event can't be recorded isn't left marked triggered (it
/// simply retries on the next pass).
pub async fn check_once(state: &AppState) -> u64 {
    let prices = state.spot_prices.lock().unwrap().clone();

    let mut tx = match state.db.begin().await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::warn!(error = %e, "alert check failed to begin transaction");
            return 0;
        }
    };

    let mut total_fired = 0;
    for (underlying, spot) in prices {
        let fired: Result<Vec<(String, String, String, String, f64, String)>, _> =
            sqlx::query_as(
                "UPDATE alerts
                    SET triggered = 1, triggered_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                 WHERE underlying = ? AND triggered = 0
                   AND ((condition = 'above' AND target_price <= ?)
                     OR (condition = 'below' AND target_price >= ?))
                 RETURNING id, wallet_address, underlying, condition, target_price, triggered_at",
            )
            .bind(&underlying)
            .bind(spot)
            .bind(spot)
            .fetch_all(&mut *tx)
            .await;

        match fired {
            Ok(rows) => {
                for (id, wallet_address, underlying, condition, target_price, triggered_at) in rows {
                    total_fired += 1;
                    tracing::info!(
                        underlying,
                        spot,
                        "alert triggered"
                    );
                    let event = DomainEvent::AlertTriggered(AlertTriggeredPayload {
                        version: EVENT_VERSION,
                        alert_id: id,
                        wallet_address,
                        underlying,
                        condition,
                        target_price,
                        triggered_at,
                    });
                    if let Err(e) = outbox::emit(&mut tx, &event).await {
                        let _ = tx.rollback().await;
                        tracing::warn!(error = %e, "failed to emit alert_triggered event");
                        return 0;
                    }
                }
            }
            Err(e) => {
                let _ = tx.rollback().await;
                tracing::warn!(error = %e, "alert check failed");
                return 0;
            }
        }
    }

    if let Err(e) = tx.commit().await {
        tracing::warn!(error = %e, "alert check failed to commit");
        return 0;
    }
    total_fired
}

/// Alerts stay in the table (and visible via GET) after triggering — they
/// just stop being re-checked — rather than being deleted, so the frontend
/// can show "this alert fired" instead of it silently vanishing.
pub async fn check_alerts_loop(state: AppState) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
    loop {
        interval.tick().await;
        check_once(&state).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn test_state() -> (AppState, std::path::PathBuf) {
        let db_path =
            std::env::temp_dir().join(format!("zenith-alerts-test-{}.db", uuid::Uuid::new_v4()));
        let pool = crate::db::init_pool(&format!("sqlite://{}", db_path.display())).await;
        (AppState::new(pool), db_path)
    }

    async fn insert_alert(
        state: &AppState,
        id: &str,
        underlying: &str,
        condition: &str,
        target: f64,
    ) {
        sqlx::query(
            "INSERT INTO accounts (wallet_address) VALUES ('GTEST') ON CONFLICT DO NOTHING",
        )
        .execute(&state.db)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO alerts (id, wallet_address, underlying, condition, target_price)
             VALUES (?, 'GTEST', ?, ?, ?)",
        )
        .bind(id)
        .bind(underlying)
        .bind(condition)
        .bind(target)
        .execute(&state.db)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn check_once_fires_an_alert_whose_condition_is_met() {
        let (state, db_path) = test_state().await;
        state
            .spot_prices
            .lock()
            .unwrap()
            .insert("BTC".into(), 70_000.0);
        insert_alert(&state, "a1", "BTC", "above", 65_000.0).await;

        let fired = check_once(&state).await;
        assert_eq!(fired, 1);

        let triggered: bool = sqlx::query_scalar("SELECT triggered FROM alerts WHERE id = 'a1'")
            .fetch_one(&state.db)
            .await
            .unwrap();
        assert!(triggered);

        state.db.close().await;
        let _ = std::fs::remove_file(&db_path);
    }

    #[tokio::test]
    async fn check_once_leaves_an_unmet_alert_untouched() {
        let (state, db_path) = test_state().await;
        state
            .spot_prices
            .lock()
            .unwrap()
            .insert("BTC".into(), 50_000.0);
        insert_alert(&state, "a1", "BTC", "above", 65_000.0).await;

        let fired = check_once(&state).await;
        assert_eq!(fired, 0);

        let triggered: bool = sqlx::query_scalar("SELECT triggered FROM alerts WHERE id = 'a1'")
            .fetch_one(&state.db)
            .await
            .unwrap();
        assert!(!triggered);

        state.db.close().await;
        let _ = std::fs::remove_file(&db_path);
    }

    #[tokio::test]
    async fn check_once_does_not_re_fire_an_already_triggered_alert() {
        let (state, db_path) = test_state().await;
        state
            .spot_prices
            .lock()
            .unwrap()
            .insert("BTC".into(), 70_000.0);
        insert_alert(&state, "a1", "BTC", "above", 65_000.0).await;

        assert_eq!(check_once(&state).await, 1);
        // Price stays well past the trigger; a second pass must not count
        // it again now that it's already triggered.
        assert_eq!(check_once(&state).await, 0);

        state.db.close().await;
        let _ = std::fs::remove_file(&db_path);
    }
}
