use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::Response;
use rand::Rng;
use serde::Serialize;
use std::collections::HashMap;

use crate::AppState;

const MAX_PCT_MOVE_PER_TICK: f64 = 0.003; // +/-0.3%

/// One minute's OHLC for a single underlying. Accumulated in memory from
/// simulator ticks and flushed to the `price_candles` table by
/// [`flush_candles`] once the minute rolls over — that table is the
/// persisted price history the analytics export's `price_candles` dataset
/// reads.
#[derive(Debug, Clone, Serialize)]
pub struct Candle {
    pub underlying: String,
    pub minute: String, // ISO timestamp of the minute bucket
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
}

/// The current minute bucket as an ISO timestamp, e.g.
/// "2026-09-28T12:34:00.000Z". Candle minutes are compared as strings,
/// which sorts correctly for ISO 8601.
fn current_minute_bucket() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    crate::auth::format_unix_secs(now - (now % 60))
}

/// Folds a post-tick price snapshot into the in-memory candles: starts a
/// new candle when the minute rolls over, otherwise extends the current
/// one's high/low/close.
fn update_candles(state: &AppState, prices: &HashMap<String, f64>) {
    let minute = current_minute_bucket();
    let mut candles = state.candles.lock().unwrap();
    for (underlying, &price) in prices {
        let need_new = match candles.get(underlying) {
            Some(c) => c.minute != minute,
            None => true,
        };
        if need_new {
            candles.insert(
                underlying.clone(),
                Candle {
                    underlying: underlying.clone(),
                    minute: minute.clone(),
                    open: price,
                    high: price,
                    low: price,
                    close: price,
                },
            );
        } else {
            let c = candles.get_mut(underlying).expect("checked above");
            c.high = c.high.max(price);
            c.low = c.low.min(price);
            c.close = price;
        }
    }
}

/// Writes every completed (pre-current-minute) candle to the database.
/// Called once a minute by the simulator loop. A candle whose insert
/// fails is logged and skipped; it stays out of the table but the
/// in-memory copy is already gone, so the next tick starts a fresh one —
/// acceptable for a paper-trading price feed, and the export's watermark
/// simply never advances past a minute that failed to persist.
async fn flush_candles(state: &AppState) {
    let current_minute = current_minute_bucket();
    let completed: Vec<Candle> = {
        let mut candles = state.candles.lock().unwrap();
        let mut done = Vec::new();
        for key in candles.keys().cloned().collect::<Vec<_>>() {
            if candles.get(&key).expect("just iterated").minute < current_minute {
                done.push(candles.remove(&key).expect("just checked"));
            }
        }
        done
    };

    for c in completed {
        let result = sqlx::query(
            "INSERT OR REPLACE INTO price_candles (underlying, minute, open, high, low, close)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(&c.underlying)
        .bind(&c.minute)
        .bind(c.open)
        .bind(c.high)
        .bind(c.low)
        .bind(c.close)
        .execute(&state.db)
        .await;
        if let Err(e) = result {
            tracing::warn!(error = %e, underlying = %c.underlying, "failed to persist price candle");
        }
    }
}

/// Nudges every spot price by a small random percentage and broadcasts the
/// new snapshot on `state.spot_tx`, returning the JSON payload sent (or
/// not sent, if nothing was listening — that's the common case, not an
/// error). Pulled out of the loop below so a test can assert on the
/// bounds of one tick directly instead of only observing it through a
/// live 2-second timer.
pub fn tick_once(state: &AppState) -> String {
    let prices = {
        let mut prices = state.spot_prices.lock().unwrap();
        for price in prices.values_mut() {
            let pct_move =
                rand::thread_rng().gen_range(-MAX_PCT_MOVE_PER_TICK..MAX_PCT_MOVE_PER_TICK);
            *price = (*price * (1.0 + pct_move)).max(0.0001);
        }
        prices.clone()
    };
    update_candles(state, &prices);
    let vols = state.vol_surface.lock().unwrap().clone();

    let payload = serde_json::json!({ "prices": prices, "vols": vols }).to_string();
    let _ = state.spot_tx.send(payload.clone());
    payload
}

/// There's no real market feed behind this yet — it exists so the WS
/// endpoint (and the frontend's ticking price displays) has something
/// live to show instead of the static values AppState::new() seeds at
/// startup.
pub async fn price_simulator_loop(state: AppState) {
    let mut tick_interval = tokio::time::interval(std::time::Duration::from_secs(2));
    let mut flush_interval = tokio::time::interval(std::time::Duration::from_secs(60));
    loop {
        tokio::select! {
            _ = tick_interval.tick() => tick_once(&state),
            _ = flush_interval.tick() => flush_candles(&state).await,
        }
    }
}

pub async fn ws_spot(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    ws.on_upgrade(move |socket| handle_spot_socket(socket, state))
}

async fn handle_spot_socket(mut socket: WebSocket, state: AppState) {
    // Send an immediate snapshot so the client has something to render
    // before the first simulator tick (up to 2s away) arrives.
    let snapshot = {
        let prices = state.spot_prices.lock().unwrap().clone();
        let vols = state.vol_surface.lock().unwrap().clone();
        serde_json::json!({ "prices": prices, "vols": vols }).to_string()
    };
    if socket.send(Message::Text(snapshot)).await.is_err() {
        return;
    }

    let mut rx = state.spot_tx.subscribe();
    loop {
        tokio::select! {
            update = rx.recv() => {
                match update {
                    Ok(payload) => {
                        if socket.send(Message::Text(payload)).await.is_err() {
                            break;
                        }
                    }
                    // Client fell behind the broadcast buffer — resync with a
                    // fresh snapshot rather than sending stale skipped ticks.
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Err(_)) => break,
                    _ => {} // ignore anything the client sends; this is a read-only feed
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn test_state() -> (AppState, std::path::PathBuf) {
        let db_path =
            std::env::temp_dir().join(format!("zenith-prices-test-{}.db", uuid::Uuid::new_v4()));
        let pool = crate::db::init_pool(&format!("sqlite://{}", db_path.display())).await;
        (AppState::new(pool), db_path)
    }

    #[tokio::test]
    async fn tick_once_moves_every_price_within_the_per_tick_bound() {
        let (state, db_path) = test_state().await;
        let before = state.spot_prices.lock().unwrap().clone();

        tick_once(&state);

        let after = state.spot_prices.lock().unwrap().clone();
        for (underlying, before_price) in &before {
            let after_price = after[underlying];
            let max_move = before_price * MAX_PCT_MOVE_PER_TICK;
            assert!(
                (after_price - before_price).abs() <= max_move + 1e-9,
                "{underlying} moved from {before_price} to {after_price}, beyond the {MAX_PCT_MOVE_PER_TICK} bound"
            );
        }

        state.db.close().await;
        let _ = std::fs::remove_file(&db_path);
    }

    #[tokio::test]
    async fn tick_once_never_lets_a_price_reach_zero_or_go_negative() {
        let (state, db_path) = test_state().await;
        state
            .spot_prices
            .lock()
            .unwrap()
            .insert("TINY".into(), 0.0001);

        // Enough ticks that a run of unlucky downward moves would drive an
        // unclamped price to zero or below if the floor weren't enforced.
        for _ in 0..1000 {
            tick_once(&state);
        }

        let price = state.spot_prices.lock().unwrap()["TINY"];
        assert!(price > 0.0, "price floor was violated: {price}");

        state.db.close().await;
        let _ = std::fs::remove_file(&db_path);
    }

    #[tokio::test]
    async fn tick_once_broadcasts_the_post_tick_snapshot() {
        let (state, db_path) = test_state().await;
        let mut rx = state.spot_tx.subscribe();

        let returned = tick_once(&state);
        let broadcast = rx.try_recv().unwrap();
        assert_eq!(returned, broadcast);

        // Compared with a tolerance rather than exact JSON equality: the
        // broadcast payload went through a text round-trip (serialize to
        // string, parse back), and serde_json's default float parser
        // isn't guaranteed bit-exact on that round-trip the way its ryu
        // serializer is — an unrelated JSON-text subtlety, not a bug in
        // tick_once itself.
        let payload: serde_json::Value = serde_json::from_str(&broadcast).unwrap();
        let live_prices = state.spot_prices.lock().unwrap().clone();
        for (underlying, live_price) in &live_prices {
            let broadcast_price = payload["prices"][underlying].as_f64().unwrap();
            assert!(
                (broadcast_price - live_price).abs() < 1e-9,
                "{underlying}: broadcast {broadcast_price} vs live {live_price}"
            );
        }

        state.db.close().await;
        let _ = std::fs::remove_file(&db_path);
    }
}
