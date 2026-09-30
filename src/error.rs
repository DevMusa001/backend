use axum::extract::rejection::{JsonRejection, QueryRejection};
use axum::extract::{FromRequest, FromRequestParts, Query, Request};
use axum::http::request::Parts;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use serde::de::DeserializeOwned;
use serde_json::json;

/// A JSON-bodied error instead of the empty-body `StatusCode` rejections
/// every handler was returning — a client currently has to infer "why"
/// from the status code alone (was that 400 a bad option_type or a
/// non-positive strike?). Existing handlers keep working unchanged since
/// this converts `From<StatusCode>`; converting them to attach a real
/// message is a per-module follow-up, not required to introduce the type.
#[derive(Debug)]
pub struct AppError {
    pub status: StatusCode,
    pub message: String,
    /// Stable, machine-readable error code (e.g. `"series_expired"`,
    /// `"budget_exceeded"`). `None` for generic errors that predate the
    /// simulation preflight; the preflight always sets it so integrators
    /// can branch on the code instead of parsing the human message.
    pub code: Option<String>,
    /// Optional remediation hint explaining how to fix the failure before
    /// re-submitting (e.g. "restore the archived ledger entry").
    pub hint: Option<String>,
}

impl AppError {
    pub fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
            code: None,
            hint: None,
        }
    }

    /// Builds an error carrying a stable API error code and an optional
    /// remediation hint, used by the transaction simulation preflight to
    /// translate opaque host/contract errors into actionable responses.
    pub fn coded(
        status: StatusCode,
        code: impl Into<String>,
        message: impl Into<String>,
        hint: Option<String>,
    ) -> Self {
        Self {
            status,
            message: message.into(),
            code: Some(code.into()),
            hint,
        }
    }
}

impl From<StatusCode> for AppError {
    fn from(status: StatusCode) -> Self {
        let message = status.canonical_reason().unwrap_or("error").to_string();
        Self {
            status,
            message,
            code: None,
            hint: None,
        }
    }
}

/// Typed failure modes for the implied-volatility solver. Each variant maps
/// to a distinct 422 message so `/api/v1/iv` can tell a client *why* a solve
/// failed instead of collapsing every failure into one opaque error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IvError {
    /// The supplied price is at or below the option's intrinsic value, so no
    /// positive implied volatility can reproduce it (IV = 0 limit).
    BelowIntrinsic,
    /// The supplied price exceeds the no-arbitrage upper bound for the given
    /// forward/strike/expiry, so no finite volatility can reproduce it.
    AboveUpperBound,
    /// The price is inside the no-arbitrage bounds but the solver failed to
    /// reach the target tolerance within its iteration budget.
    NoConvergence,
    /// A structurally invalid input: non-finite price/strike/expiry, T <= 0,
    /// or a non-positive strike.
    InvalidInput,
}

impl IvError {
    /// The client-facing message for this failure mode. Kept distinct per
    /// variant so the 422 body is actionable.
    pub fn message(&self) -> &'static str {
        match self {
            IvError::BelowIntrinsic => {
                "price is at or below intrinsic value; implied volatility is undefined"
            }
            IvError::AboveUpperBound => {
                "price exceeds the no-arbitrage upper bound for these parameters"
            }
            IvError::NoConvergence => {
                "implied volatility solver failed to converge for this price"
            }
            IvError::InvalidInput => {
                "invalid input: price, strike and expiry must be finite and positive"
            }
        }
    }
}

impl std::fmt::Display for IvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}

impl std::error::Error for IvError {}

impl From<IvError> for AppError {
    fn from(err: IvError) -> Self {
        AppError::new(StatusCode::UNPROCESSABLE_ENTITY, err.message())
    }
}

/// Turns an unexpected sqlx::Error into a 500 whose client-facing message
/// names WHICH operation failed ("failed to load alerts") without leaking
/// the raw database error (table/column names, SQL fragments) into the
/// response body — the raw error still goes to the logs via tracing, for
/// whoever's actually debugging it.
pub fn db_error(context: &str, e: sqlx::Error) -> AppError {
    tracing::error!(error = %e, context, "database operation failed");
    AppError::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("failed to {context}"),
    )
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let mut body = json!({ "error": self.message });
        if let Some(code) = self.code {
            body["code"] = json!(code);
        }
        if let Some(hint) = self.hint {
            body["hint"] = json!(hint);
        }
        (self.status, Json(body)).into_response()
    }
}

impl From<QueryRejection> for AppError {
    fn from(rejection: QueryRejection) -> Self {
        AppError::new(rejection.status(), rejection.body_text())
    }
}

/// Drop-in replacement for `axum::extract::Query<T>` whose rejection is a
/// JSON `{"error": "..."}` body via AppError, instead of axum's default
/// plain-text rejection — the only place in this API a malformed request
/// broke the JSON-error-body contract every other handler upholds. Every
/// `Query<T>` extractor in the app should use this instead.
pub struct AppQuery<T>(pub T);

#[axum::async_trait]
impl<S, T> FromRequestParts<S> for AppQuery<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = AppError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let Query(value) = Query::<T>::from_request_parts(parts, state).await?;
        Ok(AppQuery(value))
    }
}

impl From<JsonRejection> for AppError {
    fn from(rejection: JsonRejection) -> Self {
        AppError::new(rejection.status(), rejection.body_text())
    }
}

/// Drop-in replacement for `axum::extract::Json<T>` whose rejection is a
/// JSON `{"error": "..."}` body via AppError, instead of axum's default
/// plain-text rejection (e.g. "Failed to deserialize the JSON body into
/// the target type: ..."). Every `Json<T>` request-body extractor in the
/// app should use this instead.
pub struct AppJson<T>(pub T);

#[axum::async_trait]
impl<S, T> FromRequest<S> for AppJson<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = AppError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let Json(value) = Json::<T>::from_request(req, state).await?;
        Ok(AppJson(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn db_error_names_the_operation_without_leaking_the_raw_sqlx_error() {
        let raw = sqlx::Error::RowNotFound;
        let app_err = db_error("load account", raw);

        assert_eq!(app_err.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(app_err.message, "failed to load account");
        // The whole point: the client-facing message must not be a
        // passthrough of sqlx's own Display text ("no rows returned").
        assert!(!app_err.message.contains("row"));
    }

    #[test]
    fn app_error_from_status_code_uses_the_canonical_reason() {
        let app_err: AppError = StatusCode::NOT_FOUND.into();
        assert_eq!(app_err.message, "Not Found");
        assert!(app_err.code.is_none());
        assert!(app_err.hint.is_none());
    }

    #[test]
    fn coded_error_carries_code_and_hint() {
        let app_err = AppError::coded(
            StatusCode::BAD_REQUEST,
            "series_expired",
            "series has expired",
            Some("choose an active series".to_string()),
        );
        assert_eq!(app_err.code.as_deref(), Some("series_expired"));
        assert_eq!(app_err.hint.as_deref(), Some("choose an active series"));
    }

    #[test]
    fn iv_error_maps_to_422_with_distinct_messages() {
        let cases = [
            IvError::BelowIntrinsic,
            IvError::AboveUpperBound,
            IvError::NoConvergence,
            IvError::InvalidInput,
        ];

        let mut messages = std::collections::HashSet::new();
        for err in cases {
            let app_err: AppError = err.into();
            assert_eq!(app_err.status, StatusCode::UNPROCESSABLE_ENTITY);
            assert!(messages.insert(app_err.message), "messages must be distinct");
        }
    }
}
