use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use serde::Serialize;
use sqlx::FromRow;
use std::fmt;
use std::ops::{Add, Sub};

/// Number of decimal places used for the fixed-point database representation.
/// Matches Stellar's stroop precision (1e-7).
pub const MONEY_SCALE: u32 = 7;

/// Fixed-point monetary amount backed by `rust_decimal::Decimal`.
///
/// Display uses banker's rounding; fees and collateral use
/// [`Money::round_against_user`] which always rounds in the protocol's favour.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct Money(Decimal);

impl Money {
    pub const ZERO: Money = Money(Decimal::ZERO);

    pub fn new(value: Decimal) -> Self {
        Money(value)
    }

    /// Explicit conversion boundary from the `f64` pricing math.
    /// NaN and Inf are rejected.
    pub fn from_price(price: f64) -> Result<Self, MoneyError> {
        if !price.is_finite() {
            return Err(MoneyError::NonFinite(price));
        }
        Decimal::from_f64(price)
            .map(Money)
            .ok_or(MoneyError::NonFinite(price))
    }

    pub fn to_f64(&self) -> f64 {
        self.0.to_f64().unwrap_or(0.0)
    }

    pub fn as_decimal(&self) -> Decimal {
        self.0
    }

    /// Banker's rounding (round-half-to-even) for display purposes.
    pub fn round_display(&self, dp: u32) -> Self {
        Money(self.0.round_dp_with_strategy(dp, RoundingStrategy::MidpointNearestEven))
    }

    /// Round against the user: fees and collateral always round up in the
    /// protocol's favour, regardless of sign.
    pub fn round_against_user(&self, dp: u32) -> Self {
        Money(self.0.round_dp_with_strategy(dp, RoundingStrategy::ToPositiveInfinity))
    }

    /// Scale to the integer representation stored in the database (10^7).
    pub fn to_scaled_i64(&self) -> Result<i64, MoneyError> {
        let scaled = self
            .0
            .checked_mul(Decimal::from(10u64.pow(MONEY_SCALE)))
            .ok_or(MoneyError::Overflow)?;
        scaled
            .round_dp_with_strategy(0, RoundingStrategy::MidpointNearestEven)
            .to_i64()
            .ok_or(MoneyError::Overflow)
    }

    /// Reconstruct from the scaled integer representation stored in the database.
    pub fn from_scaled_i64(scaled: i64) -> Self {
        Money(Decimal::from(scaled) / Decimal::from(10u64.pow(MONEY_SCALE)))
    }
}

impl fmt::Display for Money {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.round_display(MONEY_SCALE).0)
    }
}

impl Add for Money {
    type Output = Money;
    fn add(self, rhs: Money) -> Money {
        Money(self.0 + rhs.0)
    }
}

impl Sub for Money {
    type Output = Money;
    fn sub(self, rhs: Money) -> Money {
        Money(self.0 - rhs.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MoneyError {
    NonFinite(f64),
    Overflow,
}

impl fmt::Display for MoneyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MoneyError::NonFinite(v) => write!(f, "non-finite monetary value: {v}"),
            MoneyError::Overflow => write!(f, "monetary overflow"),
        }
    }
}

impl std::error::Error for MoneyError {}

/// Fixed-point contract quantity, same representation as [`Money`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct Qty(Decimal);

impl Qty {
    pub const ZERO: Qty = Qty(Decimal::ZERO);

    pub fn new(value: Decimal) -> Self {
        Qty(value)
    }

    pub fn from_price(price: f64) -> Result<Self, MoneyError> {
        if !price.is_finite() {
            return Err(MoneyError::NonFinite(price));
        }
        Decimal::from_f64(price)
            .map(Qty)
            .ok_or(MoneyError::NonFinite(price))
    }

    pub fn to_f64(&self) -> f64 {
        self.0.to_f64().unwrap_or(0.0)
    }

    pub fn as_decimal(&self) -> Decimal {
        self.0
    }

    pub fn to_scaled_i64(&self) -> Result<i64, MoneyError> {
        let scaled = self
            .0
            .checked_mul(Decimal::from(10u64.pow(MONEY_SCALE)))
            .ok_or(MoneyError::Overflow)?;
        scaled
            .round_dp_with_strategy(0, RoundingStrategy::MidpointNearestEven)
            .to_i64()
            .ok_or(MoneyError::Overflow)
    }

    pub fn from_scaled_i64(scaled: i64) -> Self {
        Qty(Decimal::from(scaled) / Decimal::from(10u64.pow(MONEY_SCALE)))
    }
}

impl fmt::Display for Qty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.round_dp_with_strategy(MONEY_SCALE, RoundingStrategy::MidpointNearestEven))
    }
}

// --- sqlx integration: store as INTEGER scaled by 10^7 ---------------------

impl sqlx::Type<sqlx::Sqlite> for Money {
    fn type_info() -> sqlx::sqlite::SqliteTypeInfo {
        <i64 as sqlx::Type<sqlx::Sqlite>>::type_info()
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Sqlite> for Money {
    fn decode(value: sqlx::sqlite::SqliteValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let scaled = <i64 as sqlx::Decode<sqlx::Sqlite>>::decode(value)?;
        Ok(Money::from_scaled_i64(scaled))
    }
}

impl<'q> sqlx::Encode<'q, sqlx::Sqlite> for Money {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::sqlite::SqliteArgumentValue<'q>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        let scaled = self.to_scaled_i64()?;
        <i64 as sqlx::Encode<sqlx::Sqlite>>::encode(scaled, buf)
    }
}

impl sqlx::Type<sqlx::Sqlite> for Qty {
    fn type_info() -> sqlx::sqlite::SqliteTypeInfo {
        <i64 as sqlx::Type<sqlx::Sqlite>>::type_info()
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Sqlite> for Qty {
    fn decode(value: sqlx::sqlite::SqliteValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let scaled = <i64 as sqlx::Decode<sqlx::Sqlite>>::decode(value)?;
        Ok(Qty::from_scaled_i64(scaled))
    }
}

impl<'q> sqlx::Encode<'q, sqlx::Sqlite> for Qty {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::sqlite::SqliteArgumentValue<'q>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        let scaled = self.to_scaled_i64()?;
        <i64 as sqlx::Encode<sqlx::Sqlite>>::encode(scaled, buf)
    }
}

#[derive(Debug, Clone, FromRow, Serialize)]
pub struct Account {
    pub wallet_address: String,
    pub balance: Money,
    pub collateral_locked: Money,
    pub created_at: String,
}

#[derive(Debug, Clone, FromRow, Serialize)]
pub struct Position {
    pub id: String,
    pub wallet_address: String,
    pub underlying: String,
    pub strike: Money,
    /// Deprecated: retained for backwards compatibility. Use `expires_at`
    /// (absolute ISO-8601 UTC timestamp) to derive time-to-expiry.
    pub expiry_days: f64,
    /// Absolute expiry timestamp in ISO-8601 UTC, backfilled as
    /// `opened_at + expiry_days`. Time-to-expiry is derived from this and the
    /// current clock, enabling real theta decay.
    pub expires_at: String,
    pub option_type: String,
    pub position_type: String,
    pub contracts: Qty,
    pub entry_premium: Money,
    pub entry_spot: Money,
    pub collateral: Money,
    pub status: String,
    pub close_premium: Option<Money>,
    pub close_spot: Option<Money>,
    pub realized_pnl: Option<Money>,
    pub opened_at: String,
    pub closed_at: Option<String>,
    pub strategy_id: Option<String>,
}

#[derive(Debug, Clone, FromRow, Serialize)]
pub struct WatchlistItem {
    pub wallet_address: String,
    pub underlying: String,
    pub added_at: String,
}

#[derive(Debug, Clone, FromRow, Serialize)]
pub struct Alert {
    pub id: String,
    pub wallet_address: String,
    pub underlying: String,
    pub condition: String,
    pub target_price: Money,
    pub triggered: bool,
    pub created_at: String,
    pub triggered_at: Option<String>,
}

/// Health state of an account with respect to its margin requirements.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountHealth {
    /// Equity is at or above the initial margin requirement.
    Healthy,
    /// Equity is between the maintenance and initial margin requirements.
    MarginCall,
    /// Equity has fallen below the maintenance margin requirement.
    Liquidating,
}

impl AccountHealth {
    /// Classifies an account given its equity and margin requirements.
    ///
    /// `initial_margin` is the requirement above which the account is healthy;
    /// `maintenance_margin` is the floor below which liquidation is triggered.
    pub fn classify(equity: f64, initial_margin: f64, maintenance_margin: f64) -> Self {
        if equity < maintenance_margin {
            AccountHealth::Liquidating
        } else if equity < initial_margin {
            AccountHealth::MarginCall
        } else {
            AccountHealth::Healthy
        }
    }
}

/// A single liquidation event recorded for an under-margined account.
#[derive(Debug, Clone, FromRow, Serialize)]
pub struct Liquidation {
    pub id: String,
    pub wallet_address: String,
    /// JSON-encoded list of the position ids closed in this liquidation.
    pub positions_closed: String,
    /// JSON-encoded list of the prices used to close each position.
    pub prices: String,
    /// Liquidation penalty charged, in the account's balance units.
    pub penalty: f64,
    /// Account equity before the liquidation ran.
    pub pre_equity: f64,
    /// Account equity after the liquidation ran.
    pub post_equity: f64,
    /// Health state before the liquidation ran.
    pub pre_health: String,
    /// Health state after the liquidation ran.
    pub post_health: String,
    /// Bad debt absorbed by the insurance fund, if equity went negative.
    pub bad_debt: f64,
    pub created_at: String,
}
