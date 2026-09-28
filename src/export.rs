//! Analytics export pipeline: incremental, partitioned Parquet exports for
//! the data warehouse.
//!
//! `zenith-admin export run` (see `src/admin.rs`) executes one export run:
//! for each dataset it reads the high-water mark from `export_watermarks`,
//! queries rows newer than that mark, writes them to a Parquet file under
//! `<dir>/<table>/<date>/part-<run_id>.parquet`, and — only after every
//! file has been written — commits the advanced watermarks and a manifest.
//! A run that crashes before the commit re-exports its rows on the next run
//! instead of silently skipping them.
//!
//! PII minimisation: wallet addresses are SHA-256 hashed with a salt by
//! default. Raw addresses are exported only when `EXPORT_RAW_WALLETS=true`
//! is set explicitly.
//!
//! Schema evolution is additive: each dataset has a `SCHEMA_VERSION`, and
//! new columns are appended as nullable fields at the end of the schema.
//! Existing Parquet files stay readable because Arrow readers tolerate
//! missing trailing columns.

use arrow::array::{Float64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use sha2::{Digest, Sha256};
use sqlx::sqlite::SqliteRow;
use sqlx::{Row, SqlitePool};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Salt used when `EXPORT_WALLET_SALT` is unset. Hashing still happens by
/// default (the PII-minimisation default); this default only means the salt
/// isn't deployment-specific. Set `EXPORT_WALLET_SALT` to a per-deployment
/// secret so hashes can't be correlated across deployments.
const DEFAULT_WALLET_SALT: &str = "zenith-analytics-export-v1";

/// Schema version for every dataset. Bump when a dataset's Arrow schema
/// gains a column (additively, as a nullable trailing field).
const SCHEMA_VERSION: u32 = 1;

// ─── Config ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ExportConfig {
    pub dir: PathBuf,
    pub wallet_salt: String,
    pub raw_wallets: bool,
}

impl ExportConfig {
    pub fn from_env() -> Self {
        let dir = std::env::var("EXPORT_DIR")
            .unwrap_or_else(|_| "./exports".to_string())
            .into();
        let wallet_salt = std::env::var("EXPORT_WALLET_SALT")
            .unwrap_or_else(|_| DEFAULT_WALLET_SALT.to_string());
        let raw_wallets = std::env::var("EXPORT_RAW_WALLETS")
            .unwrap_or_else(|_| "false".to_string())
            .eq_ignore_ascii_case("true");
        Self {
            dir,
            wallet_salt,
            raw_wallets,
        }
    }

    /// The wallet address as it should appear in the export: hashed with
    /// the salt by default, raw only when explicitly configured.
    fn wallet_address(&self, address: &str) -> String {
        if self.raw_wallets {
            return address.to_string();
        }
        let mut hasher = Sha256::new();
        hasher.update(self.wallet_salt.as_bytes());
        hasher.update(address.as_bytes());
        data_encoding::HEXLOWER.encode(&hasher.finalize())
    }
}

// ─── Errors ──────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum ExportError {
    Sqlx(sqlx::Error),
    Io(std::io::Error),
    Arrow(arrow::error::ArrowError),
    Parquet(parquet::errors::ParquetError),
}

impl std::fmt::Display for ExportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExportError::Sqlx(e) => write!(f, "export database error: {e}"),
            ExportError::Io(e) => write!(f, "export io error: {e}"),
            ExportError::Arrow(e) => write!(f, "export arrow error: {e}"),
            ExportError::Parquet(e) => write!(f, "export parquet error: {e}"),
        }
    }
}

impl std::error::Error for ExportError {}

impl From<sqlx::Error> for ExportError {
    fn from(e: sqlx::Error) -> Self {
        ExportError::Sqlx(e)
    }
}
impl From<std::io::Error> for ExportError {
    fn from(e: std::io::Error) -> Self {
        ExportError::Io(e)
    }
}
impl From<arrow::error::ArrowError> for ExportError {
    fn from(e: arrow::error::ArrowError) -> Self {
        ExportError::Arrow(e)
    }
}
impl From<parquet::errors::ParquetError> for ExportError {
    fn from(e: parquet::errors::ParquetError) -> Self {
        ExportError::Parquet(e)
    }
}

// ─── Manifest ────────────────────────────────────────────────────────────────

#[derive(Debug, serde::Serialize)]
pub struct ExportManifest {
    pub run_id: String,
    pub started_at: String,
    pub finished_at: String,
    pub datasets: Vec<DatasetManifest>,
}

#[derive(Debug, serde::Serialize)]
pub struct DatasetManifest {
    pub name: String,
    pub schema_version: u32,
    pub watermark_before: String,
    pub watermark_after: String,
    pub rows: u64,
    pub files: Vec<String>,
}

// ─── Watermarks ──────────────────────────────────────────────────────────────

/// The watermark for a dataset, or None before it has ever been exported.
async fn read_watermark(db: &SqlitePool, dataset: &str) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar("SELECT watermark FROM export_watermarks WHERE dataset = ?")
        .bind(dataset)
        .fetch_optional(db)
        .await
}

// ─── Arrow helpers ───────────────────────────────────────────────────────────

fn field(name: &str, data_type: DataType, nullable: bool) -> Field {
    Field::new(name, data_type, nullable)
}

/// Writes one record batch to a Parquet file, creating parent directories.
fn write_parquet(
    path: &Path,
    schema: &Arc<Schema>,
    batch: &RecordBatch,
) -> Result<(), ExportError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = std::fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None)?;
    writer.write(batch)?;
    writer.close()?;
    Ok(())
}

/// The path of a dataset's file in this run, relative to the export dir,
/// e.g. "positions/2026-09-28/part-<run_id>.parquet". Kept relative so the
/// manifest is portable across export directories.
fn dataset_file_rel(name: &str, run_date: &str, run_id: &str) -> String {
    Path::new(name)
        .join(run_date)
        .join(format!("part-{run_id}.parquet"))
        .to_string_lossy()
        .into_owned()
}

// ─── Dataset: positions ──────────────────────────────────────────────────────

fn positions_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        field("id", DataType::Utf8, false),
        field("wallet_address", DataType::Utf8, false),
        field("underlying", DataType::Utf8, false),
        field("strike", DataType::Float64, false),
        field("expiry_days", DataType::Float64, false),
        field("option_type", DataType::Utf8, false),
        field("position_type", DataType::Utf8, false),
        field("contracts", DataType::Float64, false),
        field("entry_premium", DataType::Float64, false),
        field("entry_spot", DataType::Float64, false),
        field("collateral", DataType::Float64, false),
        field("status", DataType::Utf8, false),
        field("close_premium", DataType::Float64, true),
        field("close_spot", DataType::Float64, true),
        field("realized_pnl", DataType::Float64, true),
        field("opened_at", DataType::Utf8, false),
        field("closed_at", DataType::Utf8, true),
        field("strategy_id", DataType::Utf8, true),
        field("updated_at", DataType::Utf8, false),
    ]))
}

async fn export_positions(
    db: &SqlitePool,
    config: &ExportConfig,
    watermark: &str,
    run_date: &str,
    run_id: &str,
) -> Result<DatasetManifest, ExportError> {
    let rows: Vec<SqliteRow> = sqlx::query(
        "SELECT * FROM positions
          WHERE updated_at >= ?
         ORDER BY updated_at, id",
    )
    .bind(watermark)
    .fetch_all(db)
    .await?;

    let before = watermark.to_string();
    let mut new_watermark = before.clone();
    let mut ids = Vec::new();
    let mut wallets = Vec::new();
    let mut underlyings = Vec::new();
    let mut strikes = Vec::new();
    let mut expiry_days = Vec::new();
    let mut option_types = Vec::new();
    let mut position_types = Vec::new();
    let mut contracts = Vec::new();
    let mut entry_premiums = Vec::new();
    let mut entry_spots = Vec::new();
    let mut collaterals = Vec::new();
    let mut statuses = Vec::new();
    let mut close_premiums = Vec::new();
    let mut close_spots = Vec::new();
    let mut realized_pnls = Vec::new();
    let mut opened_ats = Vec::new();
    let mut closed_ats = Vec::new();
    let mut strategy_ids = Vec::new();
    let mut updated_ats = Vec::new();

    for row in &rows {
        let updated_at = row.get::<String, _>("updated_at");
        new_watermark = updated_at.clone();
        ids.push(row.get::<String, _>("id"));
        wallets.push(config.wallet_address(&row.get::<String, _>("wallet_address")));
        underlyings.push(row.get::<String, _>("underlying"));
        strikes.push(row.get::<f64, _>("strike"));
        expiry_days.push(row.get::<f64, _>("expiry_days"));
        option_types.push(row.get::<String, _>("option_type"));
        position_types.push(row.get::<String, _>("position_type"));
        contracts.push(row.get::<f64, _>("contracts"));
        entry_premiums.push(row.get::<f64, _>("entry_premium"));
        entry_spots.push(row.get::<f64, _>("entry_spot"));
        collaterals.push(row.get::<f64, _>("collateral"));
        statuses.push(row.get::<String, _>("status"));
        close_premiums.push(row.get::<Option<f64>, _>("close_premium"));
        close_spots.push(row.get::<Option<f64>, _>("close_spot"));
        realized_pnls.push(row.get::<Option<f64>, _>("realized_pnl"));
        opened_ats.push(row.get::<String, _>("opened_at"));
        closed_ats.push(row.get::<Option<String>, _>("closed_at"));
        strategy_ids.push(row.get::<Option<String>, _>("strategy_id"));
        updated_ats.push(updated_at);
    }

    let rows = rows.len() as u64;
    let mut files = Vec::new();
    if rows > 0 {
        let batch = RecordBatch::try_new(
            positions_schema(),
            vec![
                Arc::new(StringArray::from(ids)),
                Arc::new(StringArray::from(wallets)),
                Arc::new(StringArray::from(underlyings)),
                Arc::new(Float64Array::from(strikes)),
                Arc::new(Float64Array::from(expiry_days)),
                Arc::new(StringArray::from(option_types)),
                Arc::new(StringArray::from(position_types)),
                Arc::new(Float64Array::from(contracts)),
                Arc::new(Float64Array::from(entry_premiums)),
                Arc::new(Float64Array::from(entry_spots)),
                Arc::new(Float64Array::from(collaterals)),
                Arc::new(StringArray::from(statuses)),
                Arc::new(Float64Array::from(close_premiums)),
                Arc::new(Float64Array::from(close_spots)),
                Arc::new(Float64Array::from(realized_pnls)),
                Arc::new(StringArray::from(opened_ats)),
                Arc::new(StringArray::from(closed_ats)),
                Arc::new(StringArray::from(strategy_ids)),
                Arc::new(StringArray::from(updated_ats)),
            ],
        )?;
        let rel = dataset_file_rel("positions", run_date, run_id);
        write_parquet(&config.dir.join(&rel), &positions_schema(), &batch)?;
        files.push(rel);
    }

    Ok(DatasetManifest {
        name: "positions".into(),
        schema_version: SCHEMA_VERSION,
        watermark_before: before,
        watermark_after: new_watermark,
        rows,
        files,
    })
}

// ─── Dataset: ledger_entries ─────────────────────────────────────────────────

fn ledger_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        field("entry_id", DataType::Utf8, false),
        field("wallet_address", DataType::Utf8, false),
        field("position_id", DataType::Utf8, false),
        field("kind", DataType::Utf8, false), // "open" | "close"
        field("underlying", DataType::Utf8, false),
        field("option_type", DataType::Utf8, false),
        field("position_type", DataType::Utf8, false),
        field("contracts", DataType::Float64, false),
        field("premium", DataType::Float64, false),
        field("spot", DataType::Float64, false),
        field("realized_pnl", DataType::Float64, true),
        field("created_at", DataType::Utf8, false),
    ]))
}

/// The positions table doubles as the trade ledger (a position's status
/// transition records everything a ledger entry needs), so this dataset is
/// derived: every position yields an `open` entry, and a closed/rolled
/// position yields a `close` entry too. Entry ids are deterministic
/// (`<position_id>:open` / `:close`) so a re-exported position regenerates
/// the same entries and the warehouse can dedupe by primary key.
async fn export_ledger_entries(
    db: &SqlitePool,
    config: &ExportConfig,
    watermark: &str,
    run_date: &str,
    run_id: &str,
) -> Result<DatasetManifest, ExportError> {
    let rows: Vec<SqliteRow> = sqlx::query(
        "SELECT * FROM positions
          WHERE updated_at >= ?
         ORDER BY updated_at, id",
    )
    .bind(watermark)
    .fetch_all(db)
    .await?;

    let before = watermark.to_string();
    let mut new_watermark = before.clone();
    let mut entry_ids = Vec::new();
    let mut wallets = Vec::new();
    let mut position_ids = Vec::new();
    let mut kinds = Vec::new();
    let mut underlyings = Vec::new();
    let mut option_types = Vec::new();
    let mut position_types = Vec::new();
    let mut contracts = Vec::new();
    let mut premiums = Vec::new();
    let mut spots = Vec::new();
    let mut realized_pnls = Vec::new();
    let mut created_ats = Vec::new();

    for row in &rows {
        let id = row.get::<String, _>("id");
        let wallet = config.wallet_address(&row.get::<String, _>("wallet_address"));
        let underlying = row.get::<String, _>("underlying");
        let option_type = row.get::<String, _>("option_type");
        let position_type = row.get::<String, _>("position_type");
        let contracts_val = row.get::<f64, _>("contracts");
        let status = row.get::<String, _>("status");
        new_watermark = row.get::<String, _>("updated_at");

        // open entry
        entry_ids.push(format!("{id}:open"));
        wallets.push(wallet.clone());
        position_ids.push(id.clone());
        kinds.push("open".to_string());
        underlyings.push(underlying.clone());
        option_types.push(option_type.clone());
        position_types.push(position_type.clone());
        contracts.push(contracts_val);
        premiums.push(row.get::<f64, _>("entry_premium"));
        spots.push(row.get::<f64, _>("entry_spot"));
        realized_pnls.push(None);
        created_ats.push(row.get::<String, _>("opened_at"));

        // close entry
        if status == "closed" || status == "rolled" {
            entry_ids.push(format!("{id}:close"));
            wallets.push(wallet);
            position_ids.push(id);
            kinds.push("close".to_string());
            underlyings.push(underlying);
            option_types.push(option_type);
            position_types.push(position_type);
            contracts.push(contracts_val);
            premiums.push(row.get::<Option<f64>, _>("close_premium").unwrap_or(0.0));
            spots.push(row.get::<Option<f64>, _>("close_spot").unwrap_or(0.0));
            realized_pnls.push(row.get::<Option<f64>, _>("realized_pnl"));
            created_ats.push(row.get::<Option<String>, _>("closed_at").unwrap_or_default());
        }
    }

    let rows = entry_ids.len() as u64;
    let mut files = Vec::new();
    if rows > 0 {
        let batch = RecordBatch::try_new(
            ledger_schema(),
            vec![
                Arc::new(StringArray::from(entry_ids)),
                Arc::new(StringArray::from(wallets)),
                Arc::new(StringArray::from(position_ids)),
                Arc::new(StringArray::from(kinds)),
                Arc::new(StringArray::from(underlyings)),
                Arc::new(StringArray::from(option_types)),
                Arc::new(StringArray::from(position_types)),
                Arc::new(Float64Array::from(contracts)),
                Arc::new(Float64Array::from(premiums)),
                Arc::new(Float64Array::from(spots)),
                Arc::new(Float64Array::from(realized_pnls)),
                Arc::new(StringArray::from(created_ats)),
            ],
        )?;
        let rel = dataset_file_rel("ledger_entries", run_date, run_id);
        write_parquet(&config.dir.join(&rel), &ledger_schema(), &batch)?;
        files.push(rel);
    }

    Ok(DatasetManifest {
        name: "ledger_entries".into(),
        schema_version: SCHEMA_VERSION,
        watermark_before: before,
        watermark_after: new_watermark,
        rows,
        files,
    })
}

// ─── Dataset: price_candles ──────────────────────────────────────────────────

fn price_candles_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        field("underlying", DataType::Utf8, false),
        field("minute", DataType::Utf8, false),
        field("open", DataType::Float64, false),
        field("high", DataType::Float64, false),
        field("low", DataType::Float64, false),
        field("close", DataType::Float64, false),
    ]))
}

async fn export_price_candles(
    db: &SqlitePool,
    config: &ExportConfig,
    watermark: &str,
    run_date: &str,
    run_id: &str,
) -> Result<DatasetManifest, ExportError> {
    let rows: Vec<SqliteRow> = sqlx::query(
        "SELECT underlying, minute, open, high, low, close
           FROM price_candles
          WHERE minute >= ?
         ORDER BY minute",
    )
    .bind(watermark)
    .fetch_all(db)
    .await?;

    let before = watermark.to_string();
    let mut new_watermark = before.clone();
    let mut underlyings = Vec::new();
    let mut minutes = Vec::new();
    let mut opens = Vec::new();
    let mut highs = Vec::new();
    let mut lows = Vec::new();
    let mut closes = Vec::new();

    for row in &rows {
        new_watermark = row.get::<String, _>("minute");
        underlyings.push(row.get::<String, _>("underlying"));
        minutes.push(row.get::<String, _>("minute"));
        opens.push(row.get::<f64, _>("open"));
        highs.push(row.get::<f64, _>("high"));
        lows.push(row.get::<f64, _>("low"));
        closes.push(row.get::<f64, _>("close"));
    }

    let rows = rows.len() as u64;
    let mut files = Vec::new();
    if rows > 0 {
        let batch = RecordBatch::try_new(
            price_candles_schema(),
            vec![
                Arc::new(StringArray::from(underlyings)),
                Arc::new(StringArray::from(minutes)),
                Arc::new(Float64Array::from(opens)),
                Arc::new(Float64Array::from(highs)),
                Arc::new(Float64Array::from(lows)),
                Arc::new(Float64Array::from(closes)),
            ],
        )?;
        let rel = dataset_file_rel("price_candles", run_date, run_id);
        write_parquet(&config.dir.join(&rel), &price_candles_schema(), &batch)?;
        files.push(rel);
    }

    Ok(DatasetManifest {
        name: "price_candles".into(),
        schema_version: SCHEMA_VERSION,
        watermark_before: before,
        watermark_after: new_watermark,
        rows,
        files,
    })
}

// ─── Dataset: settlements ────────────────────────────────────────────────────

fn settlements_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        field("settlement_id", DataType::Utf8, false),
        field("wallet_address", DataType::Utf8, false),
        field("position_id", DataType::Utf8, false),
        field("underlying", DataType::Utf8, false),
        field("close_premium", DataType::Float64, false),
        field("close_spot", DataType::Float64, false),
        field("realized_pnl", DataType::Float64, false),
        field("closed_at", DataType::Utf8, false),
    ]))
}

/// Settlements are derived from closed/rolled positions. Like ledger entries,
/// the ids are deterministic (`<position_id>:settle`) so re-exports dedupe.
async fn export_settlements(
    db: &SqlitePool,
    config: &ExportConfig,
    watermark: &str,
    run_date: &str,
    run_id: &str,
) -> Result<DatasetManifest, ExportError> {
    let rows: Vec<SqliteRow> = sqlx::query(
        "SELECT * FROM positions
          WHERE status IN ('closed', 'rolled') AND updated_at >= ?
         ORDER BY updated_at, id",
    )
    .bind(watermark)
    .fetch_all(db)
    .await?;

    let before = watermark.to_string();
    let mut new_watermark = before.clone();
    let mut settlement_ids = Vec::new();
    let mut wallets = Vec::new();
    let mut position_ids = Vec::new();
    let mut underlyings = Vec::new();
    let mut close_premiums = Vec::new();
    let mut close_spots = Vec::new();
    let mut realized_pnls = Vec::new();
    let mut closed_ats = Vec::new();

    for row in &rows {
        let id = row.get::<String, _>("id");
        new_watermark = row.get::<String, _>("updated_at");
        settlement_ids.push(format!("{id}:settle"));
        wallets.push(config.wallet_address(&row.get::<String, _>("wallet_address")));
        position_ids.push(id);
        underlyings.push(row.get::<String, _>("underlying"));
        close_premiums.push(row.get::<f64, _>("close_premium").unwrap_or(0.0));
        close_spots.push(row.get::<f64, _>("close_spot").unwrap_or(0.0));
        realized_pnls.push(row.get::<f64, _>("realized_pnl").unwrap_or(0.0));
        closed_ats.push(row.get::<String, _>("closed_at").unwrap_or_default());
    }

    let rows = rows.len() as u64;
    let mut files = Vec::new();
    if rows > 0 {
        let batch = RecordBatch::try_new(
            settlements_schema(),
            vec![
                Arc::new(StringArray::from(settlement_ids)),
                Arc::new(StringArray::from(wallets)),
                Arc::new(StringArray::from(position_ids)),
                Arc::new(StringArray::from(underlyings)),
                Arc::new(Float64Array::from(close_premiums)),
                Arc::new(Float64Array::from(close_spots)),
                Arc::new(Float64Array::from(realized_pnls)),
                Arc::new(StringArray::from(closed_ats)),
            ],
        )?;
        let rel = dataset_file_rel("settlements", run_date, run_id);
        write_parquet(&config.dir.join(&rel), &settlements_schema(), &batch)?;
        files.push(rel);
    }

    Ok(DatasetManifest {
        name: "settlements".into(),
        schema_version: SCHEMA_VERSION,
        watermark_before: before,
        watermark_after: new_watermark,
        rows,
        files,
    })
}

// ─── Dataset: chain_events ───────────────────────────────────────────────────

fn chain_events_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        field("event_id", DataType::Utf8, false),
        field("chain", DataType::Utf8, false),
        field("kind", DataType::Utf8, false),
        field("payload", DataType::Utf8, false), // JSON
        field("created_at", DataType::Utf8, false),
    ]))
}

/// There is no on-chain integration yet, so this dataset has no source rows.
/// It's still exported (schema-only, zero rows) so the warehouse knows the
/// shape waiting for chain events to exist.
async fn export_chain_events(
    db: &SqlitePool,
    config: &ExportConfig,
    watermark: &str,
    _run_date: &str,
    _run_id: &str,
) -> Result<DatasetManifest, ExportError> {
    let _ = (db, config);
    Ok(DatasetManifest {
        name: "chain_events".into(),
        schema_version: SCHEMA_VERSION,
        watermark_before: watermark.to_string(),
        watermark_after: watermark.to_string(),
        rows: 0,
        files: Vec::new(),
    })
}

// ─── Orchestration ───────────────────────────────────────────────────────────

/// The initial watermark for a dataset's first export: everything.
const INITIAL_WATERMARK: &str = "1970-01-01T00:00:00.000Z";

/// Executes one full export run. See the module docs for the guarantees.
pub async fn run_export(db: &SqlitePool, config: &ExportConfig) -> Result<ExportManifest, ExportError> {
    let run_id = uuid::Uuid::new_v4().to_string();
    let started_at = crate::auth::format_unix_secs(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64,
    );
    let run_date = started_at[..10].to_string();

    // 1. Fetch and write every dataset's Parquet files first. A failure
    //    here aborts before any watermark moves, so the next run re-exports.
    let datasets = [
        ("positions", "updated_at"),
        ("ledger_entries", "updated_at"),
        ("price_candles", "minute"),
        ("settlements", "updated_at"),
        ("chain_events", "none"),
    ];

    let mut manifests = Vec::new();
    for (name, kind) in datasets {
        let watermark = read_watermark(db, name)
            .await?
            .unwrap_or_else(|| INITIAL_WATERMARK.to_string());
        let manifest = match name {
            "positions" => export_positions(db, config, &watermark, &run_date, &run_id).await?,
            "ledger_entries" => {
                export_ledger_entries(db, config, &watermark, &run_date, &run_id).await?
            }
            "price_candles" => {
                export_price_candles(db, config, &watermark, &run_date, &run_id).await?
            }
            "settlements" => export_settlements(db, config, &watermark, &run_date, &run_id).await?,
            "chain_events" => {
                export_chain_events(db, config, &watermark, &run_date, &run_id).await?
            }
            _ => unreachable!(),
        };
        manifests.push((name, kind, manifest));
    }

    // 2. Only after every file is written, commit the advanced watermarks
    //    in one transaction. A dataset whose rows didn't move the watermark
    //    (empty export) keeps its old mark.
    let mut tx = db.begin().await?;
    for (name, kind, manifest) in &manifests {
        if manifest.rows > 0 && manifest.watermark_after != manifest.watermark_before {
            sqlx::query(
                "INSERT INTO export_watermarks (dataset, watermark, watermark_kind, updated_at)
                 VALUES (?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                 ON CONFLICT(dataset) DO UPDATE SET
                    watermark = excluded.watermark,
                    watermark_kind = excluded.watermark_kind,
                    updated_at = excluded.updated_at",
            )
            .bind(name)
            .bind(&manifest.watermark_after)
            .bind(kind)
            .execute(&mut *tx)
            .await?;
        }
    }
    tx.commit().await?;

    let finished_at = crate::auth::format_unix_secs(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64,
    );

    let manifest = ExportManifest {
        run_id,
        started_at,
        finished_at,
        datasets: manifests.into_iter().map(|(_, _, m)| m).collect(),
    };

    // 3. Write the manifest only after the watermarks have committed.
    let manifest_path = config.dir.join("manifest.json");
    if let Some(parent) = manifest_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let manifest_json = serde_json::to_string_pretty(&manifest)?;
    std::fs::write(&manifest_path, manifest_json)?;

    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn test_db() -> (SqlitePool, std::path::PathBuf) {
        let db_path =
            std::env::temp_dir().join(format!("zenith-export-test-{}.db", uuid::Uuid::new_v4()));
        let pool = crate::db::init_pool(&format!("sqlite://{}", db_path.display())).await;
        (pool, db_path)
    }

    #[test]
    fn wallet_addresses_are_hashed_by_default_and_raw_only_when_configured() {
        let mut config = ExportConfig {
            dir: std::path::PathBuf::from("/tmp/exports"),
            wallet_salt: "s3cret".to_string(),
            raw_wallets: false,
        };
        let hashed = config.wallet_address("GABC");
        assert_ne!(hashed, "GABC");
        assert_eq!(hashed.len(), 64); // sha256 hex

        // Same input + same salt -> same hash (stable for the warehouse).
        assert_eq!(hashed, config.wallet_address("GABC"));

        config.raw_wallets = true;
        assert_eq!(config.wallet_address("GABC"), "GABC");
    }

    #[tokio::test]
    async fn a_full_run_exports_positions_and_writes_a_manifest() {
        let (db, db_path) = test_db().await;
        sqlx::query("INSERT INTO accounts (wallet_address) VALUES ('W')")
            .execute(&db)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO positions
                (id, wallet_address, underlying, strike, expiry_days, option_type,
                 position_type, contracts, entry_premium, entry_spot, status, updated_at)
             VALUES ('p1', 'W', 'BTC', 70000, 30, 'call', 'long', 1, 100, 67000, 'open',
                     '2026-09-28T00:00:00.000Z')",
        )
        .execute(&db)
        .await
        .unwrap();

        let dir = std::env::temp_dir().join(format!("zenith-export-{}", uuid::Uuid::new_v4()));
        let config = ExportConfig {
            dir: dir.clone(),
            wallet_salt: "s3cret".to_string(),
            raw_wallets: false,
        };

        let manifest = run_export(&db, &config).await.unwrap();
        assert_eq!(manifest.datasets.len(), 5);

        let positions = &manifest.datasets[0];
        assert_eq!(positions.name, "positions");
        assert_eq!(positions.rows, 1);
        assert_eq!(positions.files.len(), 1);

        // The Parquet file exists and is readable.
        let file = std::fs::File::open(dir.join(&positions.files[0])).unwrap();
        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap();
        let mut reader = builder.build().unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);

        // The wallet address in the batch is hashed, not raw.
        let schema = batch.schema();
        let wallet_idx = schema.index_of("wallet_address").unwrap();
        let wallet_col = batch
            .column(wallet_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_ne!(wallet_col.value(0), "W");
        assert_eq!(wallet_col.value(0).len(), 64);

        // The watermark advanced.
        let watermark = read_watermark(&db, "positions").await.unwrap().unwrap();
        assert_eq!(watermark, "2026-09-28T00:00:00.000Z");

        // The manifest was written.
        let manifest_json = std::fs::read_to_string(dir.join("manifest.json")).unwrap();
        assert!(manifest_json.contains("\"run_id\""));

        db.close().await;
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_second_run_exports_only_rows_past_the_watermark() {
        let (db, db_path) = test_db().await;
        sqlx::query("INSERT INTO accounts (wallet_address) VALUES ('W')")
            .execute(&db)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO positions
                (id, wallet_address, underlying, strike, expiry_days, option_type,
                 position_type, contracts, entry_premium, entry_spot, status, updated_at)
             VALUES ('p1', 'W', 'BTC', 70000, 30, 'call', 'long', 1, 100, 67000, 'open',
                     '2026-09-28T00:00:00.000Z')",
        )
        .execute(&db)
        .await
        .unwrap();

        let dir = std::env::temp_dir().join(format!("zenith-export-{}", uuid::Uuid::new_v4()));
        let config = ExportConfig {
            dir: dir.clone(),
            wallet_salt: "s3cret".to_string(),
            raw_wallets: false,
        };

        run_export(&db, &config).await.unwrap();

        // A second position with a later updated_at.
        sqlx::query(
            "INSERT INTO positions
                (id, wallet_address, underlying, strike, expiry_days, option_type,
                 position_type, contracts, entry_premium, entry_spot, status, updated_at)
             VALUES ('p2', 'W', 'ETH', 3500, 30, 'call', 'long', 1, 50, 3400, 'open',
                     '2026-09-29T00:00:00.000Z')",
        )
        .execute(&db)
        .await
        .unwrap();

        let manifest = run_export(&db, &config).await.unwrap();
        let positions = &manifest.datasets[0];
        assert_eq!(positions.rows, 1, "only the new row should be exported");
        assert_eq!(positions.watermark_before, "2026-09-28T00:00:00.000Z");
        assert_eq!(positions.watermark_after, "2026-09-29T00:00:00.000Z");

        db.close().await;
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
