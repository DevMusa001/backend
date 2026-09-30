# Options Pricing API

A high-performance options pricing and market data API.

## API

### REST

- `GET /api/v1/spot` — current spot prices for all underlyings.
- `GET /api/v1/chain` — option chain for an underlying/expiry.
- `GET /api/v1/surface` — implied volatility surface for an underlying.

### WebSocket

#### Legacy: `/api/v1/ws/spot`

> **Deprecation note:** `/api/v1/ws/spot` is deprecated in favor of the
> multiplexed `/api/v1/ws` endpoint below. It continues to work unchanged for
> backwards compatibility, but new clients should use `/api/v1/ws`.

Pushes every underlying's spot price to every connected client roughly every
2 seconds, regardless of what the client is interested in.

#### Multiplexed: `/api/v1/ws`

A single connection can subscribe to multiple public channels using a JSON
subscribe/unsubscribe protocol. Each channel delivers a snapshot followed by
deltas, with per-channel monotonically increasing sequence numbers so clients
can detect gaps and resubscribe for a fresh snapshot.

**Client messages**

```json
{"op": "subscribe", "channels": ["spot.BTC", "chain.BTC.2024-12-27", "surface.BTC"]}
{"op": "unsubscribe", "channels": ["spot.BTC"]}
```

**Server messages**

```json
{"channel": "spot.BTC", "seq": 1, "type": "snapshot", "data": { ... }}
{"channel": "spot.BTC", "seq": 2, "type": "update", "data": { ... }}
```

**Channels**

- `spot.<UNDERLYING>` — spot price updates for an underlying.
- `chain.<UNDERLYING>.<EXPIRY>` — option chain updates for an underlying/expiry.
- `surface.<UNDERLYING>` — implied volatility surface updates for an underlying.

**Behavior**

- Per-channel `seq` is monotonically increasing. A gap indicates missed
  messages; resubscribe to receive a fresh snapshot.
- Chain updates are computed lazily and shared across subscribers: a channel is
  only computed when it has at least one subscriber.
- A maximum of 50 subscriptions per connection is allowed. Exceeding the limit
  returns a structured error message rather than disconnecting.
- Subscribing to an unknown underlying returns an error message, not a
  disconnect.
- Duplicate subscriptions are idempotent.
- Connections that never send a subscribe are closed after a 60s idle timeout.

**Out of scope:** private/authenticated channels (tracked separately).

See [`docs/ws-protocol.md`](docs/ws-protocol.md) for the full protocol write-up.

### Auth (public, rate-limited)

| Endpoint | What it does |
|---|---|
| `POST /api/v1/auth/nonce` | Issue a single-use, 5-minute sign-in message for a wallet address |
| `POST /api/v1/auth/verify` | Verify the signed message, get a 24h bearer session token |
| `GET /api/v1/auth/me` **auth** | Confirm the current token's wallet address |

Every `POST` below (positions open/close/roll, watchlist/alerts create,
strategies/execute) is rate-limited per-IP (5/s, burst 20) on top of
requiring a session — see `mutation_rate_limited_routes()` in `lib.rs`.

### Account & positions **auth**

| Endpoint | What it does |
|---|---|
| `GET /api/v1/account` | Balance + locked collateral |
| `GET /api/v1/wallet/readiness` | Balance, locked collateral, available buying power and whether the wallet can currently trade (cached per-wallet, invalidated on every trade) |
| `GET /api/v1/positions` | List positions (`?status=`, `?strategy_id=`, `?limit=`, `?offset=`) |
| `POST /api/v1/positions/open` | Price and open one position |
| `POST /api/v1/positions/:id/close` | Settle an open position at current spot/vol |
| `POST /api/v1/positions/:id/roll` | Close + reopen at a new strike/expiry, atomically |
| `GET /api/v1/history` | Closed/rolled positions + win/loss/pnl stats (`?limit=`, `?offset=` — stats always cover the full history, not just the returned page) |
| `GET /api/v1/portfolio/greeks` | Aggregate Greeks across all open positions, repriced live |
| `POST /api/v1/strategies/execute` | Open 2+ legs atomically under one shared `strategy_id` |
| `GET /api/v1/strategies` | List multi-leg strategies with aggregate realized/unrealized P&L |
| `GET /api/v1/strategies/:id` | One strategy's legs + aggregate P&L |
| `POST /api/v1/strategies/:id/close` | Close every currently-open leg of a strategy atomically |

### Watchlist & alerts **auth**

| Endpoint | What it does |
|---|---|
| `GET` / `POST /api/v1/watchlist` | List / add a watched symbol |
| `DELETE /api/v1/watchlist/:underlying` | Remove a watched symbol |
| `GET` / `POST /api/v1/alerts` | List / create a price alert (`above`/`below` a target) |
| `DELETE /api/v1/alerts/:id` | Remove an alert |

Alerts are checked against spot every 10s by a background task; a
triggered alert stays in the table (visible via GET) rather than being
deleted.

Every response carries an `x-request-id` header — a fresh UUIDv4 if the
request didn't already have one, or the caller's own value echoed back
unchanged otherwise — for tracing a single request through logs.

## Architecture

```
src/
├── main.rs          # Thin entrypoint: init_tracing -> init_state -> build_router -> serve
├── lib.rs           # Pricing engine, AppState, request/response types, route wiring
├── db.rs            # SQLite pool + migration runner
├── models.rs        # Row structs (Account, Position, WatchlistItem, Alert)
├── error.rs         # AppError: JSON {"error": "..."} instead of empty-body status codes
├── auth.rs          # Sign-in-with-wallet: nonce, verify, AuthUser extractor, session cleanup
├── strkey.rs         # Stellar G... address <-> raw ed25519 pubkey codec
├── collateral.rs    # Collateral rules for writing options (100% calls, 110% puts)
├── payoff.rs         # Combined multi-leg P&L math (ported from the frontend's lib/payoff.ts)
├── positions.rs      # Account/position/roll/greeks handlers + the open/close tx helpers
├── strategies.rs     # Multi-leg atomic execution, built on positions.rs's tx helpers
├── history.rs         # Closed/rolled positions + stats
├── request_id.rs      # UUIDv4 generator for the x-request-id middleware
├── watchlist.rs, alerts.rs, prices.rs  # Per-domain CRUD + background loops
migrations/           # One file per schema change, embedded into the binary at compile time
tests/
├── common/mod.rs     # TestApp: real router over a throwaway temp-file DB, via tower::oneshot
└── *_test.rs         # One file per domain, black-box HTTP-level assertions
```

`main.rs` is intentionally thin — everything testable lives in the
`zenith_backend` library crate, which is what lets `tests/*_test.rs`
exercise the real router without a bin-only crate's usual restriction
(a `tests/` directory can only see a *library* crate's public items).

### A note on the cache

Expensive, shareable computations (option chains, the spot/vol surface,
expiry calendars, protocol stats) are cached per **market snapshot version** —
a counter bumped on every price-simulator tick — so a new tick makes the
previous tick's entries unreachable with no explicit invalidation. Wallet
readiness is the exception: it is keyed by wallet and invalidated on every
trade that moves balance/collateral. The backend is moka (in-process, the
default) or Redis, configured via `ZENITH_CACHE` / `ZENITH_REDIS_URL`; a Redis
outage degrades to computing on every request rather than erroring. See
`src/cache.rs` and `docs/sqlx-offline.md`.

### A note on the pricing model

`smile_vol()` is a bit-for-bit port of the frontend's `smileVol()` —
including its wing term being unconditionally `(|moneyness-1|-0.15)^2`
rather than clamped at zero near the money, which looks like a bug but
matches the frontend's shipped (if quirky) behavior on purpose, for
pricing parity between client and server.

### A note on time-to-expiry

Closing/rolling a position reprices it with the *same* time-to-expiry
it was opened with, rather than tracking an absolute expiry timestamp
and computing real elapsed time. Fine for a paper-trading demo; not a
real theta-decay model.

## Known gaps

- No real market data feed — spot prices are seeded constants nudged by
  a random-walk simulator, not sourced from anywhere real.
- No on-chain / Soroban integration — this is pure off-chain paper
  trading.
- `Dockerfile` and the CI workflow are not build/run-tested against a
  real Docker daemon or GitHub Actions runner from this environment —
  reviewed for correctness, not executed end-to-end.
- The mutation rate limiter's bearer-token fallback (for requests with
  no token at all) keys on peer IP only, without replicating
  SmartIpKeyExtractor's x-forwarded-for/x-real-ip/forwarded header
  chain — acceptable since that fallback path is only reached by
  requests that fail AuthUser's own check regardless, but it does mean
  that one specific path isn't proxy-aware the way the auth endpoints'
  limiter is.

Previously listed here and since addressed: the three background loops
(auth cleanup, alert checks, price simulator) now have direct unit
tests against their extracted per-tick logic; the rate limiter moved
off a single global quota to per-IP (`SmartIpKeyExtractor`) on the auth
endpoints and per-wallet (`BearerOrIpKeyExtractor`) on every mutating
one (positions, watchlist, alerts, strategies), so wallets sharing an
IP no longer share a quota; `list_positions`/`get_history` now report
total count and whether more pages exist (`x-total-count`/`x-has-more`
headers on positions, a `has_more` field on history) instead of
leaving a paging client to guess; malformed query params and JSON
bodies now return the same `{"error": "..."}` shape as every other
failure instead of axum's plain-text rejections (`AppQuery`/`AppJson`
in `error.rs`); every remaining unexpected-DB-failure path across the
whole app now names the specific operation that failed (`db_error()`
in `error.rs`) instead of a bare "Internal Server Error" with no other
detail; and cross-wallet ownership on close/roll (a stranger 404s
trying to close or roll another wallet's position, same as it already
did for deleting someone else's alert/watchlist entry) and
roll_position's replacement-leg validation are now actually tested
rather than just assumed from reading the SQL.

## License

MIT © Zenith Protocol Contributors
