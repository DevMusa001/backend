# --- build stage -------------------------------------------------------
FROM rust:1-slim AS builder
WORKDIR /app

# The query macros resolve their metadata from the committed .sqlx/ directory
# at compile time; SQLX_OFFLINE=true keeps `cargo build` from trying to open a
# database connection during the build (there is none in this stage).
ENV SQLX_OFFLINE=true

# sqlx's "sqlite" feature builds SQLite from source via libsqlite3-sys,
# which needs a C compiler — the slim image doesn't ship one by default.
RUN apt-get update && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

# Cache dependency compilation separately from the app source: this
# layer only invalidates when Cargo.toml/Cargo.lock change, not on
# every source edit.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && echo "" > src/lib.rs
RUN cargo build --release && rm -rf src

COPY . .
RUN touch src/main.rs src/lib.rs && cargo build --release

# --- runtime stage -------------------------------------------------------
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
# migrations/ isn't needed here — sqlx::migrate!() embeds the SQL files
# into the binary at compile time, not read from disk at runtime.
COPY --from=builder /app/target/release/zenith-backend ./

ENV DATABASE_URL=sqlite:///data/zenith.db
VOLUME ["/data"]
EXPOSE 8081

CMD ["./zenith-backend"]
