use axum::extract::ConnectInfo;
use axum::http::Request;
use std::net::SocketAddr;
use tower_governor::{errors::GovernorError, key_extractor::KeyExtractor};

/// Keys the rate limiter on the caller's bearer token when present, so
/// wallets sharing an IP (NAT, VPN, corporate network) each get their own
/// quota instead of splitting one — falling back to the peer address for
/// requests with no token at all (which, on every route this extractor is
/// actually used for, will fail AuthUser's own check regardless; this
/// fallback only needs to be *some* reasonable key, not a perfect one).
///
/// Unlike SmartIpKeyExtractor's fallback, this doesn't also check
/// x-forwarded-for/x-real-ip/forwarded — those helpers aren't exported by
/// tower_governor, and replicating them isn't worth it for a path that's
/// only reached by requests missing the token these routes require anyway.
#[derive(Debug, Clone, Copy, Default)]
pub struct BearerOrIpKeyExtractor;

impl KeyExtractor for BearerOrIpKeyExtractor {
    type Key = String;

    // No `name()`/`key_name()` override: those are gated behind
    // tower_governor's own "tracing" feature (not a feature of this
    // crate — we don't enable it), so the trait doesn't require them as
    // compiled here.
    fn extract<T>(&self, req: &Request<T>) -> Result<Self::Key, GovernorError> {
        if let Some(token) = req
            .headers()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
        {
            return Ok(format!("token:{token}"));
        }

        req.extensions()
            .get::<ConnectInfo<SocketAddr>>()
            .map(|addr| format!("ip:{}", addr.0.ip()))
            .ok_or(GovernorError::UnableToExtractKey)
    }
}

/// Extracts the caller's IP for WebSocket connection accounting, reusing the
/// same trusted-proxy logic as the rate limiter: when the request carries a
/// `ConnectInfo<SocketAddr>` (populated by `into_make_service_with_connect_info`
/// behind a trusted proxy) we key on the peer address, otherwise we fall back
/// to the `x-forwarded-for` / `x-real-ip` headers so per-IP connection caps
/// still work when the server sits behind a reverse proxy.
///
/// Returns `None` only when no address can be determined at all, in which case
/// callers should treat the connection as unkeyed rather than rejecting it.
pub fn extract_client_ip<T>(req: &Request<T>) -> Option<std::net::IpAddr> {
    if let Some(addr) = req.extensions().get::<ConnectInfo<SocketAddr>>() {
        return Some(addr.0.ip());
    }

    if let Some(ip) = req
        .headers()
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(',').next())
        .and_then(|v| v.trim().parse::<std::net::IpAddr>().ok())
    {
        return Some(ip);
    }

    req.headers()
        .get("x-real-ip")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<std::net::IpAddr>().ok())
}
