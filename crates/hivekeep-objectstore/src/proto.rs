//! The object-store plugin API and the plugin's private credential exchange.
//!
//! | package                            | module                    |
//! |------------------------------------|---------------------------|
//! | `hivekeep.objectstore.v1`          | [`v1`]                    |
//! | `hivekeep.objectstore.exchange.v1` | [`exchange::v1`]          |
//!
//! Requests carrying an actor JWT, an asset URI or an access token print
//! those fields as `<redacted>` in their `Debug` output.

#![allow(clippy::doc_lazy_continuation, clippy::large_enum_variant)]

use std::fmt;

/// Version 1 of the plugin API.
pub mod v1 {
    tonic::include_proto!("hivekeep.objectstore.v1");
}

/// The node and control plugins' credential exchange.
pub mod exchange {
    /// Version 1 of the exchange.
    pub mod v1 {
        tonic::include_proto!("hivekeep.objectstore.exchange.v1");
    }
}

const REDACTED: &str = "<redacted>";

fn redacted(s: &str) -> &'static str {
    if s.is_empty() {
        ""
    } else {
        REDACTED
    }
}

impl fmt::Debug for v1::FetchSnapshotRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FetchSnapshotRequest")
            .field("snapshot_uri", &self.snapshot_uri)
            .field("actor_jwt", &redacted(&self.actor_jwt))
            .field("write_path", &self.write_path)
            .field("files", &self.files)
            .finish()
    }
}

impl fmt::Debug for v1::UploadSnapshotRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UploadSnapshotRequest")
            .field("snapshot_uri", &self.snapshot_uri)
            .field("actor_jwt", &redacted(&self.actor_jwt))
            .field("local_path", &self.local_path)
            .field("files", &self.files)
            .finish()
    }
}

impl fmt::Debug for v1::FetchAssetRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FetchAssetRequest")
            .field("asset_uri", &redacted(&self.asset_uri))
            .field("sha256", &self.sha256)
            .field("write_path", &self.write_path)
            .field("max_bytes", &self.max_bytes)
            .finish()
    }
}

impl fmt::Debug for exchange::v1::ExchangeActorJwtRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExchangeActorJwtRequest")
            .field("actor_jwt", &redacted(&self.actor_jwt))
            .field("force", &self.force)
            .finish()
    }
}

impl fmt::Debug for exchange::v1::ExchangeActorJwtResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExchangeActorJwtResponse")
            .field("access_token", &redacted(&self.access_token))
            .field("expire_time", &self.expire_time)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_never_prints_credentials() {
        let req = v1::FetchSnapshotRequest {
            snapshot_uri: "gs://b/p".into(),
            actor_jwt: "secret-jwt".into(),
            write_path: "/w".into(),
            files: vec!["a".into()],
        };
        let s = format!("{req:?}");
        assert!(!s.contains("secret-jwt"), "{s}");
        assert!(s.contains("gs://b/p"));
        let asset = v1::FetchAssetRequest {
            asset_uri: "gs://b/o?sig=secret".into(),
            ..Default::default()
        };
        assert!(!format!("{asset:?}").contains("secret"));
        let resp = exchange::v1::ExchangeActorJwtResponse {
            access_token: "tok-secret".into(),
            expire_time: None,
        };
        assert!(!format!("{resp:?}").contains("tok-secret"));
    }
}
