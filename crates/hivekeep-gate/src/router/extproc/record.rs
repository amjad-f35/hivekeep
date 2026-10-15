//! A ring buffer of recent routing decisions, shown on the status page.

use std::time::Duration;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::Serialize;

use super::attributes::AUTHORITY_HEADER;
use super::metadata::RequestMetadata;

/// One routed (or refused) request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RecordedQuery {
    /// When routing started.
    pub timestamp: DateTime<Utc>,
    /// The request's authority.
    pub client: String,
    /// The request's host.
    pub host: String,
    /// The path without its query.
    pub path: String,
    /// The method.
    pub method: String,
    /// What the router did.
    pub action: String,
    /// Where it routed.
    pub target: String,
    /// How long routing took, in nanoseconds on the wire.
    #[serde(serialize_with = "nanos")]
    pub duration: Duration,
}

fn nanos<S: serde::Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_u128(d.as_nanos())
}

struct Ring {
    queries: Vec<RecordedQuery>,
    index: usize,
}

/// Keeps the last `size` queries.
pub struct QueryRecorder {
    ring: RwLock<Ring>,
    size: usize,
}

/// Strips the query string, which may carry secrets.
pub fn redact_path(path: &str) -> &str {
    path.split_once('?').map_or(path, |(p, _)| p)
}

impl QueryRecorder {
    /// A recorder keeping `size` queries.
    pub fn new(size: usize) -> Self {
        QueryRecorder {
            ring: RwLock::new(Ring {
                queries: Vec::with_capacity(size),
                index: 0,
            }),
            size,
        }
    }

    /// Records `q`, evicting the oldest when full.
    pub fn add(&self, q: RecordedQuery) {
        if self.size == 0 {
            return;
        }
        let mut r = self.ring.write();
        if r.queries.len() < self.size {
            r.queries.push(q);
        } else {
            let i = r.index;
            r.queries[i] = q;
            r.index = (i + 1) % self.size;
        }
    }

    /// The recorded queries, newest first.
    pub fn get(&self) -> Vec<RecordedQuery> {
        let r = self.ring.read();
        let n = r.queries.len();
        if n < self.size {
            return r.queries.iter().rev().cloned().collect();
        }
        (0..n)
            .map(|i| r.queries[(r.index + n - 1 - i) % n].clone())
            .collect()
    }

    /// Records a routing decision for `md`.
    pub fn add_router_request(
        &self,
        start: DateTime<Utc>,
        duration: Duration,
        action: &str,
        target: &str,
        md: &RequestMetadata,
    ) {
        self.add(RecordedQuery {
            timestamp: start,
            client: md.header(AUTHORITY_HEADER).to_string(),
            host: md.host.clone(),
            path: redact_path(&md.path).to_string(),
            method: md.header(":method").to_string(),
            action: action.to_string(),
            target: target.to_string(),
            duration,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(i: usize) -> RecordedQuery {
        RecordedQuery {
            timestamp: Utc::now(),
            client: String::new(),
            host: format!("h{i}"),
            path: String::new(),
            method: String::new(),
            action: String::new(),
            target: String::new(),
            duration: Duration::ZERO,
        }
    }

    #[test]
    fn ring_keeps_newest_first() {
        let r = QueryRecorder::new(3);
        assert!(r.get().is_empty());
        for i in 0..2 {
            r.add(q(i));
        }
        assert_eq!(
            r.get().iter().map(|q| q.host.as_str()).collect::<Vec<_>>(),
            ["h1", "h0"]
        );
        for i in 2..5 {
            r.add(q(i));
        }
        assert_eq!(
            r.get().iter().map(|q| q.host.as_str()).collect::<Vec<_>>(),
            ["h4", "h3", "h2"]
        );
    }

    #[test]
    fn redacts_query() {
        assert_eq!(redact_path("/a?token=x"), "/a");
        assert_eq!(redact_path("/a"), "/a");
    }
}
