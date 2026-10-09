//! A ceiling on how long any request may run.

use std::future::Future;
use std::time::Duration;

use tokio::time::Instant;
use tonic::{Request, Status};

/// The deadline a request runs under, placed in the request extensions by
/// [`with_max_deadline`] so handlers can read it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Deadline(pub Instant);

/// Parses a `grpc-timeout` header value: up to eight digits and a unit
/// (`H`, `M`, `S`, `m`, `u`, `n`).
pub fn parse_grpc_timeout(v: &str) -> Option<Duration> {
    if v.len() < 2 || v.len() > 9 {
        return None;
    }
    let (digits, unit) = v.split_at(v.len() - 1);
    let n: u64 = digits.parse().ok()?;
    Some(match unit {
        "H" => Duration::from_secs(n * 3600),
        "M" => Duration::from_secs(n * 60),
        "S" => Duration::from_secs(n),
        "m" => Duration::from_millis(n),
        "u" => Duration::from_micros(n),
        "n" => Duration::from_nanos(n),
        _ => return None,
    })
}

/// Runs `handler` under the earlier of the caller's deadline (a
/// [`Deadline`] already in the extensions, or the `grpc-timeout` header)
/// and `ceiling` from now. A ceiling above the caller's own deadline never
/// extends it. When the deadline passes the handler is dropped and the call
/// fails with `DeadlineExceeded`.
pub async fn with_max_deadline<Req, Resp, F, Fut>(
    ceiling: Duration,
    mut req: Request<Req>,
    handler: F,
) -> Result<Resp, Status>
where
    F: FnOnce(Request<Req>) -> Fut,
    Fut: Future<Output = Result<Resp, Status>>,
{
    let now = Instant::now();
    let mut deadline = now + ceiling;
    if let Some(Deadline(caller)) = req.extensions().get::<Deadline>() {
        deadline = deadline.min(*caller);
    }
    if let Some(t) = req
        .metadata()
        .get("grpc-timeout")
        .and_then(|v| v.to_str().ok())
        .and_then(parse_grpc_timeout)
    {
        deadline = deadline.min(now + t);
    }
    req.extensions_mut().insert(Deadline(deadline));
    match tokio::time::timeout_at(deadline, handler(req)).await {
        Ok(r) => r,
        Err(_) => Err(Status::deadline_exceeded("context deadline exceeded")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Code;

    #[tokio::test]
    async fn max_deadline_is_enforced() {
        const CEILING: Duration = Duration::from_millis(50);
        for (name, header) in [
            ("no caller deadline", None),
            ("caller deadline longer than ceiling", Some("1H")),
        ] {
            let mut req = Request::new(());
            if let Some(h) = header {
                req.metadata_mut()
                    .insert("grpc-timeout", h.parse().unwrap());
            }
            let started = Instant::now();
            let res: Result<(), Status> = with_max_deadline(CEILING, req, |req| async move {
                let Deadline(d) = *req
                    .extensions()
                    .get::<Deadline>()
                    .expect("deadline present");
                assert!(
                    d.duration_since(Instant::now()) <= CEILING,
                    "{name}: deadline beyond the ceiling"
                );
                std::future::pending::<()>().await;
                Ok(())
            })
            .await;
            assert_eq!(res.unwrap_err().code(), Code::DeadlineExceeded, "{name}");
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "{name}: waited out the caller's deadline"
            );
        }
    }

    #[tokio::test]
    async fn shorter_deadline_is_preserved() {
        let caller = Instant::now() + Duration::from_millis(50);
        let mut req = Request::new(());
        req.extensions_mut().insert(Deadline(caller));
        let res = with_max_deadline(Duration::from_secs(3600), req, |req| async move {
            let Deadline(d) = *req.extensions().get::<Deadline>().unwrap();
            assert_eq!(
                d, caller,
                "a ceiling above the caller's deadline must not override it"
            );
            Ok("response")
        })
        .await;
        assert_eq!(res.unwrap(), "response");

        // The same through the header.
        let mut req = Request::new(());
        req.metadata_mut()
            .insert("grpc-timeout", "50m".parse().unwrap());
        let before = Instant::now();
        with_max_deadline(Duration::from_secs(3600), req, |req| async move {
            let Deadline(d) = *req.extensions().get::<Deadline>().unwrap();
            assert!(
                d <= before + Duration::from_millis(60) && d >= before + Duration::from_millis(50)
            );
            Ok(())
        })
        .await
        .unwrap();
    }

    #[test]
    fn grpc_timeout_parsing() {
        assert_eq!(parse_grpc_timeout("5S"), Some(Duration::from_secs(5)));
        assert_eq!(parse_grpc_timeout("100m"), Some(Duration::from_millis(100)));
        assert_eq!(parse_grpc_timeout("2H"), Some(Duration::from_secs(7200)));
        assert_eq!(parse_grpc_timeout("S"), None);
        assert_eq!(parse_grpc_timeout("123456789S"), None);
        assert_eq!(parse_grpc_timeout("5x"), None);
    }
}
