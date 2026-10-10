//! The process log subscriber: JSON lines with trace correlation and
//! redacted protobuf values, behind a dynamic minimum level.

use std::sync::atomic::{AtomicU8, Ordering};

use tracing::{Level, Metadata, Subscriber};
use tracing_subscriber::filter::{DynFilterFn, Filtered};
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;

use super::error::BootError;
use crate::contextlogging::JsonLayer;

/// The dynamic minimum level, as an index into [`LEVELS`]. An atomic so
/// [`set_log_level`] works before or after the subscriber is installed.
static LEVEL: AtomicU8 = AtomicU8::new(2);

const LEVELS: [Level; 4] = [Level::ERROR, Level::WARN, Level::INFO, Level::DEBUG];

/// The current minimum level. The untouched default is `INFO`.
pub fn log_level() -> Level {
    LEVELS[LEVEL.load(Ordering::Relaxed) as usize]
}

/// Sets the minimum level from a flag value: `debug`, `info`, `warn` or
/// `error`, in any case. Empty means unset and leaves the level unchanged,
/// so configurations that never populate the field keep the default.
pub fn set_log_level(level: &str) -> Result<(), BootError> {
    if level.is_empty() {
        return Ok(());
    }
    let idx = match level.to_ascii_lowercase().as_str() {
        "error" => 0,
        "warn" | "warning" => 1,
        "info" => 2,
        "debug" => 3,
        _ => return Err(BootError::InvalidLogLevel(level.to_string())),
    };
    LEVEL.store(idx, Ordering::Relaxed);
    Ok(())
}

/// The level filter type of [`json_log_layer`].
pub type LevelFilterFn<S> = DynFilterFn<S, fn(&Metadata<'_>, &Context<'_, S>) -> bool>;

fn at_or_above_level<S>(meta: &Metadata<'_>, _: &Context<'_, S>) -> bool {
    *meta.level() <= log_level()
}

/// The filter applying the dynamic level of [`set_log_level`].
pub fn dynamic_level_filter<S>() -> LevelFilterFn<S>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    DynFilterFn::new(at_or_above_level::<S> as fn(&Metadata<'_>, &Context<'_, S>) -> bool)
}

/// The JSON layer behind the dynamic level, writing to `make_writer`.
pub fn json_log_layer<S, W>(make_writer: W) -> Filtered<JsonLayer<W>, LevelFilterFn<S>, S>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    W: for<'a> MakeWriter<'a> + 'static,
{
    let filter: LevelFilterFn<S> =
        DynFilterFn::new(at_or_above_level::<S> as fn(&Metadata<'_>, &Context<'_, S>) -> bool);
    JsonLayer::new(make_writer).with_filter(filter)
}

/// Installs the JSON logger writing to stdout as the global subscriber.
/// Call once at process start.
pub fn init_logger() -> Result<(), BootError> {
    init_logger_with_writer(std::io::stdout)
}

/// [`init_logger`] with an explicit destination. Use it to share one
/// synchronized writer between the runtime logger and another writer (an
/// Actor log forwarder) so their lines do not interleave.
pub fn init_logger_with_writer<W>(make_writer: W) -> Result<(), BootError>
where
    W: for<'a> MakeWriter<'a> + Send + Sync + 'static,
{
    tracing_subscriber::registry()
        .with(json_log_layer(make_writer))
        .try_init()
        .map_err(|e| BootError::Subscriber(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contextlogging::tests::Buf;
    use crate::contextlogging::ProtoValue;
    use hivekeep_proto::api::v1 as api;

    #[test]
    fn set_log_level_is_dynamic() {
        assert_eq!(
            log_level(),
            Level::INFO,
            "the untouched default must be exactly info"
        );
        let buf = Buf::default();
        let subscriber = tracing_subscriber::registry().with(json_log_layer(buf.clone()));
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!("visible at default level");
            assert!(buf.text().contains("visible at default level"));
            buf.clear();
            tracing::debug!("hidden at default level");
            assert!(
                buf.text().is_empty(),
                "debug emitted at default level: {}",
                buf.text()
            );

            set_log_level("debug").unwrap();
            tracing::debug!("visible at debug");
            assert!(buf.text().contains("visible at debug"));

            set_log_level("WARN").unwrap();
            buf.clear();
            tracing::info!("hidden at warn");
            assert!(
                buf.text().is_empty(),
                "info emitted at warn: {}",
                buf.text()
            );

            assert!(set_log_level("verbose").is_err());
            set_log_level("").unwrap();
            assert_eq!(
                log_level(),
                Level::WARN,
                "empty must leave the level unchanged"
            );
        });
        set_log_level("info").unwrap();
    }

    #[test]
    fn json_log_layer_masks_debug_redact_fields() {
        const TOKEN: &str = "eyJhbGciOiJSUzI1NiJ9.secret-token";
        let buf = Buf::default();
        let subscriber = tracing_subscriber::registry().with(json_log_layer(buf.clone()));
        tracing::subscriber::with_default(subscriber, || {
            let resp = api::MintActorJwtResponse {
                actor_jwt: TOKEN.into(),
                ..Default::default()
            };
            tracing::error!(resp = %ProtoValue::named("hivekeep.api.v1.MintActorJwtResponse", &resp), "Handle RPC");
        });
        let got = buf.text();
        assert!(!got.contains(TOKEN), "{got}");
        assert!(got.contains(r#""actor_jwt":"[REDACTED]""#), "{got}");
    }
}
