use clap::CommandFactory;

use super::*;

fn parse(extra: &[String]) -> Result<Args, clap::Error> {
    let mut v = vec!["hivekeep-keeper".to_string()];
    v.extend_from_slice(extra);
    Args::try_parse_from(v)
}

fn default_of(flag: &str) -> String {
    let cmd = Args::command();
    let arg = cmd
        .get_arguments()
        .find(|a| a.get_long() == Some(flag))
        .unwrap_or_else(|| panic!("no --{flag} flag registered"));
    arg.get_default_values()
        .iter()
        .map(|v| v.to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join(",")
}

fn secs(d: Duration) -> String {
    format!("{}s", d.as_secs())
}

#[test]
fn port_flag_default() {
    assert_eq!(default_of("port"), DEFAULT_PORT.to_string());
}

/// The snapshot delays must be positive, the upload timeout must not be
/// negative (0 turns it off), and the upload delay must not exceed the
/// detach delay, which the defaults satisfy too. A negative duration does
/// not parse.
#[test]
fn validate_snapshot_delay_flags() {
    let min = Duration::from_secs(60);
    let hour = Duration::from_secs(3600);
    let five = Duration::from_secs(300);
    // Name, raw flag values (used when no durations are given), durations
    // and whether validation fails.
    type Case<'a> = (&'a str, [&'a str; 4], Option<[Duration; 4]>, bool);
    let cases: Vec<Case> = vec![
        (
            "defaults",
            ["", "", "", ""],
            Some([
                st::DEFAULT_UPLOAD_DELAY,
                st::DEFAULT_DETACH_DELAY,
                st::DEFAULT_UPLOAD_TIMEOUT,
                st::DEFAULT_REPORT_GIVE_UP_AFTER,
            ]),
            false,
        ),
        (
            "upload sooner than detach",
            ["", "", "", ""],
            Some([min, hour, five, min]),
            false,
        ),
        (
            "upload as late as detach",
            ["", "", "", ""],
            Some([hour, hour, five, min]),
            false,
        ),
        (
            "upload later than detach",
            ["", "", "", ""],
            Some([hour, min, five, min]),
            true,
        ),
        (
            "zero upload delay",
            ["", "", "", ""],
            Some([Duration::ZERO, hour, five, min]),
            true,
        ),
        (
            "negative detach delay",
            ["1m", "-1m", "5m", "1m"],
            None,
            true,
        ),
        (
            "zero upload timeout",
            ["", "", "", ""],
            Some([min, hour, Duration::ZERO, min]),
            false,
        ),
        (
            "negative upload timeout",
            ["1m", "1h", "-1m", "1m"],
            None,
            true,
        ),
        (
            "zero report give-up delay",
            ["", "", "", ""],
            Some([min, hour, five, Duration::ZERO]),
            true,
        ),
        (
            "negative report give-up delay",
            ["1m", "1h", "5m", "-1m"],
            None,
            true,
        ),
    ];
    let flags = [
        "--snapshot-upload-delay",
        "--snapshot-detach-delay",
        "--snapshot-upload-timeout",
        "--snapshot-report-give-up-after",
    ];
    for (name, raw, durations, want_err) in cases {
        let values: Vec<String> = match durations {
            Some(d) => d.iter().map(|d| secs(*d)).collect(),
            None => raw.iter().map(|s| s.to_string()).collect(),
        };
        let argv: Vec<String> = flags
            .iter()
            .zip(&values)
            .map(|(f, v)| format!("{f}={v}"))
            .collect();
        let res = parse(&argv)
            .map_err(|e| e.to_string())
            .and_then(|a| a.validate());
        assert_eq!(res.is_err(), want_err, "{name}: {res:?}");
    }
    // The defaults are the tracker's.
    let a = parse(&[]).unwrap();
    assert_eq!(a.snapshot_upload_delay, st::DEFAULT_UPLOAD_DELAY);
    assert_eq!(a.snapshot_detach_delay, st::DEFAULT_DETACH_DELAY);
    assert_eq!(a.snapshot_upload_timeout, st::DEFAULT_UPLOAD_TIMEOUT);
    assert_eq!(
        a.snapshot_report_give_up_after,
        st::DEFAULT_REPORT_GIVE_UP_AFTER
    );
}

/// The throughput is between 0, adding no time for a snapshot's size, and
/// a cap keeping its byte rate from overflowing; it defaults to the
/// tracker's default in MiB/s.
#[test]
fn validate_snapshot_upload_est_throughput() {
    let default = st::DEFAULT_UPLOAD_EST_THROUGHPUT >> 20;
    for (mibps, want_err) in [
        (default, false),
        (1, false),
        (0, false),
        (MAX_UPLOAD_EST_MIBPS, false),
        (MAX_UPLOAD_EST_MIBPS + 1, true),
        (-1, true),
    ] {
        let a = parse(&[format!("--snapshot-upload-est-throughput-mib={mibps}")]).unwrap();
        assert_eq!(a.validate().is_err(), want_err, "{mibps}");
    }
    assert_eq!(default_of("snapshot-upload-est-throughput-mib"), "100");
}

/// Each worker count is at least 1 and defaults to the tracker's; a
/// negative count does not parse.
#[test]
fn validate_snapshot_worker_flags() {
    let d = (
        st::DEFAULT_UPLOAD_WORKERS as i64,
        st::DEFAULT_DETACH_WORKERS as i64,
        st::DEFAULT_REPORT_WORKERS as i64,
    );
    for (name, (u, de, r), want_err) in [
        ("defaults", d, false),
        ("single worker each", (1, 1, 1), false),
        ("zero upload workers", (0, 2, 1), true),
        ("negative upload workers", (-1, 2, 1), true),
        ("zero detach workers", (2, 0, 1), true),
        ("negative detach workers", (2, -1, 1), true),
        ("zero report workers", (2, 2, 0), true),
        ("negative report workers", (2, 2, -1), true),
    ] {
        let argv = vec![
            format!("--snapshot-upload-workers={u}"),
            format!("--snapshot-detach-workers={de}"),
            format!("--snapshot-report-workers={r}"),
        ];
        let res = parse(&argv)
            .map_err(|e| e.to_string())
            .and_then(|a| a.validate());
        assert_eq!(res.is_err(), want_err, "{name}: {res:?}");
    }
    for (flag, want) in [
        ("snapshot-upload-workers", st::DEFAULT_UPLOAD_WORKERS),
        ("snapshot-detach-workers", st::DEFAULT_DETACH_WORKERS),
        ("snapshot-report-workers", st::DEFAULT_REPORT_WORKERS),
    ] {
        assert_eq!(default_of(flag), want.to_string(), "--{flag}");
    }
}
