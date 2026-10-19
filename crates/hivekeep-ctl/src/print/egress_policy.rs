//! Egress policies: `HIVE ACTOR RULES VERSION AGE`. JSON and YAML emit the
//! bare policy, which does not name its Actor, so the caller passes it.

use std::io::Write;

use hivekeep_proto::api::v1 as pb;

use crate::error::Result;
use crate::output::Printer;
use crate::table::Table;

/// Prints the egress policy of `actor`.
pub fn print(
    p: &Printer,
    out: &mut dyn Write,
    actor: &str,
    policy: &pb::EgressPolicy,
) -> Result<()> {
    p.write_one(out, policy, || {
        let m = policy.metadata.clone().unwrap_or_default();
        let mut t = Table::new(["HIVE", "ACTOR", "RULES", "VERSION", "AGE"]);
        t.row([
            m.hive.clone(),
            actor.to_string(),
            policy.rules.len().to_string(),
            m.version.to_string(),
            p.age(m.create_time.as_ref()),
        ]);
        t
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::age::timestamp_before;
    use crate::io::SharedBuf;
    use crate::output::OutputFormat;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn now() -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(1_767_268_800)
    }

    fn policy() -> pb::EgressPolicy {
        pb::EgressPolicy {
            metadata: Some(pb::ResourceMetadata {
                hive: "team-a".into(),
                name: "default".into(),
                uid: "u-1".into(),
                version: 3,
                create_time: Some(timestamp_before(now(), Duration::from_secs(300))),
                ..Default::default()
            }),
            rules: vec![pb::EgressRule::default(), pb::EgressRule::default()],
        }
    }

    #[test]
    fn table() {
        let mut buf = SharedBuf::default();
        print(
            &Printer::at(OutputFormat::Table, now()),
            &mut buf,
            "act-1",
            &policy(),
        )
        .unwrap();
        assert_eq!(
            buf.contents(),
            "HIVE     ACTOR   RULES   VERSION   AGE\nteam-a   act-1   2       3         5m\n"
        );
    }

    #[test]
    fn yaml_is_the_bare_policy() {
        let mut buf = SharedBuf::default();
        print(
            &Printer::at(OutputFormat::Yaml, now()),
            &mut buf,
            "act-1",
            &policy(),
        )
        .unwrap();
        let got = buf.contents();
        assert!(got.starts_with("metadata:\n"), "{got}");
        assert!(
            got.contains("uid: u-1") && got.contains("version: \"3\""),
            "{got}"
        );
        assert!(!got.contains("act-1"), "{got}");
    }
}
