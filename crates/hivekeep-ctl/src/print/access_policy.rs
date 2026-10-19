//! Access policies: one `SCOPE ROLE MEMBERS` row per binding. JSON and YAML
//! emit the bare policy. Its metadata names no hive, so the caller passes
//! the scope; an empty hive is the global policy.

use std::io::Write;

use hivekeep_proto::api::v1 as pb;

use crate::error::Result;
use crate::output::Printer;
use crate::table::Table;

/// Prints the access policy of `hive`, or the global one when it is empty.
pub fn print(
    p: &Printer,
    out: &mut dyn Write,
    hive: &str,
    policy: &pb::AccessPolicy,
) -> Result<()> {
    p.write_one(out, policy, || {
        let scope = if hive.is_empty() { "<global>" } else { hive };
        let mut t = Table::new(["SCOPE", "ROLE", "MEMBERS"]);
        if policy.bindings.is_empty() {
            t.row([scope, "<none>", "<none>"]);
        }
        for b in &policy.bindings {
            t.row([scope.to_string(), b.role.clone(), b.members.join(",")]);
        }
        t
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::SharedBuf;
    use crate::output::OutputFormat;

    fn policy() -> pb::AccessPolicy {
        pb::AccessPolicy {
            metadata: Some(pb::ResourceMetadata {
                name: "default".into(),
                uid: "u-1".into(),
                version: 2,
                ..Default::default()
            }),
            bindings: vec![
                pb::Binding {
                    role: "owner".into(),
                    members: vec!["user:alice".into(), "user:bob".into()],
                },
                pb::Binding {
                    role: "viewer".into(),
                    members: vec!["user:carol".into()],
                },
            ],
        }
    }

    fn render(format: OutputFormat, hive: &str, policy: &pb::AccessPolicy) -> String {
        let mut buf = SharedBuf::default();
        print(&Printer::new(format), &mut buf, hive, policy).unwrap();
        buf.contents()
    }

    #[test]
    fn tables() {
        assert_eq!(
            render(OutputFormat::Table, "team-a", &policy()),
            "SCOPE    ROLE     MEMBERS\nteam-a   owner    user:alice,user:bob\nteam-a   viewer   user:carol\n"
        );
        assert!(render(OutputFormat::Table, "", &policy()).contains("<global>   owner"));
        assert!(
            render(OutputFormat::Table, "team-a", &pb::AccessPolicy::default())
                .contains("team-a   <none>   <none>")
        );
    }

    #[test]
    fn yaml_carries_preconditions() {
        let got = render(OutputFormat::Yaml, "team-a", &policy());
        for want in ["uid: u-1", "version: \"2\"", "role: owner"] {
            assert!(got.contains(want), "{got:?} lacks {want:?}");
        }
    }
}
