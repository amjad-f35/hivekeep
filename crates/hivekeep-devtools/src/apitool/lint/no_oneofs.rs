//! The API uses no explicit `oneof` groups.

use crate::apitool::model::{append_name, Api};

use super::method::for_each_field;
use super::{CheckResult, Finding, Rule};

pub const NO_ONEOFS: Rule = Rule {
    name: "no-oneofs",
    description: "No field belongs to an explicit oneof group.",
    check: check_no_oneofs,
};

fn check_no_oneofs(api: &Api) -> CheckResult {
    for_each_field(api, |m, f| {
        if f.oneof_name.is_empty() {
            return vec![];
        }
        vec![Finding::new(
            append_name(&m.full_name, &f.name),
            format!(
                "belongs to oneof {:?} - oneofs are not used in this API",
                f.oneof_name
            ),
        )]
    })
}

#[cfg(test)]
mod tests {
    use super::super::fixtures::*;
    use super::*;
    use crate::apitool::model::Field;

    #[test]
    fn no_oneofs() {
        let cases = vec![
            ("plain field", named("gadget"), vec![]),
            (
                "proto3 optional scalar - not a real oneof",
                Field {
                    proto3_optional: true,
                    ..named("gadget")
                },
                vec![],
            ),
            (
                "member of a real oneof",
                Field {
                    oneof_name: "reference".into(),
                    ..named("gadget")
                },
                findings(
                    "test.Widget.gadget",
                    &[r#"belongs to oneof "reference" - oneofs are not used in this API"#],
                ),
            ),
        ];
        for (name, field, want) in cases {
            let api = messages_api(vec![msg("Widget", vec![field])]);
            assert_eq!((NO_ONEOFS.check)(&api).unwrap(), want, "{name}");
        }
    }
}
