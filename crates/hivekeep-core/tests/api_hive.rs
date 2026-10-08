//! Hive request validation.

mod common;

use common::*;
use hivekeep_core::field::ErrorList;
use hivekeep_core::validation::api::*;
use hivekeep_core::validation::Operation;
use hivekeep_proto::api::v1 as pb;

fn run(cases: Vec<(&str, ErrorList, Vec<Want>)>) {
    for (name, got, want) in cases {
        assert_errs(name, got, &want);
    }
}

fn oref(hive: &str, name: &str) -> Option<pb::ObjectRef> {
    Some(pb::ObjectRef {
        hive: hive.into(),
        name: name.into(),
    })
}

fn valid_hive() -> pb::Hive {
    pb::Hive {
        metadata: Some(pb::ResourceMetadata {
            name: "team1".into(),
            ..Default::default()
        }),
    }
}

#[test]
fn create_hive_request() {
    let c = |hive: Option<pb::Hive>| {
        validate_create_hive_request(&Operation::create(), &pb::CreateHiveRequest { hive })
    };
    let m = |f: fn(&mut pb::ResourceMetadata)| {
        Some(with(valid_hive(), |h| f(h.metadata.as_mut().unwrap())))
    };
    run(vec![
        ("valid", c(Some(valid_hive())), vec![]),
        ("missing hive", c(None), vec![required("hive")]),
        (
            "missing hive.metadata",
            c(Some(with(valid_hive(), |h| h.metadata = None))),
            vec![required("hive.metadata")],
        ),
        (
            "hive.metadata.hive must be empty",
            c(m(|m| m.hive = "as".into())),
            vec![forbidden("hive.metadata.hive")],
        ),
        (
            "missing hive.metadata.name",
            c(m(|m| m.name.clear())),
            vec![required("hive.metadata.name")],
        ),
        (
            "invalid metadata.name",
            c(m(|m| m.name = "invalid value".into())),
            vec![dns_label("hive.metadata.name")],
        ),
    ]);
}

/// The ref cases shared by Get and Delete.
fn ref_cases(f: impl Fn(Option<pb::ObjectRef>) -> ErrorList) {
    run(vec![
        ("valid", f(oref("", "team1")), vec![]),
        ("missing hive", f(None), vec![required("hive")]),
        (
            "hive.hive must be empty",
            f(oref("as", "team1")),
            vec![forbidden("hive.hive")],
        ),
        (
            "missing hive.name",
            f(oref("", "")),
            vec![required("hive.name")],
        ),
        (
            "invalid hive.name",
            f(oref("", "invalid value")),
            vec![dns_label("hive.name")],
        ),
    ]);
}

#[test]
fn get_hive_request() {
    ref_cases(|hive| validate_get_hive_request(&Operation::create(), &pb::GetHiveRequest { hive }));
}

#[test]
fn list_hives_request() {
    let c = |options: Option<pb::ListOptions>| {
        validate_list_hives_request(&Operation::create(), &pb::ListHivesRequest { options })
    };
    let size = |page_size| {
        Some(pb::ListOptions {
            page_size,
            ..Default::default()
        })
    };
    let token = |n: usize| {
        Some(pb::ListOptions {
            page_token: "x".repeat(n),
            ..Default::default()
        })
    };
    run(vec![
        ("valid, no page_size", c(None), vec![]),
        ("valid, positive page_size", c(size(10)), vec![]),
        (
            "negative page_size",
            c(size(-1)),
            vec![invalid_by("options.page_size", "minimum")],
        ),
        ("valid page_token", c(token(256)), vec![]),
        (
            "too-large page_token",
            c(token(257)),
            vec![too_long("options.page_token")],
        ),
    ]);
}

#[test]
fn delete_hive_request() {
    ref_cases(|hive| {
        validate_delete_hive_request(
            &Operation::create(),
            &pb::DeleteHiveRequest {
                hive,
                ..Default::default()
            },
        )
    });
}
