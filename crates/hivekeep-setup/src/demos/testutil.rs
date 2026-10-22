//! Fixtures the demo tests share: an Env needing no cluster, and checks that
//! a rendered template is a complete manifest.

use crate::config::{self, resolve, ResolveOptions};
use crate::kube::decode_manifest_bytes;
use crate::steps::hivekeep::actor_template_from_manifest;
use crate::steps::Env;
use hivekeep_core::resource::ActorTemplateRef;
use std::collections::BTreeMap;

/// An Env with no cluster, resolved from `env` so a test can steer a demo's
/// own settings the way an operator would with an export.
pub(crate) fn env_with(vars: &BTreeMap<String, String>) -> Env {
    super::register_settings();
    let r = resolve(
        None,
        ResolveOptions {
            file: None,
            env: Some(vars),
        },
    )
    .expect("resolve");
    let mut cfg = config::test_config();
    cfg.bucket_name = "hivekeep-snapshots".into();
    cfg.set_resolved(r);
    Env::local(cfg)
}

pub(crate) fn env() -> Env {
    env_with(&BTreeMap::new())
}

/// No non-comment line still holds a `${PLACEHOLDER}`.
fn assert_no_placeholders(manifest: &[u8]) {
    for (i, line) in String::from_utf8_lossy(manifest).lines().enumerate() {
        if line.trim_start().starts_with('#') {
            continue;
        }
        assert!(
            !line.contains("${"),
            "line {} still contains a placeholder: {line}",
            i + 1
        );
    }
}

/// Every placeholder resolved, and the result a non-empty manifest stream.
pub(crate) fn assert_rendered(manifest: &[u8]) {
    assert_no_placeholders(manifest);
    let objs = decode_manifest_bytes(manifest).expect("decode");
    assert!(!objs.is_empty(), "rendered manifest decoded to no objects");
}

/// Every placeholder resolved, and the result strictly parses into the
/// ActorTemplate it will be created as, naming `want`.
pub(crate) fn assert_rendered_actor_template(manifest: &[u8], want: &ActorTemplateRef) {
    assert_no_placeholders(manifest);
    let template = actor_template_from_manifest(manifest).expect("actor template");
    assert_eq!(&ActorTemplateRef::of(&template), want);
}
