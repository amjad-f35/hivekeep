//! Field defaults of the public API resources, applied in place.
//!
//! Defaults live apart from validation so both the RPC handlers (before
//! validating input) and the store (when backfilling records on read) can
//! apply them.

use hivekeep_proto::api::v1 as pb;

/// Default wakeup probe timeout.
pub const DEFAULT_WAKEUP_PROBE_TIMEOUT_SECONDS: i32 = 30;
/// Default wakeup probe path.
pub const DEFAULT_WAKEUP_PROBE_PATH: &str = "/";
/// Port an http rule matches when it names none.
pub const DEFAULT_HTTP_PORT: i32 = 80;
/// Port an https rule matches when it names none.
pub const DEFAULT_HTTPS_PORT: i32 = 443;

/// A resource message that has defaults.
pub trait Defaults {
    /// Fills unset fields with their defaults.
    fn apply_defaults(&mut self);
}

/// Applies the defaults of `resource` in place.
pub fn apply<T: Defaults>(resource: &mut T) {
    resource.apply_defaults();
}

impl Defaults for pb::ActorTemplate {
    fn apply_defaults(&mut self) {
        // An omitted snapshot_config takes every default.
        let sc = self.snapshot_config.get_or_insert_with(Default::default);
        apply_snapshot_config(sc);
        if let Some(policy) = &mut self.golden_egress_policy {
            apply_egress_rules(&mut policy.rules);
        }
        for c in &mut self.containers {
            apply_container(c);
        }
    }
}

impl Defaults for pb::EgressPolicy {
    fn apply_defaults(&mut self) {
        apply_egress_rules(&mut self.rules);
    }
}

macro_rules! no_defaults {
    ($($t:ty),*) => {
        $(impl Defaults for $t {
            fn apply_defaults(&mut self) {}
        })*
    };
}

no_defaults!(pb::AccessPolicy, pb::Actor, pb::Hive, pb::Tag, pb::Worker);

fn apply_snapshot_config(sc: &mut pb::SnapshotConfig) {
    if sc.preferred_fidelity == pb::SnapshotFidelity::Unspecified as i32 {
        sc.preferred_fidelity = pb::SnapshotFidelity::Memory as i32;
    }
    let golden = sc
        .golden_snapshot_config
        .get_or_insert_with(Default::default);
    if golden.mode == pb::GoldenSnapshotMode::Unspecified as i32 {
        golden.mode = pb::GoldenSnapshotMode::Enabled as i32;
    }
}

fn apply_container(c: &mut pb::Container) {
    let Some(probe) = &mut c.wakeup_probe else {
        return;
    };
    if probe.timeout_seconds == 0 {
        probe.timeout_seconds = DEFAULT_WAKEUP_PROBE_TIMEOUT_SECONDS;
    }
    if let Some(get) = &mut probe.http_get {
        if get.path.is_empty() {
            get.path = DEFAULT_WAKEUP_PROBE_PATH.to_string();
        }
    }
}

/// An http or https rule that names no ports matches its protocol's
/// well-known port. tls_passthrough has no default; validation requires
/// its ports.
fn apply_egress_rules(rules: &mut [pb::EgressRule]) {
    for r in rules {
        if let Some(http) = &mut r.http {
            http.ports.get_or_insert_with(|| pb::Ports {
                all: None,
                numbers: vec![DEFAULT_HTTP_PORT],
            });
        } else if let Some(https) = &mut r.https {
            https.ports.get_or_insert_with(|| pb::Ports {
                all: None,
                numbers: vec![DEFAULT_HTTPS_PORT],
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn defaulted<T: Defaults>(mut v: T) -> T {
        apply(&mut v);
        v
    }

    fn full_snapshot_config() -> pb::SnapshotConfig {
        pb::SnapshotConfig {
            preferred_fidelity: pb::SnapshotFidelity::Memory as i32,
            golden_snapshot_config: Some(pb::GoldenSnapshotConfig {
                mode: pb::GoldenSnapshotMode::Enabled as i32,
            }),
            ..Default::default()
        }
    }

    #[test]
    fn snapshot_config_defaults() {
        let t = defaulted(pb::ActorTemplate::default());
        assert_eq!(t.snapshot_config, Some(full_snapshot_config()));
        let t = defaulted(pb::ActorTemplate {
            snapshot_config: Some(Default::default()),
            ..Default::default()
        });
        assert_eq!(t.snapshot_config, Some(full_snapshot_config()));
        let set = pb::SnapshotConfig {
            preferred_fidelity: pb::SnapshotFidelity::Volumes as i32,
            golden_snapshot_config: Some(pb::GoldenSnapshotConfig {
                mode: pb::GoldenSnapshotMode::Disabled as i32,
            }),
            storage_location: "gs://b/r".into(),
        };
        let t = defaulted(pb::ActorTemplate {
            snapshot_config: Some(set.clone()),
            ..Default::default()
        });
        assert_eq!(t.snapshot_config, Some(set));
    }

    #[test]
    fn container_defaults() {
        let t = defaulted(pb::ActorTemplate {
            containers: vec![
                pb::Container {
                    name: "none".into(),
                    ..Default::default()
                },
                pb::Container {
                    name: "probe".into(),
                    wakeup_probe: Some(pb::WakeupProbe {
                        http_get: Some(pb::HttpGetAction {
                            port: 8080,
                            ..Default::default()
                        }),
                        timeout_seconds: 0,
                    }),
                    ..Default::default()
                },
                pb::Container {
                    name: "bare".into(),
                    wakeup_probe: Some(pb::WakeupProbe::default()),
                    ..Default::default()
                },
                pb::Container {
                    name: "set".into(),
                    wakeup_probe: Some(pb::WakeupProbe {
                        http_get: Some(pb::HttpGetAction {
                            path: "/ready".into(),
                            port: 1,
                        }),
                        timeout_seconds: 5,
                    }),
                    ..Default::default()
                },
            ],
            ..Default::default()
        });
        assert_eq!(t.containers[0].wakeup_probe, None);
        let p = t.containers[1].wakeup_probe.as_ref().unwrap();
        assert_eq!(
            (
                p.timeout_seconds,
                p.http_get.as_ref().unwrap().path.as_str()
            ),
            (30, "/")
        );
        assert_eq!(
            t.containers[2].wakeup_probe,
            Some(pb::WakeupProbe {
                http_get: None,
                timeout_seconds: 30
            })
        );
        let p = t.containers[3].wakeup_probe.as_ref().unwrap();
        assert_eq!(
            (
                p.timeout_seconds,
                p.http_get.as_ref().unwrap().path.as_str()
            ),
            (5, "/ready")
        );
    }

    #[test]
    fn egress_port_defaults() {
        let rules = vec![
            pb::EgressRule {
                http: Some(pb::HttpRule::default()),
                ..Default::default()
            },
            pb::EgressRule {
                https: Some(pb::HttpsRule::default()),
                ..Default::default()
            },
            pb::EgressRule {
                tls_passthrough: Some(pb::TlsPassthroughRule::default()),
                ..Default::default()
            },
            pb::EgressRule {
                http: Some(pb::HttpRule {
                    ports: Some(pb::Ports {
                        all: Some(pb::AllPorts {}),
                        numbers: vec![],
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ];
        let p = defaulted(pb::EgressPolicy {
            rules: rules.clone(),
            ..Default::default()
        });
        assert_eq!(
            p.rules[0]
                .http
                .as_ref()
                .unwrap()
                .ports
                .as_ref()
                .unwrap()
                .numbers,
            vec![80]
        );
        assert_eq!(
            p.rules[1]
                .https
                .as_ref()
                .unwrap()
                .ports
                .as_ref()
                .unwrap()
                .numbers,
            vec![443]
        );
        assert_eq!(p.rules[2].tls_passthrough.as_ref().unwrap().ports, None);
        assert!(p.rules[3]
            .http
            .as_ref()
            .unwrap()
            .ports
            .as_ref()
            .unwrap()
            .all
            .is_some());
        let t = defaulted(pb::ActorTemplate {
            golden_egress_policy: Some(pb::EgressPolicyTemplate { rules }),
            ..Default::default()
        });
        assert_eq!(
            t.golden_egress_policy.unwrap().rules[0]
                .http
                .as_ref()
                .unwrap()
                .ports
                .as_ref()
                .unwrap()
                .numbers,
            vec![80]
        );
    }

    #[test]
    fn resources_without_defaults_are_untouched() {
        let a = pb::Actor {
            metadata: Some(pb::ResourceMetadata {
                name: "a".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(defaulted(a.clone()), a);
        assert_eq!(defaulted(pb::Hive::default()), pb::Hive::default());
        assert_eq!(defaulted(pb::Tag::default()), pb::Tag::default());
        assert_eq!(defaulted(pb::Worker::default()), pb::Worker::default());
        assert_eq!(
            defaulted(pb::AccessPolicy::default()),
            pb::AccessPolicy::default()
        );
    }
}
