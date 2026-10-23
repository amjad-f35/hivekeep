//! The sandbox class under test and the installed fixtures that depend on
//! it.
//!
//! The suites are runtime-agnostic: `HK_E2E_SANDBOX_CLASS=microvm` repoints
//! every fixture at its micro-VM variant; unset means gVisor.

use std::time::Duration;

use crate::harness::T;
use crate::images::Image;
use crate::manifest::Substitutions;

/// Selects the runtime the suites build Actors on.
pub const SANDBOX_CLASS_ENV: &str = "HK_E2E_SANDBOX_CLASS";
/// Replaces the golden-snapshot wait budget.
pub const TEMPLATE_READY_TIMEOUT_ENV: &str = "HK_E2E_TEMPLATE_READY_TIMEOUT";

/// The micro-VM class, spelled as WorkerPool `spec.sandboxClasses[].name`.
pub const SANDBOX_CLASS_MICROVM: &str = "microvm";
/// The gVisor class, spelled likewise.
pub const SANDBOX_CLASS_GVISOR: &str = "gvisor";

/// A sandbox runtime family.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxClass {
    /// gVisor (the default).
    Gvisor,
    /// The micro-VM runtime.
    MicroVm,
}

impl SandboxClass {
    /// The class `value` of [`SANDBOX_CLASS_ENV`] selects.
    pub fn from_env_value(value: Option<&str>) -> Self {
        if value == Some(SANDBOX_CLASS_MICROVM) {
            SandboxClass::MicroVm
        } else {
            SandboxClass::Gvisor
        }
    }

    /// The class under test.
    pub fn current() -> Self {
        Self::from_env_value(std::env::var(SANDBOX_CLASS_ENV).ok().as_deref())
    }

    /// The WorkerPool spelling.
    pub fn name(self) -> &'static str {
        match self {
            SandboxClass::Gvisor => SANDBOX_CLASS_GVISOR,
            SandboxClass::MicroVm => SANDBOX_CLASS_MICROVM,
        }
    }
}

/// Whether the suites are pointed at the micro-VM fixtures. Assertions that
/// hold for one runtime only gate on this.
pub fn is_microvm() -> bool {
    SandboxClass::current() == SandboxClass::MicroVm
}

/// An installed WorkerPool + CRD-era template pair, by namespace and name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fixture {
    /// The pool's namespace (also the hive).
    pub namespace: String,
    /// The pool and template name.
    pub name: String,
    /// How the fixture is installed, for a missing-fixture failure.
    pub deploy_with: String,
}

/// An installed ActorTemplate (the API resource) plus the WorkerPool backing
/// it. Suites copy the resolved runtime out of them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HiveFixture {
    /// The template's hive.
    pub hive: String,
    /// The template's name.
    pub name: String,
    /// The WorkerPool's namespace.
    pub pool_namespace: String,
    /// The WorkerPool's name.
    pub pool_name: String,
    /// How the fixture is installed.
    pub deploy_with: String,
}

/// The counter demo for the sandbox class under test.
/// `HK_E2E_TEMPLATE_HIVE` / `HK_E2E_TEMPLATE_NAME` / `HK_E2E_POOL_NAMESPACE`
/// / `HK_E2E_POOL_NAME` relocate it.
pub fn counter_fixture() -> HiveFixture {
    counter_fixture_with(SandboxClass::current(), |k| std::env::var(k).ok())
}

/// [`counter_fixture`] for `class`, with overrides from `lookup`.
pub fn counter_fixture_with(
    class: SandboxClass,
    lookup: impl Fn(&str) -> Option<String>,
) -> HiveFixture {
    let mut f = match class {
        SandboxClass::Gvisor => HiveFixture {
            hive: "hivekeep-demo-counter".into(),
            name: "counter".into(),
            pool_namespace: "hivekeep-demo-counter".into(),
            pool_name: "counter".into(),
            deploy_with: "hivekeep-setup deploy demo-counter".into(),
        },
        SandboxClass::MicroVm => HiveFixture {
            hive: "hivekeep-demo-counter-microvm".into(),
            name: "counter-microvm".into(),
            pool_namespace: "hivekeep-demo-counter-microvm".into(),
            pool_name: "counter-microvm".into(),
            deploy_with: "hivekeep-setup deploy demo-counter-microvm".into(),
        },
    };
    let get = |k: &str| lookup(k).filter(|v| !v.is_empty());
    if let Some(v) = get("HK_E2E_TEMPLATE_HIVE") {
        f.hive = v;
    }
    if let Some(v) = get("HK_E2E_TEMPLATE_NAME") {
        f.name = v;
    }
    if let Some(v) = get("HK_E2E_POOL_NAMESPACE") {
        f.pool_namespace = v;
    }
    if let Some(v) = get("HK_E2E_POOL_NAME") {
        f.pool_name = v;
    }
    f
}

/// The egress demo for the sandbox class under test. Its Actors trust the
/// CA the egress gate mints its leaves from.
pub fn egress_fixture() -> Fixture {
    egress_fixture_for(SandboxClass::current())
}

/// [`egress_fixture`] for `class`.
pub fn egress_fixture_for(class: SandboxClass) -> Fixture {
    match class {
        SandboxClass::MicroVm => Fixture {
            namespace: "hivekeep-demo-egress-microvm".into(),
            name: "egress-microvm".into(),
            deploy_with: "hivekeep-setup deploy demo-egress-microvm".into(),
        },
        SandboxClass::Gvisor => Fixture {
            namespace: "hivekeep-demo-egress".into(),
            name: "egress".into(),
            deploy_with: "hivekeep-setup deploy demo-egress".into(),
        },
    }
}

/// Suffixes `base` for the sandbox class under test, so the gVisor and
/// micro-VM lanes, which run one after the other, never share a name (a
/// namespace still terminating from one lane would fail the next one's
/// apply). `${FIXTURE_SUFFIX}` does the same inside the manifests.
pub fn fixture_name(base: &str) -> String {
    fixture_name_for(SandboxClass::current(), base)
}

/// [`fixture_name`] for `class`.
pub fn fixture_name_for(class: SandboxClass, base: &str) -> String {
    match class {
        SandboxClass::MicroVm => format!("{base}-{SANDBOX_CLASS_MICROVM}"),
        SandboxClass::Gvisor => base.to_string(),
    }
}

/// How long to wait for an ActorTemplate's golden snapshot: a micro-VM
/// golden (a cold boot plus checkpoint, often on nested KVM) takes many
/// times a gVisor one. [`TEMPLATE_READY_TIMEOUT_ENV`] overrides it.
pub fn template_ready_timeout(t: &T) -> Duration {
    template_ready_timeout_with(
        SandboxClass::current(),
        std::env::var(TEMPLATE_READY_TIMEOUT_ENV).ok().as_deref(),
    )
    .unwrap_or_else(|e| t.fatal(e))
}

/// [`template_ready_timeout`] for `class` and an override value.
pub fn template_ready_timeout_with(
    class: SandboxClass,
    value: Option<&str>,
) -> Result<Duration, String> {
    if let Some(v) = value.filter(|v| !v.is_empty()) {
        return humantime::parse_duration(v)
            .map_err(|e| format!("invalid {TEMPLATE_READY_TIMEOUT_ENV} {v:?}: {e}"));
    }
    Ok(match class {
        SandboxClass::MicroVm => Duration::from_secs(600),
        SandboxClass::Gvisor => Duration::from_secs(90),
    })
}

/// The placeholders the pool-side fixture manifests carry, for `class`.
/// `name` distinguishes the caller (by convention its suite name) and joins
/// `${FIXTURE_SUFFIX}`: suites run concurrently, so each caller gets its
/// own copy of a fixture and no suite's cleanup deletes another's.
pub fn fixture_substitutions(class: SandboxClass, bucket: &str, name: &str) -> Substitutions {
    let mut s = Substitutions::default();
    s.inline("${BUCKET_NAME}", bucket);
    s.inline("${WORKER_IMAGE}", Image::Worker(class).reference());
    s.inline("${FIXTURE_SUFFIX}", suffix(class, name));
    s.block(
        "${WORKERPOOL_RUNTIME}",
        format!("  sandboxClasses:\n  - name: {}", class.name()),
    );
    // Off unless a caller opts in; see the trust-bundle probe options.
    s.block("${TEMPLATE_TRUST_BUNDLE}", "");
    s
}

/// The placeholders the protojson ActorTemplate manifests carry, for
/// `class`. The inline values match [`fixture_substitutions`], so one
/// (bucket, name) pair renders both halves of a fixture. `trust_bundles`
/// names the bundles projected into one `trust-bundle.pem`.
pub fn hive_template_substitutions(
    class: SandboxClass,
    bucket: &str,
    name: &str,
    trust_bundles: &[String],
) -> Substitutions {
    let mut s = Substitutions::default();
    s.inline("${BUCKET_NAME}", bucket);
    s.inline("${FIXTURE_SUFFIX}", suffix(class, name));
    s.inline("${PROBE_IMAGE}", Image::Probe.reference());
    s.inline("${TESTSERVER_IMAGE}", Image::Testserver.reference());
    if trust_bundles.is_empty() {
        s.block("${TEMPLATE_TRUST_BUNDLE}", "");
    } else {
        // Indented to sit in a template's systemInfo dataSources list. Each
        // name must be on the keeper's supported-bundle allowlist.
        let mut block = String::from("    - trustBundle:\n        names:\n");
        for bundle in trust_bundles {
            block.push_str(&format!("        - {bundle}\n"));
        }
        block.push_str("        path: trust-bundle.pem");
        s.block("${TEMPLATE_TRUST_BUNDLE}", block);
    }
    s
}

fn suffix(class: SandboxClass, name: &str) -> String {
    match class {
        SandboxClass::MicroVm => format!("-{SANDBOX_CLASS_MICROVM}-{name}"),
        SandboxClass::Gvisor => format!("-{name}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn egress_fixture_follows_the_class() {
        let g = egress_fixture_for(SandboxClass::from_env_value(Some("")));
        assert_eq!(
            (g.namespace.as_str(), g.name.as_str()),
            ("hivekeep-demo-egress", "egress")
        );
        let m = egress_fixture_for(SandboxClass::from_env_value(Some(SANDBOX_CLASS_MICROVM)));
        assert_eq!(
            (m.namespace.as_str(), m.name.as_str()),
            ("hivekeep-demo-egress-microvm", "egress-microvm")
        );
    }

    #[test]
    fn counter_fixture_cases() {
        let none = |_: &str| None;
        // gvisor
        assert_eq!(
            counter_fixture_with(SandboxClass::Gvisor, none),
            HiveFixture {
                hive: "hivekeep-demo-counter".into(),
                name: "counter".into(),
                pool_namespace: "hivekeep-demo-counter".into(),
                pool_name: "counter".into(),
                deploy_with: "hivekeep-setup deploy demo-counter".into(),
            }
        );
        // microvm
        assert_eq!(
            counter_fixture_with(SandboxClass::MicroVm, none),
            HiveFixture {
                hive: "hivekeep-demo-counter-microvm".into(),
                name: "counter-microvm".into(),
                pool_namespace: "hivekeep-demo-counter-microvm".into(),
                pool_name: "counter-microvm".into(),
                deploy_with: "hivekeep-setup deploy demo-counter-microvm".into(),
            }
        );
        // explicit override wins
        let env: HashMap<&str, &str> = [
            ("HK_E2E_TEMPLATE_HIVE", "elsewhere"),
            ("HK_E2E_TEMPLATE_NAME", "other"),
            ("HK_E2E_POOL_NAMESPACE", "pool-ns"),
            ("HK_E2E_POOL_NAME", "pool"),
        ]
        .into();
        let got =
            counter_fixture_with(SandboxClass::MicroVm, |k| env.get(k).map(|v| v.to_string()));
        assert_eq!(
            (
                got.hive.as_str(),
                got.name.as_str(),
                got.pool_namespace.as_str(),
                got.pool_name.as_str()
            ),
            ("elsewhere", "other", "pool-ns", "pool")
        );
    }

    #[test]
    fn ready_timeout_defaults_and_override() {
        assert_eq!(
            template_ready_timeout_with(SandboxClass::Gvisor, None),
            Ok(Duration::from_secs(90))
        );
        assert_eq!(
            template_ready_timeout_with(SandboxClass::MicroVm, None),
            Ok(Duration::from_secs(600))
        );
        assert_eq!(
            template_ready_timeout_with(SandboxClass::MicroVm, Some("2s")),
            Ok(Duration::from_secs(2))
        );
        assert!(template_ready_timeout_with(SandboxClass::Gvisor, Some("soon")).is_err());
    }

    #[test]
    fn fixture_names() {
        assert_eq!(fixture_name_for(SandboxClass::Gvisor, "suite"), "suite");
        assert_eq!(
            fixture_name_for(SandboxClass::MicroVm, "suite"),
            "suite-microvm"
        );
    }
}
