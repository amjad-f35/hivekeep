//! Admission-time validation of the `hivekeep.io/v1` resources.
//!
//! The rules here mirror what the API server enforces from the generated
//! CRD schemas (OpenAPI constraints and `x-kubernetes-validations` rules)
//! and from the shipped SandboxConfig admission policy, with messages in
//! the API server's shape. They work on the JSON form of an object, as the
//! API server does, so objects a typed struct could not represent (an
//! unknown enum value, a missing required field) are judged too. Use them
//! to check manifests before applying them, and to test the rules.

use std::collections::HashSet;
use std::fmt;

use serde_json::Value;

use super::group::is_reserved_key;
use super::sandbox_class::SandboxClass;
use super::sandboxconfig::{GVISOR_REQUIRED_ASSETS, MAX_VERSIONS, MICROVM_REQUIRED_ASSETS};
use super::workerpool::{MAX_NAME_LEN, MAX_TEMPLATE_METADATA, MAX_TOLERATIONS};

/// Message of the gVisor asset policy rule.
pub const GVISOR_ASSETS_MSG: &str = "a gvisor SandboxConfig must define a 'gvisor' (release tarball) asset for every architecture of every version under spec.versions[].assets";
/// Message of the micro-VM asset policy rule.
pub const MICROVM_ASSETS_MSG: &str = "a microvm SandboxConfig must define cloud-hypervisor, virtiofsd, kata-kernel, and kata-image assets for every architecture of every version under spec.versions[].assets";
const RESERVED_MSG: &str = "hivekeep.io and its subdomains are reserved";

/// The field errors of a rejected object.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ValidationErrors(pub Vec<String>);

impl fmt::Display for ValidationErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}]", self.0.join(", "))
    }
}

impl std::error::Error for ValidationErrors {}

impl ValidationErrors {
    fn push(&mut self, path: &str, msg: impl fmt::Display) {
        self.0.push(format!("{path}: {msg}"));
    }

    fn into_result(self) -> Result<(), ValidationErrors> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(self)
        }
    }
}

fn str_at<'a>(v: &'a Value, key: &str) -> Option<&'a str> {
    v.get(key).and_then(Value::as_str)
}

fn check_string(
    errs: &mut ValidationErrors,
    path: &str,
    v: Option<&Value>,
    min: usize,
    max: usize,
    required: bool,
) {
    match v {
        None | Some(Value::Null) => {
            if required {
                errs.push(path, "Required value");
            }
        }
        Some(Value::String(s)) => {
            let n = s.chars().count();
            if n < min {
                errs.push(
                    path,
                    format!(
                        "Invalid value: {s:?}: {path} in body should be at least {min} chars long"
                    ),
                );
            }
            if n > max {
                errs.push(path, format!("Too long: may not be more than {max} bytes"));
            }
        }
        Some(other) => errs.push(path, format!("Invalid value: {other}: must be a string")),
    }
}

fn check_class(errs: &mut ValidationErrors, path: &str, v: Option<&Value>, required: bool) {
    match v.and_then(Value::as_str) {
        None if required => errs.push(path, "Required value"),
        None => {}
        Some(s) if SandboxClass::parse(s).is_none() => errs.push(
            path,
            format!("Unsupported value: {s:?}: supported values: \"gvisor\", \"microvm\""),
        ),
        Some(_) => {}
    }
}

fn check_metadata_map(
    errs: &mut ValidationErrors,
    path: &str,
    v: Option<&Value>,
    what: &str,
    values_are_labels: bool,
) {
    let Some(map) = v.and_then(Value::as_object) else {
        return;
    };
    if map.len() > MAX_TEMPLATE_METADATA {
        errs.push(
            path,
            format!(
                "Too many: {}: must have at most {MAX_TEMPLATE_METADATA} items",
                map.len()
            ),
        );
    }
    if map.keys().any(|k| is_reserved_key(k)) {
        errs.push(path, format!("Invalid value: \"object\": {RESERVED_MSG}"));
    }
    if map
        .keys()
        .any(|k| !hivekeep_core::names::check_label_key(k).is_empty())
    {
        errs.push(
            path,
            format!(
                "Invalid value: \"object\": {what} keys must be valid Kubernetes qualified names"
            ),
        );
    }
    if values_are_labels {
        for (k, val) in map {
            let s = val.as_str().unwrap_or_default();
            if s.len() > 63 || !hivekeep_core::names::check_label_value(s).is_empty() {
                errs.push(
                    &format!("{path}.{k}"),
                    format!(
                        "Invalid value: {s:?}: {path}.{k} in body should match '^(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?$'"
                    ),
                );
            }
        }
    }
}

/// Validates a WorkerPool, given as JSON; `old` is the stored object on an
/// update.
pub fn validate_worker_pool(obj: &Value, old: Option<&Value>) -> Result<(), ValidationErrors> {
    let mut errs = ValidationErrors::default();
    let name = obj
        .pointer("/metadata/name")
        .and_then(Value::as_str)
        .unwrap_or("");
    if name.chars().count() > MAX_NAME_LEN {
        errs.push(
            "<root>",
            "Invalid value: \"object\": metadata.name must be at most 63 characters, because worker pods use it as a label value",
        );
    }
    let spec = obj.get("spec").unwrap_or(&Value::Null);
    match spec.get("replicas") {
        None | Some(Value::Null) => errs.push("spec.replicas", "Required value"),
        Some(r) => {
            if r.as_i64().is_some_and(|r| r < 0) {
                errs.push(
                    "spec.replicas",
                    format!("Invalid value: {r}: spec.replicas in body should be greater than or equal to 0"),
                );
            }
        }
    }
    check_string(
        &mut errs,
        "spec.workerImage",
        spec.get("workerImage"),
        1,
        usize::MAX,
        true,
    );

    match spec.get("sandboxClasses").and_then(Value::as_array) {
        None => errs.push("spec.sandboxClasses", "Required value"),
        Some(classes) => {
            if classes.is_empty() {
                errs.push(
                    "spec.sandboxClasses",
                    "Invalid value: 0: spec.sandboxClasses in body should have at least 1 items",
                );
            }
            if classes.len() > 1 {
                errs.push(
                    "spec.sandboxClasses",
                    format!("Too many: {}: must have at most 1 items", classes.len()),
                );
            }
            let mut seen = HashSet::new();
            for (i, c) in classes.iter().enumerate() {
                let path = format!("spec.sandboxClasses[{i}]");
                check_class(&mut errs, &format!("{path}.name"), c.get("name"), true);
                if let Some(n) = str_at(c, "name") {
                    if !seen.insert(n.to_string()) {
                        errs.push(&path, format!("Duplicate value: {{\"name\":{n:?}}}"));
                    }
                }
                if let Some(r) = c.get("configRef").filter(|r| !r.is_null()) {
                    check_string(
                        &mut errs,
                        &format!("{path}.configRef.name"),
                        r.get("name"),
                        1,
                        253,
                        true,
                    );
                }
            }
            if let Some(old_classes) = old
                .and_then(|o| o.pointer("/spec/sandboxClasses"))
                .and_then(Value::as_array)
            {
                let names = |l: &[Value]| {
                    l.iter()
                        .map(|c| str_at(c, "name").unwrap_or("").to_string())
                        .collect::<Vec<_>>()
                };
                if names(classes) != names(old_classes) {
                    errs.push(
                        "spec.sandboxClasses",
                        "Invalid value: \"array\": sandboxClasses names are immutable",
                    );
                }
            }
        }
    }

    if let Some(t) = spec.get("template").filter(|t| !t.is_null()) {
        check_metadata_map(
            &mut errs,
            "spec.template.labels",
            t.get("labels"),
            "label",
            true,
        );
        check_metadata_map(
            &mut errs,
            "spec.template.annotations",
            t.get("annotations"),
            "annotation",
            false,
        );
        if let Some(tol) = t.get("tolerations").and_then(Value::as_array) {
            if tol.len() > MAX_TOLERATIONS {
                errs.push(
                    "spec.template.tolerations",
                    format!(
                        "Too many: {}: must have at most {MAX_TOLERATIONS} items",
                        tol.len()
                    ),
                );
            }
        }
        if let Some(sa) = t.get("serviceAccountName").filter(|v| !v.is_null()) {
            check_string(
                &mut errs,
                "spec.template.serviceAccountName",
                Some(sa),
                1,
                usize::MAX,
                false,
            );
        }
    }
    errs.into_result()
}

/// Applies the SandboxConfig schema defaults: `sandboxClass: gvisor` and
/// `state: Enabled` on each version.
pub fn default_sandbox_config(obj: &mut Value) {
    let Some(spec) = obj.get_mut("spec").and_then(Value::as_object_mut) else {
        return;
    };
    if spec
        .get("sandboxClass")
        .is_none_or(|v| v.is_null() || v.as_str() == Some(""))
    {
        spec.insert("sandboxClass".into(), SandboxClass::Gvisor.as_str().into());
    }
    if let Some(versions) = spec.get_mut("versions").and_then(Value::as_array_mut) {
        for v in versions.iter_mut().filter_map(Value::as_object_mut) {
            v.entry("state").or_insert_with(|| "Enabled".into());
        }
    }
}

fn is_sha256_hex(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn is_dns_label(s: &str) -> bool {
    let b = s.as_bytes();
    !b.is_empty()
        && b.len() <= 63
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
        && b[0] != b'-'
        && b[b.len() - 1] != b'-'
}

/// Validates a SandboxConfig against its CRD schema and rules, after
/// applying schema defaults. `old` is the stored object on an update.
pub fn validate_sandbox_config_schema(
    obj: &Value,
    _old: Option<&Value>,
) -> Result<(), ValidationErrors> {
    let mut obj = obj.clone();
    default_sandbox_config(&mut obj);
    let mut errs = ValidationErrors::default();
    let spec = obj.get("spec").unwrap_or(&Value::Null);
    check_class(
        &mut errs,
        "spec.sandboxClass",
        spec.get("sandboxClass"),
        false,
    );
    check_string(
        &mut errs,
        "spec.defaultVersion",
        spec.get("defaultVersion"),
        1,
        63,
        true,
    );
    let empty = Vec::new();
    let versions = spec.get("versions").and_then(Value::as_array);
    match versions {
        None => errs.push("spec.versions", "Required value"),
        Some(v) if v.is_empty() => errs.push(
            "spec.versions",
            "Invalid value: 0: spec.versions in body should have at least 1 items",
        ),
        Some(v) if v.len() > MAX_VERSIONS => errs.push(
            "spec.versions",
            format!(
                "Too many: {}: must have at most {MAX_VERSIONS} items",
                v.len()
            ),
        ),
        _ => {}
    }
    let versions = versions.unwrap_or(&empty);
    let mut seen = HashSet::new();
    for (i, v) in versions.iter().enumerate() {
        let path = format!("spec.versions[{i}]");
        let name = str_at(v, "name").unwrap_or("");
        if name.is_empty() {
            errs.push(&format!("{path}.name"), "Required value");
        } else if !is_dns_label(name) {
            errs.push(
                &format!("{path}.name"),
                format!("Invalid value: {name:?}: {path}.name in body should match '^[a-z0-9]([-a-z0-9]*[a-z0-9])?$'"),
            );
        }
        if !seen.insert(name.to_string()) {
            errs.push(&path, format!("Duplicate value: {{\"name\":{name:?}}}"));
        }
        if let Some(state) = v.get("state").and_then(Value::as_str) {
            if state != "Enabled" && state != "Disabled" {
                errs.push(
                    &format!("{path}.state"),
                    format!(
                        "Unsupported value: {state:?}: supported values: \"Enabled\", \"Disabled\""
                    ),
                );
            }
        }
        for (arch, assets) in v
            .get("assets")
            .and_then(Value::as_object)
            .into_iter()
            .flatten()
        {
            for (asset, file) in assets.as_object().into_iter().flatten() {
                let apath = format!("{path}.assets.{arch}.{asset}");
                check_string(
                    &mut errs,
                    &format!("{apath}.url"),
                    file.get("url"),
                    1,
                    2048,
                    true,
                );
                match file.get("sha256").and_then(Value::as_str) {
                    None => errs.push(&format!("{apath}.sha256"), "Required value"),
                    Some(s) if !is_sha256_hex(s) => errs.push(
                        &format!("{apath}.sha256"),
                        format!("Invalid value: {s:?}: {apath}.sha256 in body should match '^[a-f0-9]{{64}}$'"),
                    ),
                    Some(_) => {}
                }
            }
        }
    }
    let default = str_at(spec, "defaultVersion").unwrap_or("");
    if !versions.iter().any(|v| str_at(v, "name") == Some(default)) {
        errs.push(
            "spec",
            "Invalid value: \"object\": defaultVersion must name an entry in versions",
        );
    }
    if versions
        .iter()
        .any(|v| str_at(v, "name") == Some(default) && str_at(v, "state") == Some("Disabled"))
    {
        errs.push(
            "spec",
            "Invalid value: \"object\": the version named by defaultVersion must not be Disabled",
        );
    }
    errs.into_result()
}

/// The shipped admission policy: every architecture of every version of a
/// config defines the assets its class needs.
pub fn admit_sandbox_config_assets(obj: &Value) -> Result<(), ValidationErrors> {
    let mut obj = obj.clone();
    default_sandbox_config(&mut obj);
    let spec = obj.get("spec").unwrap_or(&Value::Null);
    let (required, msg) = match str_at(spec, "sandboxClass").and_then(SandboxClass::parse) {
        Some(SandboxClass::Gvisor) => (GVISOR_REQUIRED_ASSETS, GVISOR_ASSETS_MSG),
        Some(SandboxClass::Microvm) => (MICROVM_REQUIRED_ASSETS, MICROVM_ASSETS_MSG),
        None => return Ok(()),
    };
    let ok = spec
        .get("versions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .all(|v| {
            let Some(assets) = v
                .get("assets")
                .and_then(Value::as_object)
                .filter(|a| !a.is_empty())
            else {
                return false;
            };
            assets
                .values()
                .all(|a| required.iter().all(|r| a.get(*r).is_some()))
        });
    if ok {
        Ok(())
    } else {
        Err(ValidationErrors(vec![format!(
            "ValidatingAdmissionPolicy 'sandboxconfig-assets' with binding 'sandboxconfig-assets' denied request: {msg}"
        )]))
    }
}

/// Schema validation and then the admission policy, as a create or update
/// of a SandboxConfig goes through them.
pub fn validate_sandbox_config(obj: &Value, old: Option<&Value>) -> Result<(), ValidationErrors> {
    validate_sandbox_config_schema(obj, old)?;
    admit_sandbox_config_assets(obj)
}

#[cfg(test)]
mod tests;
