use super::*;

fn policy(hive: &str, namespaces: &[&str]) -> HivePolicy {
    HivePolicy {
        hive: hive.into(),
        allowed_namespaces: namespaces.iter().map(|s| s.to_string()).collect(),
        secret_selector: None,
    }
}

fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn narrowed(hive: &str, namespaces: &[&str], pairs: &[(&str, &str)]) -> HivePolicy {
    HivePolicy {
        secret_selector: Some(SecretSelector {
            match_labels: labels(pairs),
        }),
        ..policy(hive, namespaces)
    }
}

fn authorizer(policies: Vec<HivePolicy>) -> NamespaceAuthorizer {
    NamespaceAuthorizer::new(PolicyFile { policies }).unwrap()
}

fn write_policy(dir: &tempfile::TempDir, body: &str) -> std::path::PathBuf {
    let path = dir.path().join("policy.yaml");
    std::fs::write(&path, body).unwrap();
    path
}

#[test]
fn namespace_authorizer() {
    let authz = authorizer(vec![
        policy("team-a", &["ns1", "shared"]),
        policy("team-b", &["ns2"]),
    ]);
    let cases = [
        ("team-a", "ns1", true),
        ("team-a", "shared", true),
        ("team-a", "ns2", false), // namespace not in team-a's list
        ("team-b", "ns2", true),  // team-b's own namespace
        ("team-c", "ns1", false), // hive absent: default deny
        ("team-a", "", false),    // empty namespace
    ];
    for (hive, ns, want) in cases {
        assert_eq!(authz.allowed(hive, ns), want, "allowed({hive:?}, {ns:?})");
    }

    // An empty file denies everything.
    let empty = NamespaceAuthorizer::new(PolicyFile::default()).unwrap();
    assert!(!empty.allowed("team-a", "ns1"));

    // A policy without a hive is rejected.
    assert!(NamespaceAuthorizer::new(PolicyFile {
        policies: vec![policy("", &["ns1"])]
    })
    .is_err());
}

#[test]
fn rejects_malformed_narrowing() {
    let cases = [
        (
            "invalid label key",
            narrowed("team-a", &["ns1"], &[("not a key", "v")]),
        ),
        (
            "invalid label value",
            narrowed("team-a", &["ns1"], &[("example.com/k", "not a value!")]),
        ),
    ];
    for (name, p) in cases {
        assert!(
            NamespaceAuthorizer::new(PolicyFile { policies: vec![p] }).is_err(),
            "{name}: accepted"
        );
    }
}

/// The deployment ships a policy that must load and grant nothing until an
/// operator lists hives.
#[test]
fn shipped_namespace_policy_grants_nothing() {
    #[derive(Deserialize)]
    struct ConfigMap {
        data: BTreeMap<String, String>,
    }
    let cm: ConfigMap =
        serde_yaml::from_str(include_str!("../tests/fixtures/namespace-policy.yaml")).unwrap();
    let policy = cm
        .data
        .get("namespace-policy.yaml")
        .expect("shipped ConfigMap has no namespace-policy.yaml key, the file the provider mounts");
    let dir = tempfile::tempdir().unwrap();
    let authz = NamespaceAuthorizer::load(write_policy(&dir, policy)).unwrap();
    assert!(
        authz.grants().is_empty(),
        "shipped policy grants {:?}",
        authz.grants()
    );
}

/// The wire shape needs its own test: a struct literal cannot catch a wrong
/// field name, and an absent selector must mean no narrowing.
#[test]
fn load_secret_narrowing() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_policy(
        &dir,
        r#"policies:
- hive: team-a
  allowedNamespaces: [ns1]
  secretSelector:
    matchLabels:
      example.com/credential: "true"
- hive: team-b
  allowedNamespaces: [ns2]
"#,
    );
    let authz = NamespaceAuthorizer::load(path).unwrap();
    assert!(
        !authz.allowed_secret("team-a", "ns1", None),
        "selector grant is unconditional"
    );
    assert!(authz.allowed_secret(
        "team-a",
        "ns1",
        Some(&labels(&[("example.com/credential", "true")]))
    ));
    assert!(!authz.allowed_secret("team-a", "ns1", Some(&labels(&[("other", "x")]))));
    assert!(
        authz.allowed_secret("team-b", "ns2", None),
        "grant without selector was narrowed"
    );
}

#[test]
fn load_rejects_unknown_fields() {
    let cases = [
        (
            "misspelled field",
            r#"policies:
- hive: team-a
  allowedNamespaces: [ns1]
  secretSelector:
    matchLabel:
      example.com/credential: "true"
"#,
        ),
        (
            "unsupported field",
            r#"policies:
- hive: team-a
  allowedNamespaces: [ns1]
  secretSelector:
    matchExpressions:
    - key: example.com/credential
      operator: Exists
"#,
        ),
    ];
    for (name, body) in cases {
        let dir = tempfile::tempdir().unwrap();
        assert!(
            NamespaceAuthorizer::load(write_policy(&dir, body)).is_err(),
            "{name}: loaded"
        );
    }
}

#[test]
fn load_reports_missing_file() {
    let dir = tempfile::tempdir().unwrap();
    assert!(matches!(
        NamespaceAuthorizer::load(dir.path().join("absent.yaml")),
        Err(PolicyError::Read { .. })
    ));
}

#[test]
fn policies_are_ored() {
    // A narrow policy does not revoke a broad one.
    let a = authorizer(vec![
        policy("team-a", &["ns1"]),
        narrowed("team-a", &["ns1"], &[("example.com/credential", "true")]),
    ]);
    assert!(a.allowed_secret("team-a", "ns1", None));

    // Either selector admits, and each keeps its own keys.
    let a = authorizer(vec![
        narrowed("team-a", &["ns1"], &[("tier", "prod")]),
        narrowed("team-a", &["ns1"], &[("team", "x")]),
    ]);
    assert!(a.allowed_secret("team-a", "ns1", Some(&labels(&[("tier", "prod")]))));
    assert!(a.allowed_secret("team-a", "ns1", Some(&labels(&[("team", "x")]))));
    assert!(!a.allowed_secret("team-a", "ns1", Some(&labels(&[("other", "y")]))));

    // Every label in one selector is required.
    let a = authorizer(vec![narrowed(
        "team-a",
        &["ns1"],
        &[("tier", "prod"), ("team", "x")],
    )]);
    assert!(!a.allowed_secret("team-a", "ns1", Some(&labels(&[("tier", "prod")]))));
    assert!(a.allowed_secret(
        "team-a",
        "ns1",
        Some(&labels(&[("tier", "prod"), ("team", "x")]))
    ));
}

#[test]
fn grants_are_sorted() {
    let authz = authorizer(vec![
        policy("team-a", &["ns2", "ns1"]),
        policy("team-b", &["ns3"]),
    ]);
    let want: BTreeMap<String, Vec<String>> = [
        (
            "team-a".to_string(),
            vec!["ns1".to_string(), "ns2".to_string()],
        ),
        ("team-b".to_string(), vec!["ns3".to_string()]),
    ]
    .into();
    assert_eq!(authz.grants(), want);
}
