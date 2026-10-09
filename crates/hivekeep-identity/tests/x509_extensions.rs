//! The private X.509 extensions carrying Pod and Actor identities.

use hivekeep_identity::extension::{
    actor_identity_oid, pod_identity_oid, ActorIdentity, IdentityExtension, PodIdentity,
};
use rcgen::{CertificateParams, CustomExtension, KeyPair};
use x509_parser::prelude::{FromDer, X509Certificate};

/// The extension numbers, spelled out so a change to them is deliberate.
const POD_OID: &[u64] = &[1, 3, 6, 1, 4, 1, 32473, 1, 1];
const ACTOR_OID: &[u64] = &[1, 3, 6, 1, 4, 1, 32473, 1, 2];

/// Self-signs a certificate carrying `extensions`.
fn mint(extensions: Vec<CustomExtension>) -> Vec<u8> {
    let mut p = CertificateParams::default();
    p.custom_extensions = extensions;
    p.self_signed(&KeyPair::generate().unwrap())
        .unwrap()
        .der()
        .to_vec()
}

/// A table case: name, extensions to mint, expected identity or error.
type Case<T> = (
    &'static str,
    Vec<CustomExtension>,
    Result<Option<T>, &'static str>,
);
/// A field name and how to empty it.
type Mutation<T> = (&'static str, fn(&mut T));

fn raw(oid: &[u64], value: &[u8]) -> CustomExtension {
    CustomExtension::from_oid_content(oid, value.to_vec())
}

fn full_pod() -> PodIdentity {
    PodIdentity {
        namespace: "hivekeep-system".into(),
        service_account_name: "keeper".into(),
        service_account_uid: "sa-uid".into(),
        pod_name: "keeper-abc".into(),
        pod_uid: "pod-uid".into(),
        node_name: "node-1".into(),
        node_uid: "node-uid".into(),
    }
}

fn full_actor() -> ActorIdentity {
    ActorIdentity {
        hive: "team-a".into(),
        actor_name: "researcher".into(),
        actor_uid: "actor-uid".into(),
    }
}

#[test]
fn oids_are_pinned() {
    assert_eq!(pod_identity_oid(), POD_OID);
    assert_eq!(actor_identity_oid(), ACTOR_OID);
}

fn check<T: IdentityExtension + PartialEq + std::fmt::Debug>(
    name: &str,
    cert: &[u8],
    want: Result<Option<T>, &str>,
) {
    match (T::from_certificate(cert), want) {
        (Ok(got), Ok(want)) => assert_eq!(got, want, "{name}"),
        (Err(e), Err(want)) => {
            let e = e.to_string();
            assert!(e.contains(want), "{name}: error {e:?} lacks {want:?}");
        }
        (got, want) => panic!("{name}: got {got:?}, want {want:?}"),
    }
}

#[test]
fn pod_identity_from_certificate() {
    let mut no_uid = full_pod();
    no_uid.pod_uid.clear();
    let dup = raw(POD_OID, br#"{"PodUID":"pod-uid"}"#);
    let cases: Vec<Case<PodIdentity>> = vec![
        (
            "RoundTrip",
            vec![full_pod().to_extension().unwrap()],
            Ok(Some(full_pod())),
        ),
        ("Absent", vec![], Ok(None)),
        (
            "Duplicate",
            vec![dup.clone(), dup],
            Err("repeats an identity extension"),
        ),
        (
            "EmptyField",
            vec![raw(POD_OID, &serde_json::to_vec(&no_uid).unwrap())],
            Err("PodUID"),
        ),
        (
            "Malformed",
            vec![raw(POD_OID, b"not json")],
            Err("decoding identity extension"),
        ),
    ];
    for (name, exts, want) in cases {
        check(name, &mint(exts), want);
    }
}

#[test]
fn add_pod_identity_empty_field() {
    let fields: [Mutation<PodIdentity>; 7] = [
        ("Namespace", |p| p.namespace.clear()),
        ("ServiceAccountName", |p| p.service_account_name.clear()),
        ("ServiceAccountUID", |p| p.service_account_uid.clear()),
        ("PodName", |p| p.pod_name.clear()),
        ("PodUID", |p| p.pod_uid.clear()),
        ("NodeName", |p| p.node_name.clear()),
        ("NodeUID", |p| p.node_uid.clear()),
    ];
    for (name, mutate) in fields {
        let mut pod = full_pod();
        mutate(&mut pod);
        let err = pod.to_extension().unwrap_err().to_string();
        assert!(err.contains(name), "{name}: {err}");
    }
}

#[test]
fn actor_identity_from_certificate() {
    let mut no_uid = full_actor();
    no_uid.actor_uid.clear();
    let dup = raw(ACTOR_OID, br#"{"ActorUid":"actor-uid"}"#);
    let cases: Vec<Case<ActorIdentity>> = vec![
        (
            "RoundTrip",
            vec![full_actor().to_extension().unwrap()],
            Ok(Some(full_actor())),
        ),
        ("Absent", vec![], Ok(None)),
        // A Pod identity is not mistaken for an Actor one.
        (
            "PodIdentityOnly",
            vec![full_pod().to_extension().unwrap()],
            Ok(None),
        ),
        (
            "Duplicate",
            vec![dup.clone(), dup],
            Err("repeats an identity extension"),
        ),
        (
            "EmptyField",
            vec![raw(ACTOR_OID, &serde_json::to_vec(&no_uid).unwrap())],
            Err("ActorUid"),
        ),
        (
            "Malformed",
            vec![raw(ACTOR_OID, b"not json")],
            Err("decoding identity extension"),
        ),
    ];
    for (name, exts, want) in cases {
        check(name, &mint(exts), want);
    }
}

#[test]
fn add_actor_identity_empty_field() {
    let fields: [Mutation<ActorIdentity>; 3] = [
        ("Hive", |a| a.hive.clear()),
        ("ActorName", |a| a.actor_name.clear()),
        ("ActorUid", |a| a.actor_uid.clear()),
    ];
    for (name, mutate) in fields {
        let mut actor = full_actor();
        mutate(&mut actor);
        let err = actor.to_extension().unwrap_err().to_string();
        assert!(err.contains(name), "{name}: {err}");
    }
}

/// The extension value is plain JSON, so verifiers need no ASN.1 library.
#[test]
fn extension_value_is_json() {
    let der = mint(vec![
        full_pod().to_extension().unwrap(),
        full_actor().to_extension().unwrap(),
    ]);
    let (_, cert) = X509Certificate::from_der(&der).unwrap();
    for (name, oid) in [("PodIdentity", POD_OID), ("ActorIdentity", ACTOR_OID)] {
        let ext = cert
            .extensions()
            .iter()
            .find(|e| {
                e.oid
                    .iter()
                    .is_some_and(|arcs| arcs.eq(oid.iter().copied()))
            })
            .unwrap_or_else(|| panic!("{name} extension not found"));
        serde_json::from_slice::<serde_json::Value>(ext.value)
            .unwrap_or_else(|e| panic!("{name} value is not JSON: {e}"));
    }
}
