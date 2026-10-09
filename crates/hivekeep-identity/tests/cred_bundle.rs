//! Credential bundles and trust bundles read from (projected) files, and
//! the status-keyed caches over them.

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use common::{new_rsa_key, self_signed_with_serial};
use hivekeep_identity::cred_bundle::{load_trust_anchors, CredentialBundle};
use hivekeep_identity::cred_loader::{CredentialBundleLoader, TrustAnchorLoader};
use hivekeep_identity::key::{KeyAlgorithm, PrivateKey};
use hivekeep_identity::pem;
use hivekeep_identity::IdentityError;
use x509_parser::prelude::{FromDer, X509Certificate};

/// A credential bundle whose leaf carries `serial`, so tests can tell
/// bundles apart.
fn make_bundle(serial: u8) -> Vec<u8> {
    let key = PrivateKey::generate(KeyAlgorithm::EcdsaP256).unwrap();
    format!(
        "{}{}",
        pem::encode(pem::CERTIFICATE, &self_signed_with_serial(&key, serial)),
        key.to_pem()
    )
    .into_bytes()
}

/// A trust bundle of one certificate carrying `serial`.
fn make_trust_bundle(serial: u8) -> Vec<u8> {
    let key = PrivateKey::generate(KeyAlgorithm::EcdsaP256).unwrap();
    pem::encode(pem::CERTIFICATE, &self_signed_with_serial(&key, serial)).into_bytes()
}

fn leaf_serial(b: &CredentialBundle) -> Vec<u8> {
    let (_, cert) = X509Certificate::from_der(&b.chain[0]).unwrap();
    cert.raw_serial().to_vec()
}

fn write_bundle(dir: &Path, bundle: &[u8]) -> PathBuf {
    let path = dir.join("bundle.pem");
    std::fs::write(&path, bundle).unwrap();
    path
}

fn set_mtime(path: &Path, t: std::time::SystemTime) {
    std::fs::File::options()
        .write(true)
        .open(path)
        .unwrap()
        .set_modified(t)
        .unwrap();
}

/// Lays a bundle out the way the kubelet's atomic writer materializes
/// projected volumes and returns the visible path:
///
/// ```text
/// <dir>/bundle.pem -> ..data/bundle.pem
/// <dir>/..data     -> ..payload-<n>/
/// <dir>/..payload-<n>/bundle.pem
/// ```
#[cfg(unix)]
fn write_projected_bundle(dir: &Path, bundle: &[u8]) -> PathBuf {
    let payload = tempfile::Builder::new()
        .prefix("..payload-")
        .tempdir_in(dir)
        .unwrap()
        .keep();
    std::fs::write(payload.join("bundle.pem"), bundle).unwrap();
    std::os::unix::fs::symlink(payload.file_name().unwrap(), dir.join("..data")).unwrap();
    std::os::unix::fs::symlink("..data/bundle.pem", dir.join("bundle.pem")).unwrap();
    dir.join("bundle.pem")
}

/// Rotates the bundle behind a projected path as the kubelet does: a fresh
/// payload directory, a `..data_tmp` symlink renamed over `..data`, then the
/// old payload removed. The visible path itself never changes.
#[cfg(unix)]
fn rotate_projected_bundle(path: &Path, bundle: &[u8]) -> std::io::Result<()> {
    let dir = path.parent().unwrap();
    let name = path.file_name().unwrap();
    let old = std::fs::read_link(dir.join("..data"))?;
    let payload = tempfile::Builder::new()
        .prefix("..payload-")
        .tempdir_in(dir)?
        .keep();
    std::fs::write(payload.join(name), bundle)?;
    let tmp = dir.join("..data_tmp");
    std::os::unix::fs::symlink(payload.file_name().unwrap(), &tmp)?;
    std::fs::rename(&tmp, dir.join("..data"))?;
    std::fs::remove_dir_all(dir.join(old))
}

#[test]
fn parse_pkcs8_private_key_block() {
    let key = new_rsa_key();
    let leaf = self_signed_with_serial(&key, 1);
    let bundle = format!("{}{}", pem::encode(pem::CERTIFICATE, &leaf), key.to_pem());
    let dir = tempfile::tempdir().unwrap();
    let b = CredentialBundle::load(write_bundle(dir.path(), bundle.as_bytes())).unwrap();
    assert_eq!(b.chain.len(), 1);
    assert_eq!(b.key.public_key(), key.public_key());
    assert_eq!(leaf_serial(&b), [1]);
}

#[test]
fn parse_rejects_non_pkcs8_private_key_block() {
    let key = new_rsa_key();
    let leaf = self_signed_with_serial(&key, 1);
    // The bytes do not matter: the label alone is refused.
    let bundle = format!(
        "{}{}",
        pem::encode(pem::CERTIFICATE, &leaf),
        pem::encode("RSA PRIVATE KEY", key.pkcs8_der())
    );
    let dir = tempfile::tempdir().unwrap();
    let err = CredentialBundle::load(write_bundle(dir.path(), bundle.as_bytes()))
        .unwrap_err()
        .to_string();
    assert!(err.contains("RSA PRIVATE KEY"), "{err}");
}

#[test]
fn loader_serves_cached_parse_while_file_unchanged() {
    let bundle = make_bundle(7);
    let dir = tempfile::tempdir().unwrap();
    let path = write_bundle(dir.path(), &bundle);
    let loader = CredentialBundleLoader::credential_bundle(&path);
    loader.get().unwrap();
    let mtime = std::fs::metadata(&path).unwrap().modified().unwrap();
    // Same-length garbage with the mtime restored: identity, size and mtime
    // all match, so the cached parse must be served (a re-read would fail).
    std::fs::write(&path, vec![b'x'; bundle.len()]).unwrap();
    set_mtime(&path, mtime);
    let b = loader.get().expect("unchanged status");
    assert_eq!(leaf_serial(&b), [7]);
}

#[cfg(unix)]
#[test]
fn loader_picks_up_projected_volume_rotation() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_projected_bundle(dir.path(), &make_bundle(1));
    let loader = CredentialBundleLoader::credential_bundle(&path);
    assert_eq!(leaf_serial(&loader.get().unwrap()), [1]);
    rotate_projected_bundle(&path, &make_bundle(2)).unwrap();
    assert_eq!(leaf_serial(&loader.get().unwrap()), [2]);
}

#[test]
fn loader_picks_up_in_place_rewrite() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_bundle(dir.path(), &make_bundle(1));
    let loader = CredentialBundleLoader::credential_bundle(&path);
    loader.get().unwrap();
    let mtime = std::fs::metadata(&path).unwrap().modified().unwrap();
    // Same inode; the mtime is pushed forward so coarse timestamps still
    // show the change.
    std::fs::write(&path, make_bundle(2)).unwrap();
    set_mtime(&path, mtime + Duration::from_secs(1));
    assert_eq!(leaf_serial(&loader.get().unwrap()), [2]);
}

#[test]
fn loader_error_when_bundle_missing() {
    let dir = tempfile::tempdir().unwrap();
    let loader = CredentialBundleLoader::credential_bundle(dir.path().join("absent.pem"));
    assert!(loader.get().is_err());
}

#[test]
fn loader_recovers_after_error() {
    let bundle = make_bundle(3);
    let dir = tempfile::tempdir().unwrap();
    let path = write_bundle(dir.path(), &bundle);
    let loader = CredentialBundleLoader::credential_bundle(&path);
    loader.get().unwrap();
    std::fs::remove_file(&path).unwrap();
    assert!(
        loader.get().is_err(),
        "no error after the bundle was removed"
    );
    std::fs::write(&path, &bundle).unwrap();
    assert_eq!(leaf_serial(&loader.get().unwrap()), [3]);
}

/// The client side uses the same loader; this mirrors its rotation check.
#[cfg(unix)]
#[test]
fn client_loader_caches_and_reloads() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_projected_bundle(dir.path(), &make_bundle(1));
    let loader = CredentialBundleLoader::credential_bundle(&path);
    let first = loader.get().unwrap();
    assert_eq!(leaf_serial(&first), [1]);
    assert!(Arc::ptr_eq(&first, &loader.get().unwrap()), "re-parsed");
    rotate_projected_bundle(&path, &make_bundle(2)).unwrap();
    assert_eq!(leaf_serial(&loader.get().unwrap()), [2]);
}

#[test]
fn pool_loader_serves_cached_parse_while_file_unchanged() {
    let trust = make_trust_bundle(5);
    let dir = tempfile::tempdir().unwrap();
    let path = write_bundle(dir.path(), &trust);
    let loader = TrustAnchorLoader::trust_anchors(&path);
    let first = loader.get().unwrap();
    let mtime = std::fs::metadata(&path).unwrap().modified().unwrap();
    std::fs::write(&path, vec![b'x'; trust.len()]).unwrap();
    set_mtime(&path, mtime);
    let second = loader.get().expect("unchanged status");
    assert!(Arc::ptr_eq(&first, &second), "re-parsed an unchanged file");
}

#[cfg(unix)]
#[test]
fn pool_loader_picks_up_projected_volume_rotation() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_projected_bundle(dir.path(), &make_trust_bundle(1));
    let loader = TrustAnchorLoader::trust_anchors(&path);
    let before = loader.get().unwrap();
    rotate_projected_bundle(&path, &make_trust_bundle(2)).unwrap();
    let after = loader.get().unwrap();
    assert_ne!(after, before, "rotation not picked up");
    assert_eq!(*after, load_trust_anchors(&path).unwrap());
}

#[test]
fn pool_loader_error_when_bundle_missing() {
    let dir = tempfile::tempdir().unwrap();
    assert!(
        TrustAnchorLoader::trust_anchors(dir.path().join("absent.pem"))
            .get()
            .is_err()
    );
}

#[test]
fn parse_pool_rejects_bundle_without_certificates() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_bundle(dir.path(), b"not a certificate");
    assert!(load_trust_anchors(&path).is_err());
}

#[cfg(unix)]
#[test]
fn loader_concurrent_handshakes() {
    let bundles = [make_bundle(1), make_bundle(2)];
    let dir = tempfile::tempdir().unwrap();
    let path = write_projected_bundle(dir.path(), &bundles[0]);
    let loader = CredentialBundleLoader::credential_bundle(&path);
    std::thread::scope(|s| {
        s.spawn(|| {
            for i in 0..25 {
                rotate_projected_bundle(&path, &bundles[i % 2]).unwrap();
            }
        });
        for _ in 0..4 {
            s.spawn(|| {
                for _ in 0..100 {
                    match loader.get() {
                        Ok(b) => {
                            let serial = leaf_serial(&b);
                            assert!(serial == [1] || serial == [2], "serial {serial:?}");
                        }
                        // macOS rename(2) is not atomic with respect to
                        // concurrent resolution through the swapped
                        // symlink and can surface a transient EINVAL; Linux
                        // always resolves to the old or the new target.
                        Err(IdentityError::Io { source, .. })
                            if cfg!(target_os = "macos") && source.raw_os_error() == Some(22) => {}
                        Err(e) => panic!("loader: {e}"),
                    }
                }
            });
        }
    });
}
