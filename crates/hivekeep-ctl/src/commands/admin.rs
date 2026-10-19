//! `admin`: creating and rotating the key pools the control plane signs
//! with, kept in Kubernetes Secrets.

use std::time::Duration;

use crate::ca_pool::{generate_ca, marshal_pool, CaKeyType};
use crate::cli::{AdminCommand, PoolSecret};
use crate::context::Ctx;
use crate::error::{CliError, Result};
use crate::jwt_pool::{
    generate_authority, generate_authority_with_rsa_bits, JwtPool, RSA_KEY_BITS,
};
use crate::secret::{
    KubectlSecrets, Secret, SecretError, SecretStore, TLS_CERT_KEY, TLS_KEY_KEY, TLS_SECRET_TYPE,
};
use crate::table::Table;
use crate::verbosity::{Logger, BASIC};

/// Secret key holding the serialized pool.
pub const POOL_KEY: &str = "pool";
/// Secret key holding a JWT pool's public JWK set.
pub const JWKS_KEY: &str = "jwks";

/// Attempts at a conditional update before giving up on conflicts.
pub const UPDATE_ATTEMPTS: u32 = 5;
const RETRY_DELAY: Duration = Duration::from_millis(10);

/// Parses a duration such as `8760h`, `1h30m`, `90s` or `1.5h`.
pub fn parse_duration(s: &str) -> std::result::Result<Duration, String> {
    let err = || format!("invalid duration {s:?}");
    if s == "0" {
        return Ok(Duration::ZERO);
    }
    let mut rest = s;
    let mut total = 0f64;
    if rest.is_empty() {
        return Err(err());
    }
    while !rest.is_empty() {
        let num_len = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .ok_or_else(err)?;
        let (num, tail) = rest.split_at(num_len);
        let value: f64 = num.parse().map_err(|_| err())?;
        let unit_len = tail
            .find(|c: char| c.is_ascii_digit() || c == '.')
            .unwrap_or(tail.len());
        let (unit, tail) = tail.split_at(unit_len);
        let scale = match unit {
            "ns" => 1e-9,
            "us" | "µs" => 1e-6,
            "ms" => 1e-3,
            "s" => 1.0,
            "m" => 60.0,
            "h" => 3600.0,
            _ => return Err(err()),
        };
        total += value * scale;
        rest = tail;
    }
    Ok(Duration::from_secs_f64(total))
}

fn secret_error(context: &str, e: SecretError) -> CliError {
    CliError::failed(format!("{context}: {e}"))
}

/// Reads the named Secret and the pool it holds.
pub fn get_jwt_pool(
    store: &dyn SecretStore,
    namespace: &str,
    name: &str,
) -> Result<(Secret, JwtPool)> {
    let secret = store
        .get(namespace, name)
        .map_err(|e| secret_error("while reading pool secret", e))?;
    let wire = secret.data.get(POOL_KEY).ok_or_else(|| {
        CliError::failed(format!("secret {namespace}/{name} has no {POOL_KEY:?} key"))
    })?;
    let pool = JwtPool::unmarshal(wire)
        .map_err(|e| CliError::failed(format!("while parsing pool: {e}")))?;
    Ok((secret, pool))
}

/// Applies `change` to the pool in the named Secret and writes the pool and
/// its JWK set back together, so a verifier never sees one lag the other.
/// The write is conditional on the version read; a conflict reruns `change`
/// against the current pool.
pub fn update_jwt_pool(
    store: &dyn SecretStore,
    namespace: &str,
    name: &str,
    log: &Logger,
    mut change: impl FnMut(&mut JwtPool) -> std::result::Result<(), String>,
) -> Result<()> {
    let mut attempt = 0;
    loop {
        attempt += 1;
        if attempt > 1 {
            log.log(
                BASIC,
                format!("Conflict updating secret {name:?} (attempt {attempt}); retrying..."),
            );
        }
        let (mut secret, mut pool) = get_jwt_pool(store, namespace, name)?;
        change(&mut pool).map_err(CliError::failed)?;
        let jwks = pool.jwks().map_err(|e| {
            CliError::failed(format!("while publishing the pool's public keys: {e}"))
        })?;
        secret.data.insert(POOL_KEY.into(), pool.marshal());
        secret.data.insert(JWKS_KEY.into(), jwks);
        match store.update(&secret) {
            Ok(()) => return Ok(()),
            Err(SecretError::Conflict(_)) if attempt < UPDATE_ATTEMPTS => {
                std::thread::sleep(RETRY_DELAY);
            }
            Err(e) => return Err(secret_error("while writing pool secret", e)),
        }
    }
}

/// The Secret for a new JWT pool holding one active key for `algorithm`,
/// and that key's ID.
pub fn new_jwt_pool_secret(
    namespace: &str,
    name: &str,
    algorithm: &str,
    key_id: &str,
) -> Result<(Secret, String)> {
    new_jwt_pool_secret_with_rsa_bits(namespace, name, algorithm, key_id, RSA_KEY_BITS)
}

/// [`new_jwt_pool_secret`] with the RSA key size as a parameter, so tests
/// need not wait for a production-size key.
fn new_jwt_pool_secret_with_rsa_bits(
    namespace: &str,
    name: &str,
    algorithm: &str,
    key_id: &str,
    rsa_bits: usize,
) -> Result<(Secret, String)> {
    let authority = generate_authority_with_rsa_bits(algorithm, key_id, rsa_bits)
        .map_err(|e| CliError::usage(format!("while generating JWT authority pool: {e}")))?;
    let id = authority.id.clone();
    let pool = JwtPool::with_active(authority);
    let jwks = pool
        .jwks()
        .map_err(|e| CliError::failed(format!("while publishing the pool's public keys: {e}")))?;
    let secret = Secret {
        namespace: namespace.into(),
        name: name.into(),
        secret_type: None,
        data: [
            (POOL_KEY.to_string(), pool.marshal()),
            (JWKS_KEY.to_string(), jwks),
        ]
        .into(),
        resource_version: None,
    };
    Ok((secret, id))
}

/// The Secret for a new CA pool holding one CA, with its certificate and
/// key also under the standard TLS keys.
pub fn new_ca_pool_secret(
    namespace: &str,
    name: &str,
    ca_id: &str,
    key_type: &str,
    validity: &str,
) -> Result<Secret> {
    let key_type: CaKeyType = key_type.parse().map_err(CliError::usage)?;
    let validity =
        parse_duration(validity).map_err(|e| CliError::usage(format!("--validity: {e}")))?;
    let ca = generate_ca(ca_id, key_type, validity)
        .map_err(|e| CliError::failed(format!("while generating CA: {e}")))?;
    Ok(Secret {
        namespace: namespace.into(),
        name: name.into(),
        secret_type: Some(TLS_SECRET_TYPE.into()),
        data: [
            (
                POOL_KEY.to_string(),
                marshal_pool(std::slice::from_ref(&ca), ca_id),
            ),
            (TLS_CERT_KEY.to_string(), ca.cert_pem()),
            (TLS_KEY_KEY.to_string(), ca.key_pem()),
        ]
        .into(),
        resource_version: None,
    })
}

/// The key table: which key signs, and every key's ID and algorithm. Nothing
/// about the private keys.
pub fn jwt_keys_table(pool: &JwtPool) -> Table {
    let mut t = Table::new(["ACTIVE", "KEY ID", "ALGORITHM"]);
    for a in &pool.authorities {
        let active = if a.id == pool.active_id() { "*" } else { "" };
        t.row([active.to_string(), a.id.clone(), a.algorithm.clone()]);
    }
    t
}

/// Runs an admin command against `store`.
pub fn run_with(ctx: &Ctx, store: &dyn SecretStore, cmd: &AdminCommand) -> Result<()> {
    let target = |s: &PoolSecret| (s.secret_namespace.clone(), s.name.clone());
    match cmd {
        AdminCommand::MakeCaPool {
            secret,
            ca_id,
            key_type,
            validity,
        } => {
            let (ns, name) = target(secret);
            let s = new_ca_pool_secret(&ns, &name, ca_id, key_type, validity)?;
            store
                .create(&s)
                .map_err(|e| secret_error("while uploading pool state to secret", e))?;
            ctx.println(&format!("Successfully created CA pool secret {ns}/{name}"))
        }
        AdminCommand::MakeJwtPool {
            secret,
            alg,
            key_id,
        } => {
            let (ns, name) = target(secret);
            let (s, id) = new_jwt_pool_secret(&ns, &name, alg, key_id)?;
            store
                .create(&s)
                .map_err(|e| secret_error("while uploading pool state to secret", e))?;
            ctx.println(&format!(
                "Successfully created JWT authority pool secret {ns}/{name} with {alg} key {id}"
            ))
        }
        AdminCommand::ListJwtKeys { secret } => {
            let (ns, name) = target(secret);
            let (_, pool) = get_jwt_pool(store, &ns, &name)?;
            Ok(jwt_keys_table(&pool).write_to(&mut ctx.out())?)
        }
        AdminCommand::AddJwtKey {
            secret,
            alg,
            key_id,
        } => {
            let (ns, name) = target(secret);
            let authority = generate_authority(alg, key_id)
                .map_err(|e| CliError::usage(format!("while generating JWT authority: {e}")))?;
            let id = authority.id.clone();
            update_jwt_pool(store, &ns, &name, &ctx.log, |pool| {
                pool.add(authority.clone())
            })?;
            ctx.println(&format!(
                "Added inactive {alg} key {id} to JWT authority pool secret {ns}/{name}"
            ))
        }
        AdminCommand::ActivateJwtKey { secret, key_id } => {
            let (ns, name) = target(secret);
            update_jwt_pool(store, &ns, &name, &ctx.log, |pool| pool.activate(key_id))?;
            ctx.println(&format!(
                "Activated key {key_id} in JWT authority pool secret {ns}/{name}"
            ))
        }
        AdminCommand::RemoveJwtKey { secret, key_id } => {
            let (ns, name) = target(secret);
            update_jwt_pool(store, &ns, &name, &ctx.log, |pool| pool.remove(key_id))?;
            ctx.println(&format!(
                "Removed key {key_id} from JWT authority pool secret {ns}/{name}"
            ))
        }
    }
}

/// `admin ...`, against the cluster's Secrets.
pub fn run(ctx: &Ctx, cmd: &AdminCommand) -> Result<()> {
    run_with(ctx, &KubectlSecrets(ctx.kubectl()), cmd)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::GlobalArgs;
    use crate::io::Io;
    use crate::jwt_pool::Authority;
    use clap::Parser;
    use std::cell::{Cell, RefCell};
    use std::collections::BTreeMap;

    type ConcurrentWrite = Box<dyn Fn(&mut Secret)>;

    /// An in-memory Secret store with resource versions and injectable
    /// conflicts.
    #[derive(Default)]
    struct MemorySecrets {
        secrets: RefCell<BTreeMap<(String, String), Secret>>,
        version: Cell<u64>,
        conflicts: Cell<u32>,
        updates: Cell<u32>,
        /// Another writer's change, applied to the stored Secret when an
        /// injected conflict fires.
        concurrent_write: RefCell<Option<ConcurrentWrite>>,
    }

    impl MemorySecrets {
        fn bump(&self) -> String {
            self.version.set(self.version.get() + 1);
            self.version.get().to_string()
        }
        fn stored(&self, ns: &str, name: &str) -> Secret {
            self.secrets.borrow()[&(ns.to_string(), name.to_string())].clone()
        }
    }

    impl SecretStore for MemorySecrets {
        fn get(&self, ns: &str, name: &str) -> std::result::Result<Secret, SecretError> {
            self.secrets
                .borrow()
                .get(&(ns.to_string(), name.to_string()))
                .cloned()
                .ok_or_else(|| SecretError::NotFound(format!("secrets {name:?} not found")))
        }
        fn create(&self, s: &Secret) -> std::result::Result<(), SecretError> {
            let key = (s.namespace.clone(), s.name.clone());
            if self.secrets.borrow().contains_key(&key) {
                return Err(SecretError::Other("already exists".into()));
            }
            let mut s = s.clone();
            s.resource_version = Some(self.bump());
            self.secrets.borrow_mut().insert(key, s);
            Ok(())
        }
        fn update(&self, s: &Secret) -> std::result::Result<(), SecretError> {
            self.updates.set(self.updates.get() + 1);
            if self.conflicts.get() > 0 {
                self.conflicts.set(self.conflicts.get() - 1);
                // Someone else wrote in between.
                let key = (s.namespace.clone(), s.name.clone());
                let v = self.bump();
                let mut secrets = self.secrets.borrow_mut();
                let stored = secrets.get_mut(&key).unwrap();
                if let Some(write) = self.concurrent_write.borrow().as_ref() {
                    write(stored);
                }
                stored.resource_version = Some(v);
                return Err(SecretError::Conflict("the object has been modified".into()));
            }
            let key = (s.namespace.clone(), s.name.clone());
            let current = self.secrets.borrow()[&key].resource_version.clone();
            if current != s.resource_version {
                return Err(SecretError::Conflict("stale".into()));
            }
            let mut s = s.clone();
            s.resource_version = Some(self.bump());
            self.secrets.borrow_mut().insert(key, s);
            Ok(())
        }
    }

    fn ctx() -> (Ctx, crate::io::SharedBuf, crate::io::SharedBuf) {
        #[derive(Parser)]
        struct G {
            #[command(flatten)]
            g: GlobalArgs,
        }
        let (io, out, err) = Io::captured();
        let mut c = Ctx::new(G::parse_from(["x"]).g, io);
        c.log = Logger::new(BASIC, c.err());
        (c, out, err)
    }

    fn admin(args: &[&str]) -> AdminCommand {
        #[derive(Parser)]
        struct A {
            #[command(subcommand)]
            cmd: AdminCommand,
        }
        A::parse_from(std::iter::once("admin").chain(args.iter().copied())).cmd
    }

    fn pool_of(store: &MemorySecrets) -> JwtPool {
        JwtPool::unmarshal(&store.stored("default", "jwt").data[POOL_KEY]).unwrap()
    }

    fn pool_ids(store: &MemorySecrets) -> (Vec<String>, String) {
        let secret = store.stored("default", "jwt");
        let pool = JwtPool::unmarshal(&secret.data[POOL_KEY]).unwrap();
        // The published key set always matches the pool.
        assert_eq!(pool.jwks().unwrap(), secret.data[JWKS_KEY]);
        (
            pool.authorities.iter().map(|a| a.id.clone()).collect(),
            pool.active_id().to_string(),
        )
    }

    fn pool_store(active: &str) -> MemorySecrets {
        let store = MemorySecrets::default();
        let (s, _) = new_jwt_pool_secret("default", "jwt", "ES256", active).unwrap();
        store.create(&s).unwrap();
        store
    }

    fn small_rsa(id: &str) -> Authority {
        generate_authority_with_rsa_bits("RS256", id, 1024).unwrap()
    }

    #[test]
    fn new_jwt_pool_secret_with_flag_defaults() {
        let (alg, key_id) = match admin(&["make-jwt-pool", "--name", "jwt"]) {
            AdminCommand::MakeJwtPool { alg, key_id, .. } => (alg, key_id),
            other => panic!("{other:?}"),
        };
        let (secret, id) =
            new_jwt_pool_secret("hk-system", "actor-id-jwt-pool", &alg, &key_id).unwrap();
        assert_eq!(
            (secret.namespace.as_str(), secret.name.as_str()),
            ("hk-system", "actor-id-jwt-pool")
        );
        let pool = JwtPool::unmarshal(&secret.data[POOL_KEY]).unwrap();
        assert_eq!(pool.authorities.len(), 1);
        let authority = &pool.authorities[0];
        assert_eq!(authority.algorithm, "ES256");
        let thumbprint =
            crate::jwt_pool::thumbprint(crate::jwt_pool::public_key(authority).unwrap().spki());
        assert_eq!(authority.id, thumbprint);
        assert_eq!(id, thumbprint);
        assert_eq!(pool.active_for_signing, thumbprint);
    }

    fn check_explicit_rs256(secret: &Secret, id: &str) {
        let pool = JwtPool::unmarshal(&secret.data[POOL_KEY]).unwrap();
        assert_eq!(id, "1");
        assert_eq!(pool.active_for_signing, "1");
        assert_eq!(pool.authorities[0].algorithm, "RS256");
    }

    #[test]
    fn new_jwt_pool_secret_explicit_key() {
        let (secret, id) =
            new_jwt_pool_secret_with_rsa_bits("hk-system", "actor-id-jwt-pool", "RS256", "1", 1024)
                .unwrap();
        check_explicit_rs256(&secret, &id);
    }

    #[test]
    #[ignore = "generates a 4096-bit RSA key, which takes about a minute in a debug build"]
    fn new_jwt_pool_secret_explicit_key_production_size() {
        let (secret, id) =
            new_jwt_pool_secret("hk-system", "actor-id-jwt-pool", "RS256", "1").unwrap();
        check_explicit_rs256(&secret, &id);
    }

    #[test]
    fn new_jwt_pool_secret_rejects_unsupported_algorithm() {
        assert!(new_jwt_pool_secret("hk-system", "actor-id-jwt-pool", "HS256", "").is_err());
    }

    #[test]
    fn update_jwt_pool_rotation() {
        let store = pool_store("old");
        let next = small_rsa("next");
        let log = Logger::silent();
        type Change = Box<dyn Fn(&mut JwtPool) -> std::result::Result<(), String>>;
        let steps: [(&str, Change, &[&str], &str); 3] = [
            (
                "add",
                Box::new(move |p| p.add(next.clone())),
                &["old", "next"],
                "old",
            ),
            (
                "activate",
                Box::new(|p| p.activate("next")),
                &["old", "next"],
                "next",
            ),
            ("remove", Box::new(|p| p.remove("old")), &["next"], "next"),
        ];
        for (name, change, ids, active) in steps {
            update_jwt_pool(&store, "default", "jwt", &log, |p| change(p)).unwrap();
            let (got_ids, got_active) = pool_ids(&store);
            assert_eq!(got_ids, ids, "after {name}");
            assert_eq!(got_active, active, "after {name}");
        }
    }

    #[test]
    fn update_jwt_pool_retries_conflict() {
        let store = pool_store("old");
        store.conflicts.set(1);
        // Another writer adds a key between our read and our write.
        let other = generate_authority("ES256", "other").unwrap();
        *store.concurrent_write.borrow_mut() = Some(Box::new(move |s: &mut Secret| {
            let mut pool = JwtPool::unmarshal(&s.data[POOL_KEY]).unwrap();
            pool.add(other.clone()).unwrap();
            s.data.insert(POOL_KEY.into(), pool.marshal());
            s.data.insert(JWKS_KEY.into(), pool.jwks().unwrap());
        }));
        let next = generate_authority("ES256", "next").unwrap();
        let (sink, err) = crate::io::Sink::buffer();
        update_jwt_pool(&store, "default", "jwt", &Logger::new(BASIC, sink), |p| {
            p.add(next.clone())
        })
        .unwrap();
        assert_eq!(pool_ids(&store).0, ["old", "other", "next"]);
        assert_eq!(
            err.contents(),
            "Conflict updating secret \"jwt\" (attempt 2); retrying...\n"
        );
    }

    #[test]
    fn update_jwt_pool_leaves_secret_on_error() {
        // The change is refused: the active key cannot be removed.
        let store = pool_store("old");
        let before = store.stored("default", "jwt");
        let err = update_jwt_pool(&store, "default", "jwt", &Logger::silent(), |p| {
            p.remove("old")
        });
        assert!(err.is_err());
        assert_eq!(store.updates.get(), 0);
        assert_eq!(store.stored("default", "jwt"), before);

        // The Secret holds no pool.
        let store = MemorySecrets::default();
        store
            .create(&Secret {
                namespace: "default".into(),
                name: "jwt".into(),
                data: [("other".to_string(), b"x".to_vec())].into(),
                ..Default::default()
            })
            .unwrap();
        let err = update_jwt_pool(&store, "default", "jwt", &Logger::silent(), |p| {
            p.remove("old")
        });
        assert!(err.is_err());
        assert_eq!(store.updates.get(), 0);
    }

    #[test]
    fn update_jwt_pool_missing_secret() {
        let store = MemorySecrets::default();
        let err = update_jwt_pool(&store, "default", "jwt", &Logger::silent(), |_| {
            panic!("change ran without a pool")
        })
        .unwrap_err();
        assert!(
            err.message().starts_with("while reading pool secret: ")
                && err.message().contains("not found"),
            "{err}"
        );
    }

    #[test]
    fn list_jwt_keys() {
        let store = pool_store("old");
        let next = small_rsa("next");
        update_jwt_pool(&store, "default", "jwt", &Logger::silent(), |p| {
            p.add(next.clone())
        })
        .unwrap();
        let (_, pool) = get_jwt_pool(&store, "default", "jwt").unwrap();
        assert_eq!(
            jwt_keys_table(&pool).render(),
            "ACTIVE   KEY ID   ALGORITHM\n*        old      ES256\n         next     RS256\n"
        );
    }

    #[test]
    fn durations() {
        assert_eq!(
            parse_duration("8760h").unwrap(),
            Duration::from_secs(365 * 24 * 3600)
        );
        assert_eq!(parse_duration("1h30m").unwrap(), Duration::from_secs(5400));
        assert_eq!(parse_duration("1.5h").unwrap(), Duration::from_secs(5400));
        assert_eq!(parse_duration("250ms").unwrap(), Duration::from_millis(250));
        assert_eq!(parse_duration("0").unwrap(), Duration::ZERO);
        for bad in ["", "h", "10", "10d", "1h-2m"] {
            assert!(parse_duration(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn make_ca_pool_defaults_to_ecdsa() {
        let (c, out, _) = ctx();
        let store = MemorySecrets::default();
        run_with(
            &c,
            &store,
            &admin(&["make-ca-pool", "--name", "ca", "--ca-id", "ca-1"]),
        )
        .unwrap();
        assert_eq!(
            out.contents(),
            "Successfully created CA pool secret default/ca\n"
        );
        let s = store.stored("default", "ca");
        assert_eq!(s.secret_type.as_deref(), Some(TLS_SECRET_TYPE));
        let key =
            rcgen::KeyPair::try_from(rustls_pem_der(&s.data[TLS_KEY_KEY]).as_slice()).unwrap();
        assert!(key.is_compatible(&rcgen::PKCS_ECDSA_P256_SHA256));
        let pool: serde_json::Value = serde_json::from_slice(&s.data[POOL_KEY]).unwrap();
        assert_eq!(pool["ActiveForSigning"], "ca-1");
    }

    fn rustls_pem_der(pem: &[u8]) -> Vec<u8> {
        use base64::Engine as _;
        let text = String::from_utf8(pem.to_vec()).unwrap();
        let body: String = text.lines().filter(|l| !l.starts_with("-----")).collect();
        base64::engine::general_purpose::STANDARD
            .decode(body)
            .unwrap()
    }

    #[test]
    fn make_ca_pool_rejects_bad_flags() {
        let (c, _, _) = ctx();
        let store = MemorySecrets::default();
        let err = run_with(
            &c,
            &store,
            &admin(&["make-ca-pool", "--name", "ca", "--key-type", "RSA"]),
        )
        .unwrap_err();
        assert!(err.is_usage() && err.message().contains("unknown key type"));
        let err = run_with(
            &c,
            &store,
            &admin(&["make-ca-pool", "--name", "ca", "--validity", "1y"]),
        )
        .unwrap_err();
        assert!(err.is_usage() && err.message().contains("--validity"));
    }

    #[test]
    fn jwt_pool_secret_with_defaults() {
        let (s, id) = new_jwt_pool_secret("ns", "jwt", "ES256", "").unwrap();
        let pool = JwtPool::unmarshal(&s.data[POOL_KEY]).unwrap();
        assert_eq!(pool.active_id(), id);
        let jwks: serde_json::Value = serde_json::from_slice(&s.data[JWKS_KEY]).unwrap();
        assert_eq!(jwks["keys"][0]["kid"], serde_json::json!(id));
        let (_, id) = new_jwt_pool_secret("ns", "jwt", "ES256", "explicit").unwrap();
        assert_eq!(id, "explicit");
        let err = new_jwt_pool_secret("ns", "jwt", "HS256", "").unwrap_err();
        assert!(err.is_usage() && err.message().contains("unsupported algorithm"));
    }

    #[test]
    fn rotation_through_the_commands() {
        let (c, out, _) = ctx();
        let store = MemorySecrets::default();
        run_with(
            &c,
            &store,
            &admin(&["make-jwt-pool", "--name", "jwt", "--key-id", "old"]),
        )
        .unwrap();
        run_with(
            &c,
            &store,
            &admin(&["add-jwt-key", "--name", "jwt", "--key-id", "new"]),
        )
        .unwrap();
        assert_eq!(pool_of(&store).active_id(), "old");
        run_with(
            &c,
            &store,
            &admin(&["activate-jwt-key", "--name", "jwt", "--key-id", "new"]),
        )
        .unwrap();
        run_with(
            &c,
            &store,
            &admin(&["remove-jwt-key", "--name", "jwt", "--key-id", "old"]),
        )
        .unwrap();
        let pool = pool_of(&store);
        assert_eq!(pool.active_id(), "new");
        assert_eq!(
            pool.authorities
                .iter()
                .map(|a| a.id.as_str())
                .collect::<Vec<_>>(),
            ["new"]
        );
        let jwks: serde_json::Value =
            serde_json::from_slice(&store.stored("default", "jwt").data[JWKS_KEY]).unwrap();
        assert_eq!(jwks["keys"].as_array().unwrap().len(), 1);
        let text = out.contents();
        assert!(text.contains(
            "Successfully created JWT authority pool secret default/jwt with ES256 key old"
        ));
        assert!(
            text.contains("Added inactive ES256 key new to JWT authority pool secret default/jwt")
        );
        assert!(text.contains("Activated key new in JWT authority pool secret default/jwt"));
        assert!(text.contains("Removed key old from JWT authority pool secret default/jwt"));
    }

    #[test]
    fn update_retries_conflicts() {
        let (c, _, err) = ctx();
        let store = MemorySecrets::default();
        run_with(
            &c,
            &store,
            &admin(&["make-jwt-pool", "--name", "jwt", "--key-id", "k1"]),
        )
        .unwrap();
        store.conflicts.set(2);
        run_with(
            &c,
            &store,
            &admin(&["add-jwt-key", "--name", "jwt", "--key-id", "k2"]),
        )
        .unwrap();
        assert_eq!(store.updates.get(), 3);
        assert_eq!(pool_of(&store).authorities.len(), 2);
        assert!(err
            .contents()
            .contains("Conflict updating secret \"jwt\" (attempt 2); retrying..."));
        assert!(err.contents().contains("(attempt 3)"));
    }

    #[test]
    fn update_gives_up_after_repeated_conflicts() {
        let (c, _, _) = ctx();
        let store = MemorySecrets::default();
        run_with(
            &c,
            &store,
            &admin(&["make-jwt-pool", "--name", "jwt", "--key-id", "k1"]),
        )
        .unwrap();
        store.conflicts.set(UPDATE_ATTEMPTS + 1);
        let err = run_with(
            &c,
            &store,
            &admin(&["activate-jwt-key", "--name", "jwt", "--key-id", "k1"]),
        )
        .unwrap_err();
        assert!(err.message().contains("while writing pool secret"), "{err}");
        assert_eq!(store.updates.get(), UPDATE_ATTEMPTS);
    }

    #[test]
    fn failed_change_leaves_the_secret() {
        let (c, _, _) = ctx();
        let store = MemorySecrets::default();
        run_with(
            &c,
            &store,
            &admin(&["make-jwt-pool", "--name", "jwt", "--key-id", "k1"]),
        )
        .unwrap();
        let before = store.stored("default", "jwt");
        for args in [
            &["remove-jwt-key", "--name", "jwt", "--key-id", "k1"][..],
            &["activate-jwt-key", "--name", "jwt", "--key-id", "missing"],
            &["add-jwt-key", "--name", "jwt", "--key-id", "k1"],
        ] {
            assert!(run_with(&c, &store, &admin(args)).is_err(), "{args:?}");
        }
        assert_eq!(store.updates.get(), 0);
        assert_eq!(store.stored("default", "jwt"), before);
    }

    #[test]
    fn missing_secret_or_pool_key() {
        let (c, _, _) = ctx();
        let store = MemorySecrets::default();
        let err = run_with(&c, &store, &admin(&["list-jwt-keys", "--name", "jwt"])).unwrap_err();
        assert!(err.message().contains("while reading pool secret"), "{err}");
        store
            .create(&Secret {
                namespace: "default".into(),
                name: "jwt".into(),
                ..Default::default()
            })
            .unwrap();
        let err = run_with(&c, &store, &admin(&["list-jwt-keys", "--name", "jwt"])).unwrap_err();
        assert!(err.message().contains("has no \"pool\" key"), "{err}");
    }

    #[test]
    fn key_table_marks_the_active_key() {
        let a = |id: &str, alg: &str| Authority {
            id: id.into(),
            algorithm: alg.into(),
            key_pkcs8: vec![],
        };
        let pool = JwtPool {
            authorities: vec![a("k1", "ES256"), a("k2", "RS256")],
            active_for_signing: "k2".into(),
        };
        assert_eq!(
            jwt_keys_table(&pool).render(),
            "ACTIVE   KEY ID   ALGORITHM\n         k1       ES256\n*        k2       RS256\n"
        );
    }
}
