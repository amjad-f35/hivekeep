//! Field checks that go beyond the standard formats. Each takes the field's
//! path and value and returns the problems found; empty values are left to
//! presence rules unless stated otherwise.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::LazyLock;

use regex::Regex;

use crate::field::{ErrorList, FieldError, Path};
use crate::image_ref::ImageRef;
use crate::names;
use crate::resource::SINGLETON_NAME;
use crate::sandbox_class;
use crate::snapshot::{validate_snapshot_location, SnapshotUri};
use crate::spiffe::validate_spiffe_id;
use crate::volume_path::validate_projected;

/// Optional prefix of a storage volume type.
pub const VOLUME_TYPE_PREFIX: &str = "hivekeep.io/";

/// Bound on the total size of a CSI-produced map (keys plus values). CSI caps
/// these maps at 4 KiB; ten times that tolerates future revisions while still
/// bounding input.
pub const CSI_MAP_MAX_BYTES: usize = 40 * 1024;

/// Maximum length of a capability name.
pub const CAPABILITY_MAX_LEN: usize = 63;

static CAPABILITY_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^[A-Z][A-Z0-9_]*$").unwrap());
static HTTP_GET_PATH_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^/(?:[A-Za-z0-9\-._~!$&'()*+,;=:@/]|%[0-9A-Fa-f]{2})*$").unwrap()
});

/// The name of a singleton sub-resource must be `"default"`.
pub fn singleton_name(path: &Path, name: &str) -> ErrorList {
    if name.is_empty() || name == SINGLETON_NAME {
        ErrorList::new()
    } else {
        FieldError::invalid(path, name, format!("must be {SINGLETON_NAME:?}"))
            .with_origin("custom=default")
            .into()
    }
}

/// A well-formed image reference pinned by digest.
pub fn pinned_image(path: &Path, value: &str) -> ErrorList {
    if value.is_empty() {
        return ErrorList::new();
    }
    match ImageRef::parse_pinned(value) {
        Ok(_) => ErrorList::new(),
        Err(crate::image_ref::ImageRefError::NotPinned) => FieldError::invalid(
            path,
            value,
            "must be pinned by digest (changing the image invalidates snapshots)",
        )
        .into(),
        Err(e) => FieldError::invalid(
            path,
            value,
            format!("must be a well-formed image reference: {e}"),
        )
        .into(),
    }
}

/// Linux capability names: uppercase, without the `CAP_` prefix. `ALL` is
/// accepted only when `allow_all` (for drop, not add).
pub fn capabilities(path: &Path, caps: &[String], allow_all: bool) -> ErrorList {
    let mut errs = ErrorList::new();
    for (i, c) in caps.iter().enumerate() {
        let p = path.index(i);
        if c == "ALL" {
            if !allow_all {
                errs.push(FieldError::invalid(&p, c, "add does not accept 'ALL'; name the individual capabilities the container needs"));
            }
        } else if c.len() > CAPABILITY_MAX_LEN {
            errs.push(FieldError::too_long(&p, CAPABILITY_MAX_LEN));
        } else if c.starts_with("CAP_") {
            errs.push(FieldError::invalid(
                &p,
                c,
                "must be named without the 'CAP_' prefix (e.g. 'NET_BIND_SERVICE')",
            ));
        } else if !CAPABILITY_RE.is_match(c) {
            errs.push(FieldError::invalid(
                &p,
                c,
                "must be an uppercase capability name like 'NET_BIND_SERVICE'",
            ));
        }
    }
    errs
}

/// A clean absolute Unix path: starts with '/', is not '/', and has no ':',
/// `.`/`..` segments, `//`, trailing '/', or control characters.
pub fn mount_path(path: &Path, p: &str) -> ErrorList {
    if p.is_empty() {
        return ErrorList::new();
    }
    let bad = !p.starts_with('/')
        || p.len() == 1
        || p.ends_with('/')
        || p.contains("//")
        || p.contains(':')
        || p.split('/').any(|s| s == "." || s == "..")
        || p.chars().any(|c| (c as u32) < 0x20 || c as u32 == 0x7f);
    if bad {
        FieldError::invalid(
            path,
            p,
            "must be a clean absolute Unix path: must start with '/', not be '/', and contain no ':', '..', '.', '//', trailing '/', or control characters",
        )
        .into()
    } else {
        ErrorList::new()
    }
}

/// Rejects mount paths that nest under or over one another. `paths[i]` is
/// reported at `at(i)`. Identical paths are left to uniqueness checks.
pub fn nested_mount_paths_at(paths: &[&str], at: impl Fn(usize) -> Path) -> ErrorList {
    let mut errs = ErrorList::new();
    for (i, p) in paths.iter().enumerate() {
        if p.is_empty() {
            continue;
        }
        for prior in &paths[..i] {
            if prior.is_empty() || prior == p {
                continue;
            }
            if p.starts_with(&format!("{prior}/")) || prior.starts_with(&format!("{p}/")) {
                errs.push(FieldError::invalid(
                    &at(i),
                    p,
                    format!("must not nest under or over another mount ({prior:?})"),
                ));
            }
        }
    }
    errs
}

/// [`nested_mount_paths_at`] for one list, reporting at
/// `path[i].mount_path`.
pub fn nested_mount_paths(path: &Path, paths: &[&str]) -> ErrorList {
    nested_mount_paths_at(paths, |i| path.index(i).child("mount_path"))
}

/// A wakeup probe path: '/' then RFC 3986 path characters or valid
/// percent-escapes; no query or fragment.
pub fn http_get_path(path: &Path, p: &str) -> ErrorList {
    if p.is_empty() || HTTP_GET_PATH_RE.is_match(p) {
        ErrorList::new()
    } else {
        FieldError::invalid(path, p, "must be a URL path starting with '/', using only RFC 3986 path-segment characters, without query or fragment").into()
    }
}

/// An environment variable name: printable ASCII except '='.
pub fn env_var_name(path: &Path, name: &str) -> ErrorList {
    if name.is_empty()
        || name
            .bytes()
            .all(|b| (b' '..=b'~').contains(&b) && b != b'=')
    {
        ErrorList::new()
    } else {
        FieldError::invalid(
            path,
            name,
            "may contain any printable ASCII character except '='",
        )
        .into()
    }
}

/// A file path projected into a system-info volume.
pub fn projected_path(path: &Path, p: &str) -> ErrorList {
    if p.is_empty() {
        return ErrorList::new();
    }
    match validate_projected(p) {
        Ok(()) => ErrorList::new(),
        Err(e) => FieldError::invalid(path, p, e.to_string()).into(),
    }
}

/// No C0/C1 control characters (other than tab, LF and CR) in a storage
/// volume ID.
pub fn storage_volume_id(path: &Path, id: &str) -> ErrorList {
    let bad = id.chars().any(|r| {
        let r = r as u32;
        r <= 0x08
            || r == 0x0b
            || r == 0x0c
            || (0x0e..=0x1f).contains(&r)
            || (0x7f..=0x9f).contains(&r)
    });
    if bad {
        FieldError::invalid(path, id, "must not contain control characters (U+0000-U+0008, U+000B, U+000C, U+000E-U+001F, U+007F-U+009F)").into()
    } else {
        ErrorList::new()
    }
}

/// An optional [`VOLUME_TYPE_PREFIX`] followed by a DNS subdomain.
pub fn volume_type(path: &Path, t: &str) -> ErrorList {
    if t.is_empty() {
        return ErrorList::new();
    }
    let rest = t.strip_prefix(VOLUME_TYPE_PREFIX).unwrap_or(t);
    names::check_dns_subdomain(rest)
        .into_iter()
        .map(|m| FieldError::invalid(path, t, m))
        .collect()
}

/// Bounds a driver-produced map at [`CSI_MAP_MAX_BYTES`]; entries are not
/// otherwise inspected.
pub fn csi_map(path: &Path, m: &HashMap<String, String>) -> ErrorList {
    let size: usize = m.iter().map(|(k, v)| k.len() + v.len()).sum();
    if size > CSI_MAP_MAX_BYTES {
        FieldError::too_long(path, CSI_MAP_MAX_BYTES).into()
    } else {
        ErrorList::new()
    }
}

/// `host:port`, where host is an IP (IPv6 in brackets) or a DNS subdomain
/// (a trailing dot is allowed) and port is 1..=65535.
pub fn host_port(path: &Path, value: &str) -> ErrorList {
    if value.is_empty() {
        return ErrorList::new();
    }
    let split = if let Some(rest) = value.strip_prefix('[') {
        rest.split_once("]:")
            .filter(|(h, _)| !h.contains(['[', ']']))
    } else {
        value.rsplit_once(':').filter(|(h, _)| !h.contains(':'))
    };
    let Some((host, port)) = split else {
        return FieldError::invalid(path, value, "must be host:port").into();
    };
    let mut errs = ErrorList::new();
    if host.parse::<IpAddr>().is_err() {
        if value.starts_with('[') {
            errs.push(FieldError::invalid(
                path,
                value,
                "host: must be a valid IP address",
            ));
        } else {
            for m in names::check_dns_subdomain(host.strip_suffix('.').unwrap_or(host)) {
                errs.push(FieldError::invalid(path, value, format!("host: {m}")));
            }
        }
    }
    match port.parse::<u32>() {
        Ok(n) if (1..=65535).contains(&n) && port.bytes().all(|b| b.is_ascii_digit()) => {}
        _ => errs.push(FieldError::invalid(
            path,
            value,
            "port must be a number between 1 and 65535",
        )),
    }
    errs
}

/// One of the known sandbox classes.
pub fn sandbox_class(path: &Path, value: &str) -> ErrorList {
    if value.is_empty() || sandbox_class::is_valid(value) {
        ErrorList::new()
    } else {
        FieldError::not_supported(path, value, &sandbox_class::ALL).into()
    }
}

/// A valid IP address in canonical form that is not an IPv4-mapped IPv6
/// address.
pub fn ip(path: &Path, value: &str) -> ErrorList {
    let Ok(addr) = value.parse::<IpAddr>() else {
        return FieldError::invalid(path, value, "must be a valid IP address").into();
    };
    let mut errs = ErrorList::new();
    if let IpAddr::V6(v6) = addr {
        if v6.to_ipv4_mapped().is_some() {
            errs.push(FieldError::invalid(
                path,
                value,
                "must not be an IPv4-mapped IPv6 address",
            ));
        }
    }
    let canon = addr.to_string();
    if canon != value {
        errs.push(FieldError::invalid(
            path,
            value,
            format!("must be in canonical form ({canon:?})"),
        ));
    }
    errs
}

/// Each entry is a valid IP in canonical form, and there is at most one per
/// IP family.
pub fn dual_stack_ips(path: &Path, ips: &[String]) -> ErrorList {
    let mut errs = ErrorList::new();
    let (mut seen4, mut seen6) = (false, false);
    for (i, value) in ips.iter().enumerate() {
        let p = path.index(i);
        let strict = ip(&p, value);
        let Ok(addr) = value.parse::<IpAddr>() else {
            errs.extend(strict);
            continue;
        };
        errs.extend(strict);
        let seen = if addr.is_ipv4() {
            &mut seen4
        } else {
            &mut seen6
        };
        if *seen {
            errs.push(FieldError::invalid(
                &p,
                value,
                "must not contain more than one address per IP family",
            ));
        }
        *seen = true;
    }
    errs
}

/// A syntactically valid SPIFFE ID.
pub fn spiffe_id(path: &Path, id: &str) -> ErrorList {
    if id.is_empty() {
        return ErrorList::new();
    }
    match validate_spiffe_id(id) {
        Ok(()) => ErrorList::new(),
        Err(e) => FieldError::invalid(path, id, e.to_string()).into(),
    }
}

/// A snapshot storage location (see [`validate_snapshot_location`]).
pub fn snapshot_location(path: &Path, location: &str) -> ErrorList {
    match validate_snapshot_location(location) {
        Ok(()) => ErrorList::new(),
        Err(e) => FieldError::invalid(path, location, e.to_string()).into(),
    }
}

/// A snapshot URI following the storage layout.
pub fn snapshot_uri(path: &Path, uri: &str) -> ErrorList {
    match SnapshotUri::parse(uri) {
        Ok(_) => ErrorList::new(),
        Err(e) => FieldError::invalid(path, uri, e.to_string()).into(),
    }
}

/// An absolute, lexically clean directory path.
pub fn abs_clean_dir(path: &Path, dir: &str) -> ErrorList {
    if dir.is_empty() {
        return ErrorList::new();
    }
    let clean = dir.starts_with('/')
        && (dir == "/" || !dir.ends_with('/'))
        && !dir.contains("//")
        && !dir.split('/').any(|s| s == "." || s == "..");
    if clean {
        ErrorList::new()
    } else {
        FieldError::invalid(path, dir, "must be an absolute, clean path").into()
    }
}

/// A worker pod UID that is safe to use as a path element: a DNS label.
pub fn worker_pod_uid(path: &Path, uid: &str) -> ErrorList {
    let msgs = names::check_dns_label(uid);
    if msgs.is_empty() {
        ErrorList::new()
    } else {
        FieldError::invalid(path, uid, msgs.join("; ")).into()
    }
}

/// Validates timestamps: seconds within years 1..=9999 and nanos within a
/// second.
pub fn timestamp(path: &Path, ts: &prost_types::Timestamp) -> ErrorList {
    const MIN: i64 = -62_135_596_800;
    const MAX: i64 = 253_402_300_799;
    if !(MIN..=MAX).contains(&ts.seconds) || !(0..1_000_000_000).contains(&ts.nanos) {
        FieldError::invalid(
            path,
            format!("{}s {}ns", ts.seconds, ts.nanos),
            "must be a valid timestamp",
        )
        .into()
    } else {
        ErrorList::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(e: ErrorList) -> bool {
        e.is_empty()
    }

    fn p() -> Path {
        Path::from("f")
    }

    #[test]
    fn mount_paths() {
        for good in ["", "/data", "/a/b-c/d.e"] {
            assert!(ok(mount_path(&p(), good)), "{good}");
        }
        for bad in [
            "data", "/", "/a/", "/a//b", "/a:b", "/a/./b", "/a/../b", "/..", "/a\u{1}",
        ] {
            assert!(!ok(mount_path(&p(), bad)), "{bad}");
        }
    }

    #[test]
    fn nested_mounts() {
        let e = nested_mount_paths(&Path::from("m"), &["/a", "/a/b", "/ab", "/a", ""]);
        let paths: Vec<_> = e.iter().map(|e| e.path.as_str()).collect();
        assert_eq!(paths, vec!["m[1].mount_path", "m[3].mount_path"]);
        let e = nested_mount_paths_at(&["/x/y", "/x"], |i| Path::from(format!("p{i}").as_str()));
        assert_eq!(
            e.iter().map(|e| e.path.as_str()).collect::<Vec<_>>(),
            vec!["p1"]
        );
    }

    #[test]
    fn http_paths_env_and_projection() {
        for good in ["", "/", "/healthz", "/a%20b", "/a:b@c"] {
            assert!(ok(http_get_path(&p(), good)), "{good}");
        }
        for bad in ["healthz", "/a?b", "/a#b", "/a%zz", "/a b"] {
            assert!(!ok(http_get_path(&p(), bad)), "{bad}");
        }
        assert!(ok(env_var_name(&p(), "PATH")) && ok(env_var_name(&p(), "a b-c.d")));
        assert!(
            !ok(env_var_name(&p(), "A=B"))
                && !ok(env_var_name(&p(), "A\tB"))
                && !ok(env_var_name(&p(), "é"))
        );
        assert!(ok(projected_path(&p(), "a/b")) && !ok(projected_path(&p(), "/a")));
    }

    #[test]
    fn storage_ids_types_and_maps() {
        assert!(ok(storage_volume_id(&p(), "vol-1\twith tab")));
        assert!(
            !ok(storage_volume_id(&p(), "a\u{0}b")) && !ok(storage_volume_id(&p(), "a\u{85}b"))
        );
        assert!(
            ok(volume_type(&p(), "hivekeep.io/pd-ssd"))
                && ok(volume_type(&p(), "pd.csi.example.com"))
        );
        assert!(!ok(volume_type(&p(), "Bad_Type")) && !ok(volume_type(&p(), "hivekeep.io/")));
        let big: HashMap<String, String> =
            [("k".to_string(), "v".repeat(CSI_MAP_MAX_BYTES))].into();
        assert!(!ok(csi_map(&p(), &big)));
        let fits: HashMap<String, String> =
            [("k".to_string(), "v".repeat(CSI_MAP_MAX_BYTES - 1))].into();
        assert!(ok(csi_map(&p(), &fits)));
    }

    #[test]
    fn capability_names() {
        let caps = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert!(ok(capabilities(
            &p(),
            &caps(&["NET_ADMIN", "SYS_PTRACE"]),
            false
        )));
        assert!(ok(capabilities(&p(), &caps(&["ALL"]), true)));
        let e = capabilities(
            &p(),
            &caps(&["ALL", "CAP_NET_ADMIN", "net_admin", &"A".repeat(64)]),
            false,
        );
        assert_eq!(
            e.iter().map(|e| e.path.as_str()).collect::<Vec<_>>(),
            vec!["f[0]", "f[1]", "f[2]", "f[3]"]
        );
    }

    #[test]
    fn host_ports() {
        for good in [
            "",
            "gw.example.com:443",
            "gw.example.com.:8080",
            "10.0.0.1:1",
            "[2001:db8::1]:65535",
        ] {
            assert!(ok(host_port(&p(), good)), "{good}");
        }
        for bad in [
            "gw.example.com",
            "gw:0",
            "gw:65536",
            "gw:http",
            "Bad_Host:80",
            "2001:db8::1:80",
            "[zz]:80",
            ":80",
        ] {
            assert!(!ok(host_port(&p(), bad)), "{bad}");
        }
    }

    #[test]
    fn ips() {
        assert!(ok(ip(&p(), "10.0.0.1")) && ok(ip(&p(), "2001:db8::1")));
        assert!(
            !ok(ip(&p(), "::ffff:10.0.0.1"))
                && !ok(ip(&p(), "2001:DB8::1"))
                && !ok(ip(&p(), "010.0.0.1"))
        );
        let v = |s: &[&str]| s.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        assert!(ok(dual_stack_ips(&p(), &v(&["10.0.0.1", "fd00::1"]))));
        assert!(!ok(dual_stack_ips(&p(), &v(&["10.0.0.1", "10.0.0.2"]))));
        assert!(!ok(dual_stack_ips(&p(), &v(&["nope"]))));
    }

    #[test]
    fn misc() {
        assert!(ok(singleton_name(&p(), "default")) && ok(singleton_name(&p(), "")));
        assert_eq!(
            singleton_name(&p(), "other").iter().next().unwrap().origin,
            Some("custom=default")
        );
        assert!(ok(sandbox_class(&p(), "gvisor")) && !ok(sandbox_class(&p(), "kata")));
        assert!(ok(pinned_image(&p(), "")) && !ok(pinned_image(&p(), "example.com/app:v1")));
        assert!(
            ok(abs_clean_dir(&p(), "/var/lib/x"))
                && !ok(abs_clean_dir(&p(), "var"))
                && !ok(abs_clean_dir(&p(), "/a/../b"))
        );
        assert!(
            ok(worker_pod_uid(&p(), "123e4567-e89b-12d3-a456-426614174000"))
                && !ok(worker_pod_uid(&p(), "../x"))
        );
        assert!(ok(timestamp(
            &p(),
            &prost_types::Timestamp {
                seconds: 0,
                nanos: 0
            }
        )));
        assert!(!ok(timestamp(
            &p(),
            &prost_types::Timestamp {
                seconds: 0,
                nanos: -1
            }
        )));
        assert!(ok(spiffe_id(&p(), "spiffe://td/a")) && !ok(spiffe_id(&p(), "http://x")));
        assert!(ok(snapshot_location(&p(), "gs://b/r")) && !ok(snapshot_location(&p(), "gs://")));
        assert!(!ok(snapshot_uri(&p(), "gs://b/r")));
    }
}
