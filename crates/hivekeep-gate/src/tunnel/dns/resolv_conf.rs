//! Upstream resolvers from a resolv.conf.

use std::io;
use std::net::IpAddr;
use std::path::Path;

/// The `nameserver` entries of the resolv.conf at `path`, as `ip:53`
/// addresses in file order. Comments (`#`, `;`) and entries that are not IP
/// addresses are ignored; a file without a usable entry is an error.
pub fn nameservers(path: &Path) -> io::Result<Vec<String>> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| io::Error::new(e.kind(), format!("dns: reading resolv.conf: {e}")))?;
    let mut out = Vec::new();
    for line in content.lines() {
        let mut line = line.trim();
        if let Some(i) = line.find(['#', ';']) {
            line = line[..i].trim();
        }
        let Some(rest) = line.strip_prefix("nameserver") else {
            continue;
        };
        let address = rest.trim();
        match address.parse::<IpAddr>() {
            Ok(IpAddr::V6(v6)) => out.push(format!("[{v6}]:53")),
            Ok(IpAddr::V4(v4)) => out.push(format!("{v4}:53")),
            Err(_) => continue,
        }
    }
    if out.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("dns: {} names no usable nameserver", path.display()),
        ));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolv_conf_nameservers() {
        let cases: Vec<(&str, &str, Option<Vec<&str>>)> = vec![
            (
                "cluster resolv.conf",
                "search hivekeep-system.svc.cluster.local svc.cluster.local\nnameserver 10.96.0.10\noptions ndots:5\n",
                Some(vec!["10.96.0.10:53"]),
            ),
            ("several, in order", "nameserver 10.96.0.10\nnameserver 8.8.8.8\n", Some(vec!["10.96.0.10:53", "8.8.8.8:53"])),
            (
                "comments and blanks",
                "# generated\n\n  nameserver 10.96.0.10  # cluster\n;nameserver 1.1.1.1\n",
                Some(vec!["10.96.0.10:53"]),
            ),
            ("no nameserver", "search cluster.local\n", None),
            ("unparsable address", "nameserver not-an-ip\n", None),
        ];
        for (name, content, want) in cases {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("resolv.conf");
            std::fs::write(&path, content).unwrap();
            let got = nameservers(&path);
            match want {
                Some(want) => assert_eq!(got.unwrap(), want, "{name}"),
                None => assert!(got.is_err(), "{name}"),
            }
        }
    }
}
