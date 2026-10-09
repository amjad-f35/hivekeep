//! Image pattern matching as the kubelet's credential providers do it.

/// Whether `image` matches `pattern`: the same number of host labels, each
/// matching its glob, the same port, and a path that starts with the
/// pattern's path (as a string prefix, as the kubelet compares).
pub fn matches_image(pattern: &str, image: &str) -> Result<bool, String> {
    let (ph, pp, ppath) =
        split(pattern).map_err(|e| format!("while parsing match pattern {pattern:?}: {e}"))?;
    let (ih, ip, ipath) =
        split(image).map_err(|e| format!("while parsing image {image:?}: {e}"))?;
    if pp != ip {
        return Ok(false);
    }
    let pl: Vec<&str> = ph.split('.').collect();
    let il: Vec<&str> = ih.split('.').collect();
    if pl.len() != il.len() {
        return Ok(false);
    }
    for (g, s) in pl.iter().zip(&il) {
        if !glob(g, s)? {
            return Ok(false);
        }
    }
    Ok(ipath.starts_with(ppath))
}

/// The host, port and path of a scheme-less reference.
fn split(s: &str) -> Result<(&str, &str, &str), String> {
    let (hostport, path) = match s.find('/') {
        Some(i) => (&s[..i], &s[i..]),
        None => (s, ""),
    };
    if hostport.is_empty() {
        return Err("empty host".into());
    }
    if hostport.contains(['?', '#', ' ']) {
        return Err("invalid host".into());
    }
    let (host, port) = match hostport.rsplit_once(':') {
        Some((h, p)) if !h.contains(':') => (h, p),
        _ => (hostport, ""),
    };
    Ok((host, port, path))
}

/// Shell-style matching of one label: `*`, `?` and `[...]` classes.
pub fn glob(pattern: &str, s: &str) -> Result<bool, String> {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = s.chars().collect();
    glob_at(&p, &t)
}

fn glob_at(p: &[char], t: &[char]) -> Result<bool, String> {
    match p.first() {
        None => Ok(t.is_empty()),
        Some('*') => {
            for i in 0..=t.len() {
                if glob_at(&p[1..], &t[i..])? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Some('?') => Ok(!t.is_empty() && glob_at(&p[1..], &t[1..])?),
        Some('[') => {
            let end = p
                .iter()
                .position(|c| *c == ']')
                .ok_or("syntax error in pattern")?;
            let Some(c) = t.first() else { return Ok(false) };
            let mut class = &p[1..end];
            let negate = matches!(class.first(), Some('^') | Some('!'));
            if negate {
                class = &class[1..];
            }
            let mut hit = false;
            let mut i = 0;
            while i < class.len() {
                if i + 2 < class.len() && class[i + 1] == '-' {
                    if class[i] <= *c && *c <= class[i + 2] {
                        hit = true;
                    }
                    i += 3;
                } else {
                    if class[i] == *c {
                        hit = true;
                    }
                    i += 1;
                }
            }
            Ok(hit != negate && glob_at(&p[end + 1..], &t[1..])?)
        }
        Some(ch) => Ok(t.first() == Some(ch) && glob_at(&p[1..], &t[1..])?),
    }
}

/// The registry part of an image reference.
pub fn registry_of(image: &str) -> &str {
    match image.find('/') {
        Some(i) => &image[..i],
        None => image,
    }
}

/// The repository of an image reference: the reference without its tag or
/// digest, the granularity the provider protocol's cache keys use.
pub fn repository_of(image: &str) -> &str {
    let image = image.split_once('@').map_or(image, |(r, _)| r);
    match image.rfind(':') {
        Some(i) if !image[i..].contains('/') => &image[..i],
        _ => image,
    }
}

/// The keys of `auth` that match `image`, most specific first.
pub fn matching_auth_keys<'a>(
    keys: impl IntoIterator<Item = &'a String>,
    image: &str,
) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    for k in keys {
        if matches_image(k, image)? {
            out.push(k.clone());
        }
    }
    sort_auth_keys(&mut out);
    Ok(out)
}

/// Sorts auth keys most specific first (reverse lexical), as the kubelet
/// orders them.
pub fn sort_auth_keys(keys: &mut [String]) {
    keys.sort_by(|a, b| b.cmp(a));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kubelet_matching_rules() {
        let cases = [
            ("gcr.io", "gcr.io/proj/img", true),
            ("gcr.io", "gcr.io", true),
            ("gcr.io", "quay.io/proj/img", false),
            ("*.gcr.io", "us.gcr.io/proj/img", true),
            ("*.gcr.io", "gcr.io/proj/img", false),
            ("*.gcr.io", "a.b.gcr.io/proj/img", false),
            (
                "*.pkg.dev",
                "us-central1-docker.pkg.dev/proj/repo/img",
                true,
            ),
            ("app*.k8s.io", "appfoo.k8s.io/img", true),
            ("k8s.*", "k8s.io/img", true),
            ("*.*.registry.io", "a.b.registry.io/img", true),
            ("registry.io/path", "registry.io/path/deeper/img", true),
            ("registry.io/path", "registry.io/other/img", false),
            ("registry.io/team/", "registry.io/team/img", true),
            ("registry.io/team", "registry.io/teammate/img", true),
            ("registry.io/a/b/c", "registry.io/a", false),
            ("registry.io:8080/path", "registry.io:8080/path/img", true),
            ("registry.io:8080/path", "registry.io:9090/path/img", false),
            ("registry.io:8080", "registry.io/img", false),
            ("registry.io", "registry.io:8080/img", false),
            ("localhost:5000", "localhost:5000/img", true),
        ];
        for (p, i, want) in cases {
            assert_eq!(matches_image(p, i).unwrap(), want, "{p} vs {i}");
        }
        assert!(matches_image("[", "x/y").is_err());
    }

    #[test]
    fn globs_and_helpers() {
        assert!(glob("a?c", "abc").unwrap());
        assert!(glob("[a-c]x", "bx").unwrap());
        assert!(!glob("[!a-c]x", "bx").unwrap());
        assert_eq!(registry_of("gcr.io/p/i"), "gcr.io");
        let mut k = vec!["gcr.io".to_string(), "gcr.io/proj".to_string()];
        sort_auth_keys(&mut k);
        assert_eq!(k[0], "gcr.io/proj");
    }
}
