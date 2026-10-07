//! The rule for relative paths at which files are projected into an Actor's
//! volumes. The API server applies it when a template is created and the
//! keeper applies it again before writing to the host, both through this one
//! function.

/// Maximum number of segments in a projected path.
pub const MAX_SEGMENTS: usize = 16;

/// Why a projected path is unacceptable.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProjectedPathError {
    /// The path is empty.
    #[error("must not be empty")]
    Empty,
    /// The path contains a NUL byte.
    #[error("must not contain a NUL byte")]
    Nul,
    /// The path is too deep.
    #[error("must have at most {MAX_SEGMENTS} path segments")]
    TooDeep,
    /// The path is absolute, has a trailing or doubled '/'.
    #[error(
        "must be a relative path with no empty segments: no leading, trailing, or doubled '/'"
    )]
    EmptySegment,
    /// The path has a `.` or `..` segment.
    #[error("must not contain a {0:?} path segment")]
    DotSegment(&'static str),
}

/// Checks a projected file path relative to the volume root: non-empty, no
/// NUL, at most [`MAX_SEGMENTS`] '/'-separated segments, none of them empty,
/// `.` or `..`. Any other byte is allowed.
pub fn validate_projected(p: &str) -> Result<(), ProjectedPathError> {
    if p.is_empty() {
        return Err(ProjectedPathError::Empty);
    }
    if p.contains('\0') {
        return Err(ProjectedPathError::Nul);
    }
    let segments: Vec<&str> = p.split('/').collect();
    if segments.len() > MAX_SEGMENTS {
        return Err(ProjectedPathError::TooDeep);
    }
    for s in segments {
        match s {
            "" => return Err(ProjectedPathError::EmptySegment),
            "." => return Err(ProjectedPathError::DotSegment(".")),
            ".." => return Err(ProjectedPathError::DotSegment("..")),
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn projected_paths() {
        for ok in [
            "ca.pem",
            "certs/ca.pem",
            "méta/имя",
            "with space",
            ".hidden",
            "..dots",
        ] {
            assert!(validate_projected(ok).is_ok(), "{ok}");
        }
        for bad in [
            "",
            "/etc/passwd",
            "a/",
            "a//b",
            ".",
            "..",
            "./a",
            "a/../b",
            "a/./b",
            "a\0b",
        ] {
            assert!(validate_projected(bad).is_err(), "{bad:?}");
        }
        assert!(validate_projected(&format!("{}f", "d/".repeat(MAX_SEGMENTS - 1))).is_ok());
        assert!(validate_projected(&format!("{}f", "d/".repeat(MAX_SEGMENTS))).is_err());
        assert!(validate_projected(&["a"; 16].join("/")).is_ok());
        assert_eq!(
            validate_projected(&["a"; 17].join("/")),
            Err(ProjectedPathError::TooDeep)
        );
    }
}
