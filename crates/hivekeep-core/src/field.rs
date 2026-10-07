//! Field paths and structured validation errors.
//!
//! A validation pass produces an [`ErrorList`]: one [`FieldError`] per
//! problem, each naming the offending field with a [`Path`] such as
//! `actor.metadata.name` or `containers[0].env[2].name`. The list renders to
//! a single message for an RPC status and keeps enough structure for tests
//! to assert on the kind, path and originating rule of each error.

use std::fmt;

/// A dotted path to a field inside a message, e.g. `spec.containers[0].name`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Path(String);

impl Path {
    /// The empty path, naming the value being validated itself.
    pub fn root() -> Self {
        Path(String::new())
    }

    /// A path made of the given field names joined by dots.
    pub fn new<I, S>(parts: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        parts
            .into_iter()
            .fold(Path::root(), |p, s| p.child(s.as_ref()))
    }

    /// The path of a named sub-field.
    pub fn child(&self, name: &str) -> Path {
        if self.0.is_empty() {
            Path(name.to_string())
        } else {
            Path(format!("{}.{}", self.0, name))
        }
    }

    /// The path of the `i`th element of a list.
    pub fn index(&self, i: usize) -> Path {
        Path(format!("{}[{}]", self.0, i))
    }

    /// The path of a map entry.
    pub fn key(&self, k: &str) -> Path {
        Path(format!("{}[{}]", self.0, k))
    }

    /// Whether this is the root path.
    pub fn is_root(&self) -> bool {
        self.0.is_empty()
    }

    /// The path as a string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Path {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_empty() {
            f.write_str("<root>")
        } else {
            f.write_str(&self.0)
        }
    }
}

impl From<&str> for Path {
    fn from(s: &str) -> Self {
        Path(s.to_string())
    }
}

/// What kind of problem a [`FieldError`] reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ErrorKind {
    /// A required field is missing.
    Required,
    /// A field is set although it must not be.
    Forbidden,
    /// A field has a malformed or out-of-range value.
    Invalid,
    /// A value appears more than once where it must be unique.
    Duplicate,
    /// A value is not one of the supported choices.
    NotSupported,
    /// A string or byte field is too long.
    TooLong,
    /// A list or map has too many entries.
    TooMany,
    /// A list or map has too few entries.
    TooFew,
    /// The validator itself hit an unexpected condition.
    Internal,
}

impl ErrorKind {
    fn describe(self) -> &'static str {
        match self {
            ErrorKind::Required => "Required value",
            ErrorKind::Forbidden => "Forbidden",
            ErrorKind::Invalid => "Invalid value",
            ErrorKind::Duplicate => "Duplicate value",
            ErrorKind::NotSupported => "Unsupported value",
            ErrorKind::TooLong => "Too long",
            ErrorKind::TooMany => "Too many",
            ErrorKind::TooFew => "Too few",
            ErrorKind::Internal => "Internal error",
        }
    }
}

/// One validation problem on one field.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FieldError {
    /// What went wrong.
    pub kind: ErrorKind,
    /// Where it went wrong.
    pub path: Path,
    /// The offending value, rendered for humans. Never carries secret values.
    pub bad_value: Option<String>,
    /// Extra explanation.
    pub detail: String,
    /// The rule that produced the error (e.g. `maxItems`, `format=dns-label`),
    /// for tests and diagnostics.
    pub origin: Option<&'static str>,
}

impl FieldError {
    fn new(
        kind: ErrorKind,
        path: &Path,
        bad_value: Option<String>,
        detail: impl Into<String>,
    ) -> Self {
        FieldError {
            kind,
            path: path.clone(),
            bad_value,
            detail: detail.into(),
            origin: None,
        }
    }

    /// A required field is missing.
    pub fn required(path: &Path, detail: impl Into<String>) -> Self {
        Self::new(ErrorKind::Required, path, None, detail)
    }

    /// A field is set although it must not be.
    pub fn forbidden(path: &Path, detail: impl Into<String>) -> Self {
        Self::new(ErrorKind::Forbidden, path, None, detail)
    }

    /// A field has a bad value.
    pub fn invalid(path: &Path, value: impl fmt::Debug, detail: impl Into<String>) -> Self {
        Self::new(ErrorKind::Invalid, path, Some(format!("{value:?}")), detail)
    }

    /// A field has a bad value that is not echoed back.
    pub fn invalid_opaque(path: &Path, detail: impl Into<String>) -> Self {
        Self::new(ErrorKind::Invalid, path, None, detail)
    }

    /// A value is repeated.
    pub fn duplicate(path: &Path, value: impl fmt::Debug) -> Self {
        Self::new(ErrorKind::Duplicate, path, Some(format!("{value:?}")), "")
    }

    /// A value is not one of `supported`.
    pub fn not_supported(path: &Path, value: impl fmt::Debug, supported: &[&str]) -> Self {
        let quoted: Vec<String> = supported.iter().map(|s| format!("{s:?}")).collect();
        Self::new(
            ErrorKind::NotSupported,
            path,
            Some(format!("{value:?}")),
            format!("supported values: {}", quoted.join(", ")),
        )
    }

    /// A string or byte field exceeds `max` bytes.
    pub fn too_long(path: &Path, max: usize) -> Self {
        Self::new(
            ErrorKind::TooLong,
            path,
            None,
            format!("may not be more than {max} bytes"),
        )
    }

    /// A collection has `actual` entries, more than `max`.
    pub fn too_many(path: &Path, actual: usize, max: usize) -> Self {
        Self::new(
            ErrorKind::TooMany,
            path,
            Some(actual.to_string()),
            format!("must have at most {max} items"),
        )
    }

    /// A collection has `actual` entries, fewer than `min`.
    pub fn too_few(path: &Path, actual: usize, min: usize) -> Self {
        Self::new(
            ErrorKind::TooFew,
            path,
            Some(actual.to_string()),
            format!("must have at least {min} items"),
        )
    }

    /// The validator hit an unexpected condition.
    pub fn internal(path: &Path, detail: impl Into<String>) -> Self {
        Self::new(ErrorKind::Internal, path, None, detail)
    }

    /// Records the rule that produced the error.
    pub fn with_origin(mut self, origin: &'static str) -> Self {
        self.origin = Some(origin);
        self
    }
}

impl fmt::Display for FieldError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.path, self.kind.describe())?;
        if let Some(v) = &self.bad_value {
            write!(f, ": {v}")?;
        }
        if !self.detail.is_empty() {
            write!(f, ": {}", self.detail)?;
        }
        Ok(())
    }
}

/// An ordered list of validation problems. Empty means valid.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ErrorList(Vec<FieldError>);

impl ErrorList {
    /// An empty list.
    pub fn new() -> Self {
        ErrorList(Vec::new())
    }

    /// Adds one error.
    pub fn push(&mut self, e: FieldError) {
        self.0.push(e);
    }

    /// Appends every error of `other`.
    pub fn extend(&mut self, other: ErrorList) {
        self.0.extend(other.0);
    }

    /// Whether there are no errors.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Number of errors.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Iterates over the errors.
    pub fn iter(&self) -> std::slice::Iter<'_, FieldError> {
        self.0.iter()
    }

    /// The errors as a slice.
    pub fn as_slice(&self) -> &[FieldError] {
        &self.0
    }

    /// `Ok(())` when empty, otherwise a [`ValidationError`].
    pub fn into_result(self) -> Result<(), ValidationError> {
        if self.is_empty() {
            Ok(())
        } else {
            Err(ValidationError(self))
        }
    }

    /// Renders all errors as one message.
    pub fn message(&self) -> String {
        match self.0.len() {
            0 => "validation failed".to_string(),
            1 => self.0[0].to_string(),
            _ => {
                let parts: Vec<String> = self.0.iter().map(ToString::to_string).collect();
                format!("[{}]", parts.join(", "))
            }
        }
    }
}

impl From<FieldError> for ErrorList {
    fn from(e: FieldError) -> Self {
        ErrorList(vec![e])
    }
}

impl From<Vec<FieldError>> for ErrorList {
    fn from(v: Vec<FieldError>) -> Self {
        ErrorList(v)
    }
}

impl FromIterator<FieldError> for ErrorList {
    fn from_iter<I: IntoIterator<Item = FieldError>>(iter: I) -> Self {
        ErrorList(iter.into_iter().collect())
    }
}

impl IntoIterator for ErrorList {
    type Item = FieldError;
    type IntoIter = std::vec::IntoIter<FieldError>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a ErrorList {
    type Item = &'a FieldError;
    type IntoIter = std::slice::Iter<'a, FieldError>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

/// A non-empty [`ErrorList`] as an error value.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{}", .0.message())]
pub struct ValidationError(pub ErrorList);

impl ValidationError {
    /// The underlying errors.
    pub fn errors(&self) -> &ErrorList {
        &self.0
    }

    /// The `INVALID_ARGUMENT` status an RPC handler answers a bad request with.
    pub fn to_invalid_argument(&self) -> tonic::Status {
        tonic::Status::invalid_argument(self.0.message())
    }

    /// The `INTERNAL` status for a server-built object that failed validation,
    /// which is a server bug rather than a client mistake.
    pub fn to_internal(&self) -> tonic::Status {
        tonic::Status::internal(self.0.message())
    }
}

impl From<ValidationError> for tonic::Status {
    fn from(e: ValidationError) -> Self {
        e.to_invalid_argument()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_render() {
        let p = Path::new(["actor", "metadata"]).child("name");
        assert_eq!(p.as_str(), "actor.metadata.name");
        assert_eq!(
            Path::root().child("c").index(2).key("k").as_str(),
            "c[2][k]"
        );
        assert_eq!(Path::root().to_string(), "<root>");
    }

    #[test]
    fn errors_render_and_convert() {
        let p = Path::from("a.b");
        let list: ErrorList = vec![
            FieldError::required(&p, ""),
            FieldError::invalid(&p, "x", "bad").with_origin("format=dns-label"),
        ]
        .into();
        assert_eq!(
            list.message(),
            "[a.b: Required value, a.b: Invalid value: \"x\": bad]"
        );
        let err = list.clone().into_result().unwrap_err();
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(ErrorList::new().into_result().is_ok());
    }
}
