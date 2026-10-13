//! Classifying API errors.

/// The API status code of `err`, if it is an API error.
pub fn code(err: &kube::Error) -> Option<u16> {
    match err {
        kube::Error::Api(s) => Some(s.code),
        _ => None,
    }
}

/// Whether `err` is a 404 from the API server.
pub fn is_not_found(err: &kube::Error) -> bool {
    code(err) == Some(404)
}

/// Whether `err` is a 409 Conflict (not AlreadyExists).
pub fn is_conflict(err: &kube::Error) -> bool {
    matches!(err, kube::Error::Api(s) if s.code == 409 && s.reason != "AlreadyExists")
}

/// Whether `err` is a 409 AlreadyExists.
pub fn is_already_exists(err: &kube::Error) -> bool {
    matches!(err, kube::Error::Api(s) if s.code == 409 && s.reason == "AlreadyExists")
}

/// Whether `err` is a 403.
pub fn is_forbidden(err: &kube::Error) -> bool {
    code(err) == Some(403)
}

/// Whether `err` is a 422 Invalid.
pub fn is_invalid(err: &kube::Error) -> bool {
    code(err) == Some(422)
}

/// Maps a 404 to `None`.
pub fn ignore_not_found<T>(r: Result<T, kube::Error>) -> Result<Option<T>, kube::Error> {
    match r {
        Ok(v) => Ok(Some(v)),
        Err(e) if is_not_found(&e) => Ok(None),
        Err(e) => Err(e),
    }
}
