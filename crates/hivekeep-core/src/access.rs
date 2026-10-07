//! Roles and members of access policies.

/// Full control.
pub const ROLE_OWNER: &str = "owner";
/// Read and write, without managing access.
pub const ROLE_EDITOR: &str = "editor";
/// Read only.
pub const ROLE_VIEWER: &str = "viewer";

/// Roles that may be granted by the deployment-wide policy.
pub const GLOBAL_ROLES: [&str; 2] = [ROLE_OWNER, ROLE_VIEWER];
/// Roles that may be granted by a Hive's policy.
pub const HIVE_ROLES: [&str; 3] = [ROLE_OWNER, ROLE_EDITOR, ROLE_VIEWER];

/// Maximum number of members across all bindings of one policy.
pub const MAX_MEMBERS_PER_POLICY: usize = 1500;

/// Prefix of every member string.
pub const USER_PREFIX: &str = "user:";

/// Why a member string is unacceptable.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MemberError {
    /// Not of the form `user:<id>`.
    #[error("member {0:?} must have non-empty \"user:<id>\" format")]
    Format(String),
    /// `user:*` is not allowed.
    #[error("wildcard member {0:?} is not allowed")]
    Wildcard(String),
    /// The id contains control characters.
    #[error("member {0:?} must not contain control characters")]
    ControlCharacter(String),
}

/// Validates a policy member (`"user:<id>"`) and returns its id.
pub fn parse_member(member: &str) -> Result<&str, MemberError> {
    let id = member
        .strip_prefix(USER_PREFIX)
        .filter(|id| !id.trim().is_empty())
        .ok_or_else(|| MemberError::Format(member.to_string()))?;
    if id == "*" {
        return Err(MemberError::Wildcard(member.to_string()));
    }
    if id.chars().any(char::is_control) {
        return Err(MemberError::ControlCharacter(member.to_string()));
    }
    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn members() {
        assert_eq!(
            parse_member("user:alice@example.com"),
            Ok("alice@example.com")
        );
        assert!(matches!(parse_member("alice"), Err(MemberError::Format(_))));
        assert!(matches!(parse_member("user:"), Err(MemberError::Format(_))));
        assert!(matches!(
            parse_member("user:  "),
            Err(MemberError::Format(_))
        ));
        assert!(matches!(
            parse_member("user:*"),
            Err(MemberError::Wildcard(_))
        ));
        assert!(matches!(
            parse_member("user:a\u{7}b"),
            Err(MemberError::ControlCharacter(_))
        ));
    }
}
