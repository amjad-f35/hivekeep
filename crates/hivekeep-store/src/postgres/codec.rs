//! Row encoding and database error mapping.

use crate::resource::{check_projection, Resource};
use crate::{DeletePreconditions, StoreError};

/// Decodes a stored record, dropping fields this build does not know and
/// filling defaults.
pub(super) fn decode<T: Resource>(bytes: &[u8]) -> Result<T, StoreError> {
    let mut msg =
        T::decode(bytes).map_err(|e| StoreError::Corrupt(format!("decoding {}: {e}", T::KIND)))?;
    msg.backfill();
    Ok(msg)
}

/// Decodes a record and checks it against its projected columns.
pub(super) fn decode_checked<T: Resource>(
    uid: &str,
    version: i64,
    bytes: &[u8],
) -> Result<T, StoreError> {
    let msg: T = decode(bytes)?;
    check_projection(&msg, uid, version)?;
    Ok(msg)
}

/// Encodes a record for storage.
pub(super) fn encode<T: Resource>(msg: &T) -> Vec<u8> {
    msg.encode_to_vec()
}

/// Maps a database error onto the store's error vocabulary.
pub(super) fn db(err: sqlx::Error) -> StoreError {
    match &err {
        sqlx::Error::RowNotFound => StoreError::NotFound,
        sqlx::Error::Database(e) => match e.code().as_deref() {
            Some("23505") => StoreError::AlreadyExists,
            Some("23503") | Some("23001") => StoreError::FailedPrecondition,
            _ => StoreError::backend(err),
        },
        sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::PoolTimedOut
        | sqlx::Error::PoolClosed
        | sqlx::Error::WorkerCrashed => StoreError::Unavailable(err.to_string()),
        _ => StoreError::backend(err),
    }
}

/// Explains why a guarded delete matched no row, given what (if anything)
/// is stored under the key now.
pub(super) fn delete_miss(
    current: Option<(String, i64)>,
    guard: &DeletePreconditions,
) -> StoreError {
    let Some((uid, version)) = current else {
        return StoreError::NotFound;
    };
    let md = hivekeep_proto::api::v1::ResourceMetadata {
        uid,
        version,
        ..Default::default()
    };
    match guard.check(Some(&md)) {
        Err(e) => e,
        // The row matches the guards now, so it changed between the two
        // statements.
        Ok(()) => StoreError::VersionConflict,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hivekeep_proto::api::v1 as pb;
    use prost::Message;

    #[test]
    fn unknown_fields_are_dropped_on_read() {
        let hive = pb::Hive {
            metadata: Some(pb::ResourceMetadata {
                name: "team-a".into(),
                uid: "u".into(),
                version: 3,
                ..Default::default()
            }),
        };
        let mut bytes = hive.encode_to_vec();
        // Field 9999, varint 42: a field no message here declares.
        bytes.extend_from_slice(&[0xf8, 0xf0, 0x04, 42]);
        let got: pb::Hive = decode_checked("u", 3, &bytes).unwrap();
        assert_eq!(got, hive);
        assert_eq!(got.encode_to_vec(), hive.encode_to_vec());
        assert!(matches!(
            decode_checked::<pb::Hive>("other", 3, &bytes),
            Err(StoreError::Corrupt(_))
        ));
    }

    #[test]
    fn delete_miss_explains_itself() {
        assert!(matches!(
            delete_miss(None, &DeletePreconditions::none()),
            StoreError::NotFound
        ));
        let current = Some(("u".to_string(), 2));
        assert!(matches!(
            delete_miss(current.clone(), &DeletePreconditions::uid("x")),
            StoreError::UidConflict
        ));
        assert!(matches!(
            delete_miss(current.clone(), &DeletePreconditions::version(1)),
            StoreError::VersionConflict
        ));
        assert!(matches!(
            delete_miss(current, &DeletePreconditions::new("u", 2)),
            StoreError::VersionConflict
        ));
    }
}
