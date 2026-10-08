//! Generic statements over the resource tables, which all share the shape
//! `(<key columns>, uid, version, proto)`.

use sqlx::{AssertSqlSafe, PgConnection, Row};

use super::codec::{db, decode, decode_checked, delete_miss, encode};
use crate::page_token::{self, ListKind};
use crate::resource::{prepare_create, prepare_update, Resource};
use crate::{DeletePreconditions, ListOptions, ListResponse, Result, StoreError};

/// A resource table and its key columns.
#[derive(Debug, Clone, Copy)]
pub(super) struct Table {
    pub name: &'static str,
    pub keys: &'static [&'static str],
}

pub(super) const HIVES: Table = Table {
    name: "hives",
    keys: &["name"],
};
pub(super) const ACTORS: Table = Table {
    name: "actors",
    keys: &["hive", "name"],
};
pub(super) const EGRESS_POLICIES: Table = Table {
    name: "actor_egress_policies",
    keys: &["hive", "actor_name"],
};
pub(super) const TEMPLATES: Table = Table {
    name: "actor_templates",
    keys: &["hive", "name"],
};
pub(super) const TAGS: Table = Table {
    name: "tags",
    keys: &["hive", "name"],
};
pub(super) const WORKERS: Table = Table {
    name: "workers",
    keys: &["name"],
};
pub(super) const HIVE_ACCESS_POLICIES: Table = Table {
    name: "hive_access_policies",
    keys: &["hive"],
};
pub(super) const GLOBAL_ACCESS_POLICY: Table = Table {
    name: "global_access_policy",
    keys: &["id"],
};

impl Table {
    /// `k1 = $1 AND k2 = $2`, numbering from `$1`.
    fn key_match(&self) -> String {
        if self.name == GLOBAL_ACCESS_POLICY.name {
            return "id = true".into();
        }
        self.keys
            .iter()
            .enumerate()
            .map(|(i, k)| format!("{k} = ${}", i + 1))
            .collect::<Vec<_>>()
            .join(" AND ")
    }

    fn bound_keys(&self) -> usize {
        if self.name == GLOBAL_ACCESS_POLICY.name {
            0
        } else {
            self.keys.len()
        }
    }
}

macro_rules! bind_keys {
    ($q:expr, $keys:expr) => {{
        let mut q = $q;
        for k in $keys {
            q = q.bind(k.to_string());
        }
        q
    }};
}

/// Reads one record, optionally locking its row for the transaction.
pub(super) async fn get<T: Resource>(
    conn: &mut PgConnection,
    table: Table,
    keys: &[&str],
    for_update: bool,
) -> Result<T> {
    let sql = format!(
        "SELECT uid, version, proto FROM {} WHERE {}{}",
        table.name,
        table.key_match(),
        if for_update { " FOR UPDATE" } else { "" }
    );
    let row = bind_keys!(sqlx::query(AssertSqlSafe(sql)), keys)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db)?
        .ok_or(StoreError::NotFound)?;
    let uid: String = row.try_get("uid").map_err(db)?;
    let version: i64 = row.try_get("version").map_err(db)?;
    let proto: Vec<u8> = row.try_get("proto").map_err(db)?;
    decode_checked(&uid, version, &proto)
}

/// Inserts a new record built from `input`.
pub(super) async fn insert<T: Resource>(
    conn: &mut PgConnection,
    table: Table,
    keys: &[&str],
    input: &T,
) -> Result<T> {
    let msg = prepare_create(input)?;
    let n = table.keys.len();
    let sql = format!(
        "INSERT INTO {} ({}, uid, version, proto) VALUES ({}${}, ${}, ${})",
        table.name,
        table.keys.join(", "),
        (1..=n).map(|i| format!("${i}, ")).collect::<String>(),
        n + 1,
        n + 2,
        n + 3
    );
    let q = sqlx::query(AssertSqlSafe(sql));
    let q = if table.name == GLOBAL_ACCESS_POLICY.name {
        q.bind(true)
    } else {
        bind_keys!(q, keys)
    };
    q.bind(msg.uid().to_string())
        .bind(msg.version())
        .bind(encode(&msg))
        .execute(&mut *conn)
        .await
        .map_err(db)?;
    Ok(msg)
}

/// Overwrites a record. With `expect`, the write only lands if the row is
/// still at that uid and version, and [`StoreError::VersionConflict`]
/// reports that it was not.
pub(super) async fn write<T: Resource>(
    conn: &mut PgConnection,
    table: Table,
    keys: &[&str],
    msg: &T,
    expect: Option<(&str, i64)>,
) -> Result<()> {
    let n = table.bound_keys();
    let mut sql = format!(
        "UPDATE {} SET version = ${}, proto = ${} WHERE {}",
        table.name,
        n + 1,
        n + 2,
        table.key_match()
    );
    if expect.is_some() {
        sql.push_str(&format!(" AND uid = ${} AND version = ${}", n + 3, n + 4));
    }
    let mut q = bind_keys!(sqlx::query(AssertSqlSafe(sql)), keys.iter().take(n))
        .bind(msg.version())
        .bind(encode(msg));
    if let Some((uid, version)) = expect {
        q = q.bind(uid.to_string()).bind(version);
    }
    let done = q.execute(&mut *conn).await.map_err(db)?;
    match done.rows_affected() {
        1 => Ok(()),
        0 => Err(StoreError::VersionConflict),
        n => Err(StoreError::Corrupt(format!(
            "updating {} {} affected {n} rows",
            T::KIND,
            keys.join("/")
        ))),
    }
}

/// Optimistic read-modify-write: read, mutate, and write guarded on the
/// revision that was read.
pub(super) async fn update<T: Resource>(
    conn: &mut PgConnection,
    table: Table,
    keys: &[&str],
    guard: &DeletePreconditions,
    mutate: Option<crate::resource::Mutate<'_, T>>,
    mark_for_deletion: bool,
) -> Result<T> {
    let stored: T = get(conn, table, keys, false).await?;
    let next = prepare_update(&stored, guard, mutate, mark_for_deletion)?;
    write(
        conn,
        table,
        keys,
        &next,
        Some((stored.uid(), stored.version())),
    )
    .await?;
    Ok(next)
}

/// Read-modify-write under a row lock; `conn` must be in a transaction.
pub(super) async fn update_locked<T: Resource>(
    conn: &mut PgConnection,
    table: Table,
    keys: &[&str],
    guard: &DeletePreconditions,
    mutate: Option<crate::resource::Mutate<'_, T>>,
    mark_for_deletion: bool,
) -> Result<T> {
    let stored: T = get(conn, table, keys, true).await?;
    let next = prepare_update(&stored, guard, mutate, mark_for_deletion)?;
    write(conn, table, keys, &next, None).await?;
    Ok(next)
}

/// Deletes a record if it matches `guard`.
pub(super) async fn delete<T: Resource>(
    conn: &mut PgConnection,
    table: Table,
    keys: &[&str],
    guard: &DeletePreconditions,
) -> Result<T> {
    let n = table.keys.len();
    let sql = format!(
        "DELETE FROM {} WHERE {} AND (${}::text = '' OR uid = ${}::text) AND (${}::bigint = 0 OR version = ${}::bigint) \
         RETURNING uid, version, proto",
        table.name,
        table.key_match(),
        n + 1,
        n + 1,
        n + 2,
        n + 2
    );
    let row = bind_keys!(sqlx::query(AssertSqlSafe(sql)), keys)
        .bind(guard.uid.clone())
        .bind(guard.version)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db)?;
    match row {
        Some(row) => {
            let uid: String = row.try_get("uid").map_err(db)?;
            let version: i64 = row.try_get("version").map_err(db)?;
            let proto: Vec<u8> = row.try_get("proto").map_err(db)?;
            decode_checked(&uid, version, &proto)
        }
        None => {
            let sql = format!(
                "SELECT uid, version FROM {} WHERE {}",
                table.name,
                table.key_match()
            );
            let current = bind_keys!(sqlx::query(AssertSqlSafe(sql)), keys)
                .fetch_optional(&mut *conn)
                .await
                .map_err(db)?
                .map(|r| -> Result<(String, i64)> {
                    Ok((r.try_get(0).map_err(db)?, r.try_get(1).map_err(db)?))
                })
                .transpose()?;
            Err(delete_miss(current, guard))
        }
    }
}

/// Lists a table in key order. Two-column tables can be scoped to one hive.
pub(super) async fn list<T: Resource>(
    conn: &mut PgConnection,
    table: Table,
    hive: &str,
    opts: &ListOptions,
    kind: ListKind,
) -> Result<ListResponse<T>> {
    let opts = opts.normalize()?;
    let limit = i64::from(opts.page_size) + 1;
    let (sql, scope, parts) = match (table.keys, hive.is_empty()) {
        ([k], _) => (
            format!("SELECT {k}, proto FROM {t} WHERE $1::text IS NULL OR {k} > $1 ORDER BY {k} LIMIT $2", t = table.name),
            "",
            1,
        ),
        ([k1, k2], false) => (
            format!(
                "SELECT {k2}, proto FROM {t} WHERE {k1} = $3 AND ($1::text IS NULL OR {k2} > $1) ORDER BY {k2} LIMIT $2",
                t = table.name
            ),
            hive,
            1,
        ),
        ([k1, k2], true) => (
            format!(
                "SELECT {k1}, {k2}, proto FROM {t} WHERE $1::text IS NULL OR ({k1}, {k2}) > ($1, $3) \
                 ORDER BY {k1}, {k2} LIMIT $2",
                t = table.name
            ),
            "",
            2,
        ),
        _ => unreachable!("resource tables have one or two key columns"),
    };
    let after = page_token::decode(&opts.page_token, kind, scope, parts)?;
    let first = after.as_ref().map(|a| a[0].clone());
    let q = sqlx::query(AssertSqlSafe(sql)).bind(first).bind(limit);
    let q = match (parts, hive.is_empty()) {
        (1, false) => q.bind(hive.to_string()),
        (2, _) => q.bind(after.as_ref().map(|a| a[1].clone())),
        _ => q,
    };
    let rows = q.fetch_all(&mut *conn).await.map_err(db)?;
    page(rows, parts, opts.page_size as usize, kind, scope, hive)
}

/// Adds the row's key to a decode failure.
fn name_row<T: Resource>(err: StoreError, prefix: &str, key: &[String]) -> StoreError {
    let StoreError::Corrupt(why) = err else {
        return err;
    };
    let mut id = key.join("/");
    if !prefix.is_empty() {
        id = format!("{prefix}/{id}");
    }
    StoreError::Corrupt(format!("{} {id}: {why}", T::KIND))
}

/// Turns `limit + 1` fetched rows of `(<key parts>, proto)` into a page.
///
/// One undecodable row fails the whole listing, so its error names the row:
/// `prefix` (the hive of a hive-scoped listing, else empty) followed by the
/// row's key parts.
pub(super) fn page<T: Resource>(
    rows: Vec<sqlx::postgres::PgRow>,
    parts: usize,
    size: usize,
    kind: ListKind,
    scope: &str,
    prefix: &str,
) -> Result<ListResponse<T>> {
    let more = rows.len() > size;
    let mut items = Vec::with_capacity(rows.len().min(size));
    let mut last = Vec::new();
    for row in rows.into_iter().take(size) {
        last = (0..parts)
            .map(|i| row.try_get::<String, _>(i).map_err(db))
            .collect::<Result<Vec<_>>>()?;
        let proto: Vec<u8> = row.try_get(parts).map_err(db)?;
        items.push(decode(&proto).map_err(|e| name_row::<T>(e, prefix, &last))?);
    }
    let next_page_token = if more {
        page_token::encode(kind, scope, last)
    } else {
        String::new()
    };
    Ok(ListResponse {
        items,
        next_page_token,
    })
}
