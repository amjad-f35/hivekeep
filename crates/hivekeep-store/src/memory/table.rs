//! Generic operations on one in-memory table.

use std::collections::BTreeMap;

use crate::page_token::{self, ListKind};
use crate::resource::{prepare_create, prepare_update, Resource};
use crate::{DeletePreconditions, ListOptions, ListResponse, Result, StoreError};

/// The record as callers see it: defaults filled in.
pub(super) fn read<T: Resource>(msg: &T) -> T {
    let mut out = msg.clone();
    out.backfill();
    out
}

pub(super) fn get<K: Ord, T: Resource>(table: &BTreeMap<K, T>, key: &K) -> Result<T> {
    table.get(key).map(read).ok_or(StoreError::NotFound)
}

pub(super) fn insert<K: Ord, T: Resource>(
    table: &mut BTreeMap<K, T>,
    key: K,
    input: &T,
) -> Result<T> {
    if table.contains_key(&key) {
        return Err(StoreError::AlreadyExists);
    }
    let msg = prepare_create(input)?;
    table.insert(key, msg.clone());
    Ok(msg)
}

pub(super) fn update<K: Ord + Clone, T: Resource>(
    table: &mut BTreeMap<K, T>,
    key: &K,
    guard: &DeletePreconditions,
    mutate: Option<crate::resource::Mutate<'_, T>>,
    mark_for_deletion: bool,
) -> Result<T> {
    let stored = get(table, key)?;
    let next = prepare_update(&stored, guard, mutate, mark_for_deletion)?;
    table.insert(key.clone(), next.clone());
    Ok(next)
}

pub(super) fn remove<K: Ord, T: Resource>(
    table: &mut BTreeMap<K, T>,
    key: &K,
    guard: &DeletePreconditions,
) -> Result<T> {
    let stored = table.get(key).ok_or(StoreError::NotFound)?;
    guard.check(stored.metadata())?;
    let removed = table.remove(key).expect("checked above");
    Ok(read(&removed))
}

/// Serves one page of `rows`, which must already be in key order.
pub(super) fn paginate<T>(
    rows: impl Iterator<Item = (Vec<String>, T)>,
    opts: &ListOptions,
    kind: ListKind,
    scope: &str,
    key_parts: usize,
) -> Result<ListResponse<T>> {
    let opts = opts.normalize()?;
    let after = page_token::decode(&opts.page_token, kind, scope, key_parts)?;
    let size = opts.page_size as usize;
    let mut items = Vec::new();
    let mut last = None;
    let mut more = false;
    for (k, item) in rows {
        if after.as_ref().is_some_and(|a| &k <= a) {
            continue;
        }
        if items.len() == size {
            more = true;
            break;
        }
        last = Some(k);
        items.push(item);
    }
    let next_page_token = match (more, last) {
        (true, Some(last)) => page_token::encode(kind, scope, last),
        _ => String::new(),
    };
    Ok(ListResponse {
        items,
        next_page_token,
    })
}
