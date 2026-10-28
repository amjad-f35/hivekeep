//! Per-worker-thread interning of policies by hash.
//!
//! Envoy runs each connection's filters on one worker thread, and the inner
//! listener of a tunnel runs on the same thread as its CONNECT stream, so a
//! thread-local table needs no locking. Entries are reference counted: the
//! table holds one reference and every CONNECT stream using the policy holds
//! another. The CONNECT stream outlives the tunnelled connection inside it,
//! so the inner listener always finds the entry its hash names. When the
//! last stream releases a policy, the entry goes too.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::policy::EgressPolicy;

/// The policies interned on the current thread.
#[derive(Debug, Default)]
pub struct PolicyStore {
    policies: HashMap<u64, Rc<EgressPolicy>>,
}

/// Result of [`PolicyStore::admit`].
#[derive(Debug, PartialEq, Eq)]
pub enum Admission {
    /// First stream with this policy; it is now interned.
    Stored(Rc<EgressPolicy>),
    /// An identical policy was already interned; this is that copy.
    Shared(Rc<EgressPolicy>),
    /// A different policy has the same hash. The caller must fail the
    /// stream: handing out the hash would apply the wrong rules.
    Collision,
}

thread_local! {
    static LOCAL: RefCell<PolicyStore> = RefCell::new(PolicyStore::default());
}

impl PolicyStore {
    /// Runs `f` with the current thread's store.
    pub fn with_local<R>(f: impl FnOnce(&mut PolicyStore) -> R) -> R {
        LOCAL.with(|s| f(&mut s.borrow_mut()))
    }

    /// Interns `policy` for a new CONNECT stream.
    pub fn admit(&mut self, policy: Rc<EgressPolicy>) -> Admission {
        match self.policies.get(&policy.hash) {
            Some(existing) if **existing == *policy => Admission::Shared(Rc::clone(existing)),
            Some(_) => Admission::Collision,
            None => {
                self.policies.insert(policy.hash, Rc::clone(&policy));
                Admission::Stored(policy)
            }
        }
    }

    /// Called as a stream holding `policy` ends, before it drops its
    /// reference. Removes the entry when that stream was the last user
    /// (only the table and the caller hold it) and reports whether it did.
    pub fn release(&mut self, policy: &Rc<EgressPolicy>) -> bool {
        let interned = self.policies.get(&policy.hash).is_some_and(|p| Rc::ptr_eq(p, policy));
        if interned && Rc::strong_count(policy) == 2 {
            self.policies.remove(&policy.hash);
            return true;
        }
        false
    }

    /// The interned policy with this hash.
    pub fn lookup(&self, hash: u64) -> Option<Rc<EgressPolicy>> {
        self.policies.get(&hash).cloned()
    }

    /// Number of interned policies.
    pub fn len(&self) -> usize {
        self.policies.len()
    }

    /// Whether nothing is interned.
    pub fn is_empty(&self) -> bool {
        self.policies.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::{SniMode, SniRule};

    fn policy(pattern: &str) -> Rc<EgressPolicy> {
        Rc::new(EgressPolicy::new(vec![SniRule { pattern: pattern.into(), mode: SniMode::Mitm }]))
    }

    #[test]
    fn identical_policies_share_one_entry_until_the_last_release() {
        let mut s = PolicyStore::default();
        let Admission::Stored(first) = s.admit(policy("api.example.com")) else { panic!("not stored") };
        let Admission::Shared(second) = s.admit(policy("api.example.com")) else { panic!("not shared") };
        assert!(Rc::ptr_eq(&first, &second));
        assert_eq!(s.len(), 1);

        // Another stream still holds it.
        assert!(!s.release(&first));
        drop(first);
        assert!(s.lookup(second.hash).is_some());

        assert!(s.release(&second));
        assert!(s.lookup(second.hash).is_none());
        assert!(s.is_empty());
    }

    #[test]
    fn a_hash_collision_is_refused_and_leaves_the_entry() {
        let mut s = PolicyStore::default();
        let Admission::Stored(held) = s.admit(policy("a.test")) else { panic!() };
        let mut other = EgressPolicy::new(vec![SniRule { pattern: "b.test".into(), mode: SniMode::Passthrough }]);
        other.hash = held.hash;
        assert_eq!(s.admit(Rc::new(other)), Admission::Collision);
        assert!(Rc::ptr_eq(&s.lookup(held.hash).unwrap(), &held));
    }

    #[test]
    fn releasing_an_unadmitted_policy_is_a_no_op() {
        let mut s = PolicyStore::default();
        let Admission::Stored(_held) = s.admit(policy("a.test")) else { panic!() };
        // Same hash, different allocation: e.g. the loser of a collision.
        let stray = policy("a.test");
        assert!(!s.release(&stray));
        assert_eq!(s.len(), 1);
        assert!(!s.release(&Rc::new(EgressPolicy::default())));
    }

    #[test]
    fn the_thread_local_store_is_per_thread() {
        let p = policy("thread.test");
        let hash = p.hash;
        PolicyStore::with_local(|s| assert!(matches!(s.admit(p), Admission::Stored(_))));
        assert!(PolicyStore::with_local(|s| s.lookup(hash)).is_some());
        std::thread::spawn(move || assert!(PolicyStore::with_local(|s| s.lookup(hash)).is_none()))
            .join()
            .unwrap();
    }
}
