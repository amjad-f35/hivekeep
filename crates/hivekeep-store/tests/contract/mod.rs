//! Backend-neutral contract of [`hivekeep_store::Store`]. Every test takes
//! a fresh, empty store; [`contract_tests!`] instantiates the whole suite for
//! one backend.

#![allow(dead_code)]

pub mod actor;
pub mod assignment;
pub mod fixtures;
pub mod hive;
pub mod lease;
pub mod list;
pub mod mark;
pub mod policy;
pub mod tag;
pub mod template;
pub mod worker;

/// A fresh store plus the future that cleans up after it.
pub type Fresh = (
    std::sync::Arc<dyn hivekeep_store::Store>,
    std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
);

/// Instantiates every contract test for the backend built by `$setup`, an
/// `async fn() -> Fresh`, putting `#[$attr]` on each test.
#[macro_export]
macro_rules! contract_tests {
    ($setup:path, $attr:meta) => {
        $crate::contract_tests!(@go $setup, $attr, {
            actor {
                get_not_found create create_already_exists malformed_snapshot_uri update update_conflict
                update_uid_conflict update_mutate_error update_missing_precondition mark_for_deletion
                delete_in_any_state delete_not_found delete_preconditions list_empty list list_pagination
                list_scoped_by_hive
            }
            policy {
                egress_lifecycle egress_delete_preconditions egress_follows_actor global_access_lifecycle
                hive_access_lifecycle
            }
            worker {
                get_not_found create create_already_exists update update_not_found update_missing_precondition
                mark_for_deletion mark_for_deletion_event update_uid_conflict update_conflict update_mutate_error
                concurrent_assign delete delete_not_found delete_preconditions watch_closed_on_close list
                list_empty list_pagination epochs_advance
            }
            assignment {
                bind_adds_assignment_and_allocation bind_replaces_same_actor rebind_keeps_identity
                refused_admission_leaves_nothing admits_replacement_that_grew admit_sees_concurrent_binds
                actor_already_on_another_worker concurrent_claims_of_one_actor bind_worker_not_found release
                release_already_released list_scoped_to_one_worker list_paginates list_empty
                find_worker_hosting_actor delete_worker_drops_assignments allocated_tracks_churn
            }
            hive {
                list_pagination create create_already_exists get_not_found list list_empty delete_empty
                delete_not_found delete_preconditions delete_non_empty_rejected delete_after_actors_removed
                delete_while_other_non_empty
            }
            template {
                lifecycle delete_preconditions update_missing_precondition mark_for_deletion block_hive_deletion
                rename_is_immutable list_scoped
            }
            tag {
                lifecycle delete_preconditions immutable_fields record_snapshot_after_reserve reused_name
                unknown_hive malformed_snapshot_uri update_missing_precondition mark_for_deletion
                list_pagination_and_scope
            }
            lease {
                acquire conflict non_reentrant close_releases close_idempotent
            }
            list {
                invalid_page_size invalid_page_token
            }
        });
    };
    (@go $setup:path, $attr:meta, { $($module:ident { $($test:ident)* })* }) => {
        $(
            mod $module {
                $(
                    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
                    #[$attr]
                    async fn $test() {
                        let (store, cleanup) = $setup().await;
                        $crate::contract::$module::$test(store).await;
                        cleanup.await;
                    }
                )*
            }
        )*
    };
}
