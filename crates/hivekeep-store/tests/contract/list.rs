use std::sync::Arc;

use hivekeep_store::*;

type S = Arc<dyn Store>;

async fn every_listing(s: &S, opts: ListOptions) -> Vec<(&'static str, Result<()>)> {
    vec![
        ("hives", s.list_hives(&opts).await.map(drop)),
        ("actors", s.list_actors("", &opts).await.map(drop)),
        (
            "actor templates",
            s.list_actor_templates("", &opts).await.map(drop),
        ),
        ("tags", s.list_tags("", &opts).await.map(drop)),
        ("workers", s.list_workers(&opts).await.map(drop)),
        (
            "assignments",
            s.list_worker_assignments("w", &opts).await.map(drop),
        ),
    ]
}

pub async fn invalid_page_size(s: S) {
    for (name, r) in every_listing(&s, ListOptions::new(-1, "")).await {
        assert!(
            matches!(r, Err(StoreError::InvalidPageSize)),
            "{name}: {r:?}"
        );
    }
    for (name, r) in every_listing(&s, ListOptions::default()).await {
        assert!(r.is_ok(), "{name}: {r:?}");
    }
}

pub async fn invalid_page_token(s: S) {
    for (name, r) in every_listing(&s, ListOptions::new(1, "%%%")).await {
        assert!(
            matches!(r, Err(StoreError::InvalidPageToken(_))),
            "{name}: {r:?}"
        );
    }
}
