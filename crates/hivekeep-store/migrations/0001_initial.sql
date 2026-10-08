-- Hivekeep control-plane schema.
--
-- Every resource is stored as its encoded protobuf message (`proto`), with
-- the key columns and the optimistic-concurrency columns (`uid`, `version`)
-- projected out so writes can be guarded in SQL. Foreign keys carry the
-- parent/child rules: children need their parent, a Hive cannot be dropped
-- while it holds Actors, ActorTemplates or Tags, and sub-resources go with
-- their owner.

CREATE TABLE hives (
    name     text PRIMARY KEY,
    uid      text NOT NULL,
    version  bigint NOT NULL,
    proto    bytea NOT NULL
);

CREATE TABLE actors (
    hive     text NOT NULL
        REFERENCES hives(name) ON DELETE RESTRICT,
    name     text NOT NULL,
    uid      text NOT NULL,
    version  bigint NOT NULL,
    proto    bytea NOT NULL,
    PRIMARY KEY (hive, name)
);

CREATE TABLE actor_egress_policies (
    hive        text NOT NULL,
    actor_name  text NOT NULL,
    uid         text NOT NULL,
    version     bigint NOT NULL,
    proto       bytea NOT NULL,
    PRIMARY KEY (hive, actor_name),
    FOREIGN KEY (hive, actor_name)
        REFERENCES actors(hive, name) ON DELETE CASCADE
);

CREATE TABLE actor_templates (
    hive     text NOT NULL
        REFERENCES hives(name) ON DELETE RESTRICT,
    name     text NOT NULL,
    uid      text NOT NULL,
    version  bigint NOT NULL,
    proto    bytea NOT NULL,
    PRIMARY KEY (hive, name)
);

CREATE TABLE tags (
    hive     text NOT NULL,
    name     text NOT NULL,
    uid      text NOT NULL,
    version  bigint NOT NULL,
    proto    bytea NOT NULL,
    PRIMARY KEY (hive, name),
    CONSTRAINT tags_hive_fk
        FOREIGN KEY (hive) REFERENCES hives(name) ON DELETE RESTRICT
);

-- Workers are global and named by an opaque, control-plane-assigned name.
CREATE TABLE workers (
    name     text PRIMARY KEY,
    uid      text NOT NULL UNIQUE,
    version  bigint NOT NULL,
    proto    bytea NOT NULL
);

-- One row per hosted Actor, keyed by Actor uid since an Actor has at most one
-- Worker. Kept apart from workers so Worker reads, writes and watch events do
-- not grow with occupancy.
CREATE TABLE worker_assignments (
    actor_uid    text PRIMARY KEY,
    worker_name  text NOT NULL
        REFERENCES workers(name) ON DELETE CASCADE,
    proto        bytea NOT NULL
);

CREATE INDEX worker_assignments_worker_idx ON worker_assignments (worker_name);

-- Transactional outbox feeding worker watches. Each Worker write appends
-- exactly one row in its own transaction, so rows are ordered by the writing
-- transaction's id and a poller can read everything below the oldest
-- still-running transaction without missing a late commit.
--
-- created_at uses clock_timestamp(): now() is frozen at transaction start.
CREATE TABLE worker_events (
    xid         xid8 NOT NULL DEFAULT pg_current_xact_id(),
    created_at  timestamptz NOT NULL DEFAULT clock_timestamp(),
    payload     bytea NOT NULL
);

CREATE INDEX worker_events_xid_idx ON worker_events (xid);
CREATE INDEX worker_events_created_at_idx ON worker_events (created_at);

-- High-water mark of retention: the greatest xid ever discarded from
-- worker_events. A watch whose cursor is below it has missed events and must
-- resynchronize.
CREATE TABLE worker_events_trim (
    id   boolean PRIMARY KEY DEFAULT true CHECK (id),
    xid  xid8 NOT NULL
);

CREATE TABLE leases (
    key         text PRIMARY KEY,
    token       text NOT NULL,
    expires_at  timestamptz NOT NULL
);

CREATE INDEX leases_expires_at_idx ON leases (expires_at);

-- Singleton: the id column only admits true.
CREATE TABLE global_access_policy (
    id       boolean PRIMARY KEY DEFAULT true CHECK (id),
    uid      text NOT NULL,
    version  bigint NOT NULL,
    proto    bytea NOT NULL
);

CREATE TABLE hive_access_policies (
    hive     text PRIMARY KEY
        REFERENCES hives(name) ON DELETE CASCADE,
    uid      text NOT NULL,
    version  bigint NOT NULL,
    proto    bytea NOT NULL
);
