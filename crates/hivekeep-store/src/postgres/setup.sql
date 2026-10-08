-- One-time database bootstrap, run by an administrator through psql before
-- the API server starts. It creates the owner role (which owns the schema and
-- runs migrations) and the read/write role (which serves requests), a login
-- user for each, and the schema itself. It is idempotent and refuses to adopt
-- roles or a schema with conflicting attributes.
--
-- psql variables: hivekeep_schema, hivekeep_owner_role, hivekeep_owner_user,
-- hivekeep_owner_password, hivekeep_readwrite_role, hivekeep_readwrite_user,
-- hivekeep_readwrite_password. An empty password leaves login disabled
-- (password NULL), for deployments that authenticate by certificate.

SELECT
    set_config('hivekeep.setup_schema', :'hivekeep_schema', true) AS schema,
    set_config('hivekeep.setup_owner_role', :'hivekeep_owner_role', true) AS owner_role,
    set_config('hivekeep.setup_owner_user', :'hivekeep_owner_user', true) AS owner_user,
    set_config('hivekeep.setup_owner_password', :'hivekeep_owner_password', true) AS owner_password,
    set_config('hivekeep.setup_readwrite_role', :'hivekeep_readwrite_role', true) AS readwrite_role,
    set_config('hivekeep.setup_readwrite_user', :'hivekeep_readwrite_user', true) AS readwrite_user,
    set_config('hivekeep.setup_readwrite_password', :'hivekeep_readwrite_password', true) AS readwrite_password
\gset

DO $setup$
DECLARE
    schema_name text := current_setting('hivekeep.setup_schema');
    owner_role text := current_setting('hivekeep.setup_owner_role');
    owner_user text := current_setting('hivekeep.setup_owner_user');
    owner_password text := NULLIF(current_setting('hivekeep.setup_owner_password'), '');
    readwrite_role text := current_setting('hivekeep.setup_readwrite_role');
    readwrite_user text := current_setting('hivekeep.setup_readwrite_user');
    readwrite_password text := NULLIF(current_setting('hivekeep.setup_readwrite_password'), '');
    managed record;
    role_attrs record;
    schema_owner text;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('hivekeep:setup:' || schema_name, 0));

    FOR managed IN SELECT * FROM (VALUES
        (owner_role, false, NULL::text),
        (readwrite_role, false, NULL::text),
        (owner_user, true, owner_password),
        (readwrite_user, true, readwrite_password)
    ) AS roles(name, can_login, password) LOOP
        SELECT rolcanlogin, rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolinherit
          INTO role_attrs FROM pg_roles WHERE rolname = managed.name;
        IF NOT FOUND THEN
            IF managed.can_login THEN
                EXECUTE format('CREATE ROLE %I LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION', managed.name);
            ELSE
                EXECUTE format('CREATE ROLE %I NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION', managed.name);
            END IF;
        ELSIF role_attrs.rolcanlogin <> managed.can_login OR role_attrs.rolsuper
           OR role_attrs.rolcreatedb OR role_attrs.rolcreaterole OR role_attrs.rolreplication
           OR (managed.can_login AND role_attrs.rolinherit) THEN
            RAISE EXCEPTION 'managed PostgreSQL role "%" conflicts with the required attributes', managed.name;
        END IF;
        IF managed.can_login THEN
            EXECUTE format('ALTER ROLE %I PASSWORD %L', managed.name, managed.password);
        END IF;
    END LOOP;

    EXECUTE format('GRANT %I TO %I', owner_role, owner_user);
    EXECUTE format('GRANT %I TO %I', readwrite_role, readwrite_user);

    SELECT pg_get_userbyid(nspowner) INTO schema_owner FROM pg_namespace WHERE nspname = schema_name;
    IF NOT FOUND THEN
        EXECUTE format('CREATE SCHEMA %I AUTHORIZATION %I', schema_name, owner_role);
    ELSIF schema_owner <> owner_role THEN
        RAISE EXCEPTION 'PostgreSQL schema "%" is owned by "%", not "%"', schema_name, schema_owner, owner_role;
    END IF;

    REVOKE CREATE ON SCHEMA public FROM PUBLIC;
    EXECUTE format('REVOKE ALL ON SCHEMA %I FROM PUBLIC', schema_name);
    EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I', schema_name, readwrite_role);
    EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA %I GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %I', owner_role, schema_name, readwrite_role);
    EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA %I GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO %I', owner_role, schema_name, readwrite_role);
    EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA %I GRANT EXECUTE ON ROUTINES TO %I', owner_role, schema_name, readwrite_role);
    EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA %I GRANT USAGE ON TYPES TO %I', owner_role, schema_name, readwrite_role);
END
$setup$;
