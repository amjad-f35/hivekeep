# Hivekeep documentation

## Concepts

- [Architecture](architecture.md): components, resume and suspend flows,
  data model, snapshot fidelities, scheduling, epochs
- [Glossary](glossary.md)
- [Resource model](resource-model.md): how Actors are sized and packed

## Using Hivekeep

- [API guide](api-guide.md): Control and WorkerService RPCs, and `hivectl`
- [Egress](egress.md): policies, credential injection, HTTPS interception
  and the trust bundle
- [Request parking](request-parking.md)
- [Networking](networking.md): ingress, the sandbox tunnel, DNS, network
  namespaces, ports

## Operating Hivekeep

- [Installing](operator-install.md) with `hivekeep-setup`
- [Configuration](configuration.md), plus the production checklist in
  [`PLACEHOLDERS.md`](../PLACEHOLDERS.md)
- [Storage](storage.md): snapshots, object stores, caches, node layout, CSI
  volumes
- [PostgreSQL](postgres.md)
- [Observability](observability.md): metrics, logs, events, tracing
- [Upgrading](upgrade.md) and the [version skew policy](version-skew-policy.md)

## Security

- [Security design](security.md)
- [Authentication](authentication.md)
- [Authorization](authorization.md)
- [Threat model](threat-model.md) (machine-readable copy:
  [threats.json](threats.json))

## Contributing

- [Development](development.md): layout, conventions, tests, Makefile
  targets
- [CONTRIBUTING.md](../CONTRIBUTING.md) and [SECURITY.md](../SECURITY.md)
