# Security policy

## Reporting a vulnerability

Please report suspected vulnerabilities privately by email to
**SECURITY_CONTACT_PLACEHOLDER**. Do not open a public issue, pull request
or discussion for a security problem.

Include as much of the following as you can:

- the affected component (for example `hivekeep-api`, `hivekeep-keeper`,
  `hivekeep-gate`, a sandbox worker) and the version or commit;
- the sandbox class (`gvisor` or `microvm`) and the gate dataplane, if
  relevant;
- steps to reproduce, or a proof of concept;
- the impact as you understand it (for example, escape from a sandbox,
  access across Hives, credential disclosure, or bypass of an egress
  policy).

We will acknowledge your report, keep you updated while we investigate, and
credit you in the advisory if you wish.

## Scope

Hivekeep is pre-1.0. Fixes go into the main branch, and no older releases
are supported. Our threat model is in
[docs/threat-model.md](docs/threat-model.md), and the security design is in
[docs/security.md](docs/security.md). The demo workloads under `demos/` and
`crates/hivekeep-demo-*` do not authenticate their callers on purpose, so
missing authentication in a demo is not a vulnerability.

Weaknesses in gVisor, Cloud Hypervisor, the Kata guest agent, Envoy or
Kubernetes should go to those projects. Please tell us as well if Hivekeep's
use of them makes the problem worse.
