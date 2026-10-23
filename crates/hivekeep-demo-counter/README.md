# hivekeep-demo-counter

The `counter` demo workload. Every request bumps three counters, kept in
process memory, in a file on a durable volume, and in a file on the root
filesystem. Comparing them across a suspend and resume shows what each
snapshot fidelity keeps. Walkthrough: [demos/counter](../../demos/counter/README.md).
