# hivekeep-demo-fspersist

The `fspersist` demo workload. It keeps a capped history file on the root
filesystem that follows the Actor across workers. It shares a WorkerPool
with the counter in the multi-template demo. Walkthrough:
[demos/multi-template](../../demos/multi-template/README.md).
