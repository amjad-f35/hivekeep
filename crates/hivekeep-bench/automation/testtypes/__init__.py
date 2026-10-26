"""The test types tests.yaml can name; the orchestrator dispatches on TYPES.

Every type module provides these hooks, called in this order:

  validate(test)          before any cluster work; raises ValueError on a
                          malformed entry.
  build_image(commit)     builds and pushes the runner image; called at most
                          once per (target cluster, type).
  pre_test(test)          after Hivekeep is installed and before the
                          workloads are deployed: the type's chance to shape
                          the cluster.
  job_tmpl(manifests_dir) path of the type's runner Job template.
  job_subs(test)          the type's ${...} substitutions for that template.

Submitting the Job, waiting, collecting logs and teardown are the same for
every type and live in the orchestrator. A new type is one module here plus
one entry in TYPES.
"""

from typing import Any, Protocol

from testtypes import locust
from testtypes import nighthawk_ingress


class TestType(Protocol):
    TEST_TYPE: str

    def validate(self, test: dict[str, Any]) -> None: ...
    def build_image(self, commit: str) -> str: ...
    def pre_test(self, test: dict[str, Any]) -> None: ...
    def job_tmpl(self, manifests_dir: str) -> str: ...
    def job_subs(self, test: dict[str, Any]) -> dict[str, Any]: ...


TYPES: dict[str, TestType] = {
    locust.TEST_TYPE: locust,
    nighthawk_ingress.TEST_TYPE: nighthawk_ingress,
}

HOOKS = ("validate", "build_image", "pre_test", "job_tmpl", "job_subs")

# Nothing type-checks these modules, so hold them to the Protocol at import
# time: every entry point then fails at once instead of mid-benchmark.
for _name, _mod in TYPES.items():
    for _hook in HOOKS:
        assert hasattr(_mod, _hook), f"test type {_name!r} missing hook {_hook!r}"
