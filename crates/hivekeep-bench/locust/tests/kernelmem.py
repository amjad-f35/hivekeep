"""KernelMemUser: suspends and resumes actors of the `kernelmem` template, whose
container runs a process holding kernel memory (page cache, sockets)."""

from common.template_cycle import TemplateCycleUser


class KernelMemUser(TemplateCycleUser):
    template_name = "kernelmem"
