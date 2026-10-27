"""UserMemUser: suspends and resumes actors of the `usermem` template, whose
container runs a process holding user-space memory."""

from common.template_cycle import TemplateCycleUser


class UserMemUser(TemplateCycleUser):
    template_name = "usermem"
