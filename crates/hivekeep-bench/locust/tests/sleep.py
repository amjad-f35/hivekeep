"""SleepUser: suspends and resumes actors of the `sleep` template, whose
container runs an idle process."""

from common.template_cycle import TemplateCycleUser


class SleepUser(TemplateCycleUser):
    template_name = "sleep"
