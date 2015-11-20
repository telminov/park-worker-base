# coding: utf-8
from parkworker.monit import Monit
from parkworker.task_processor import TaskResult


class AsyncMonit(Monit):

    def check(self, host: str, **kwargs):
        raise Exception('Use async_monit() method instead.')

    async def async_check(self, host: str, **kwargs) -> TaskResult:
        raise NotImplemented()
