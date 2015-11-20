# coding: utf-8
from parkworker.work import Work
from parkworker.task_processor import TaskResult


class AsyncWork(Work):

    def work(self, **kwargs):
        raise Exception('Use async_work() method instead.')

    async def async_work(self, **kwargs) -> TaskResult:
        raise NotImplemented()
