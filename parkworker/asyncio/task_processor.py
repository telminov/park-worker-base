# coding: utf-8
from parkworker.const import TASK_TYPE_MONIT
from parkworker.task_processor import TaskProcessor


class AsyncTaskProcessor(TaskProcessor):

    @classmethod
    def process(cls, task_name, task_type, **kwargs):
        raise Exception('Use async_process() method instead.')

    @classmethod
    async def async_process(cls, task_name, task_type, **kwargs):
        task_processor = cls.get_task_processor(task_name, task_type)
        try:
            if task_processor.task_type == TASK_TYPE_MONIT:
                result = await task_processor.async_check(**kwargs)
            else:
                result = await task_processor.async_work(**kwargs)
        except Exception as ex:
            result = cls._get_exp_result(ex)
        return result
