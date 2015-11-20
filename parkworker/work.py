# coding: utf-8
from parkworker.const import TASK_TYPE_WORK
from parkworker.task_processor import TaskProcessor


class Work(TaskProcessor):
    task_type = TASK_TYPE_WORK

    def work(self, **kwargs):
        raise NotImplemented()

    @classmethod
    def get_work(cls, name):
        works = cls.get_all_works()
        for work_name, work_class in works:
            if name == work_name:
                return work_class

    @classmethod
    def get_all_works(cls):
        if not hasattr(cls, '_works'):
            cls._works = cls._get_classes(
                package_name='works',
                base_class=Work,
            )
        return cls._works


