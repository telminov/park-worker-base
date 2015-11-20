# coding: utf-8
from parkworker.const import TASK_TYPE_MONIT
from parkworker.task_processor import TaskProcessor


class Monit(TaskProcessor):
    task_type = TASK_TYPE_MONIT

    def check(self, host, **kwargs):
        raise NotImplemented()

    @classmethod
    def get_monit(cls, name):
        monits = cls.get_all_monits()
        for monit_name, monit_class in monits:
            if name == monit_name:
                return monit_class

    @classmethod
    def get_all_monits(cls):
        if not hasattr(cls, '_monits'):
            cls._monits = cls._get_classes(
                package_name='monits',
                base_class=Monit,
            )

        return cls._monits

