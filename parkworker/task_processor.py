# coding: utf-8
import sys

import os
import traceback

from parkworker.const import LEVEL_FAIL, TASK_TYPE_MONIT, TASK_TYPE_WORK
from parkworker.utils import now


class DuplicatedNameException(Exception):
    pass


class UnknownTaskTypeException(Exception):
    pass


class UnknownTaskNameException(Exception):
    pass


class TaskResult(object):
    def __init__(self, level, dt=None, extra=None):
        self.level = level
        self.extra = extra
        self.dt = dt or now()

    def get_dict(self):
        return {
            'level': self.level,
            'extra': self.extra,
            'dt': self.dt,
        }


class TaskProcessor(object):
    name = None
    description = None
    task_type = None

    @classmethod
    def process(cls, task_name, task_type, **kwargs):
        task_processor = cls.get_task_processor(task_name, task_type)
        try:
            if task_processor.task_type == TASK_TYPE_MONIT:
                result = task_processor.check(**kwargs)
            else:
                result = task_processor.work(**kwargs)
        except Exception as ex:
            result = cls._get_exp_result(ex)
        return result

    @staticmethod
    def get_task_processor(task_name, task_type):
        from parkworker.work import Work
        from parkworker.monit import Monit

        if task_type == TASK_TYPE_MONIT:
            task_processor_class = Monit.get_monit(task_name)
        elif task_type == TASK_TYPE_WORK:
            task_processor_class = Work.get_work(task_name)
        else:
            raise UnknownTaskTypeException('Unknown task type "%s"' % task_type)

        if not task_processor_class:
            raise UnknownTaskNameException('Unknown task name "%s" for task type "%s"' % (task_name, task_type))

        task_processor = task_processor_class()
        return task_processor

    @staticmethod
    def _get_exp_result(ex):
        result = TaskResult(
            level=LEVEL_FAIL,
            extra={
                'description': str(ex),
                'exception_type': str(type(ex)),
                'stack_trace':  traceback.format_exc(),
            }
        )
        return result

    @staticmethod
    def _get_classes(package_name, base_class):
        """
        search monits or works classes. Class must have 'name' attribute
        :param package_name: 'monits' or 'works'
        :param base_class: Monit or Work
        :return: tuple of tuples monit/work-name and class
        """
        classes = {}

        base_dir = os.getcwd()
        root_module_name = base_dir.split('/')[-1]
        package_dir = base_dir + '/%s' % package_name
        if os.path.isdir(package_dir):
            for module_path in os.listdir(package_dir):
                if not module_path.endswith('.py'):
                    continue

                module_name = os.path.splitext(module_path)[0]
                module_full_name = '%s.%s.%s' % (root_module_name, package_name, module_name)
                __import__(module_full_name)
                work_module = sys.modules[module_full_name]
                for module_item in work_module.__dict__.values():
                    if type(module_item) is type \
                            and issubclass(module_item, base_class) \
                            and module_item is not base_class\
                            and hasattr(module_item, 'name') and module_item.name:
                        classes.setdefault(module_item.name, []).append(module_item)

        # check no duplicated names
        for work_name, work_modules in classes.items():
            if len(work_modules) > 1:
                raise DuplicatedNameException('Modules %s have same name "%s"' % (
                    ' and '.join(map(str, work_modules)),
                    work_name
                ))

        # create immutable list of modules
        return tuple([(work_name, work_modules[0]) for work_name, work_modules in classes.items()])

