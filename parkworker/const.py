# coding: utf-8

WORK_STATUS_EVENT = b'WORK_STATUS_EVENT'
MONIT_STATUS_EVENT = b'MONIT_STATUS_EVENT'
TASK_EVENT = b'TASK_EVENT'
WORKER_EVENT = b'WORKER_EVENT'

WORKER_HEART_BEAT_PERIOD = 5

LEVEL_OK = 1
LEVEL_WARNING = 2
LEVEL_FAIL = 3
LEVEL_CHOICES = (
    (LEVEL_OK, 'ok'),
    (LEVEL_WARNING, 'warning'),
    (LEVEL_FAIL, 'fail'),
)

FRESH_TASK_CANCEL_REASON = 'Similar fresh task was created.'
TASK_GENERATOR_RESTART_CANCEL_REASON = 'Task generator restarted.'
EXISTS_MORE_LATE_TASK_CANCEL_REASON = 'Task not started and exists more late same task.'
WORKER_DEAD_CANCEL_REASON = 'Worker not alive.'

TASK_TYPE_MONIT = 'monit'
TASK_TYPE_WORK = 'work'

STOP_WORKER_MSG = 'stop_worker'
