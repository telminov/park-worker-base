# coding: utf-8
from __future__ import print_function
import time
import json
import multiprocessing
import socket
import uuid
from copy import deepcopy
import zmq
from bson import json_util

from parkworker.const import WORKER_HEART_BEAT_PERIOD, MONIT_STATUS_EVENT, \
    WORKER_EVENT, TASK_EVENT, TASK_TYPE_MONIT, TASK_TYPE_WORK, STOP_WORKER_MSG, WORK_STATUS_EVENT
from parkworker.utils import now
from parkworker.task_processor import TaskProcessor, UnknownTaskTypeException
from parkworker.monit import Monit
from parkworker.work import Work


class BaseWorker(multiprocessing.Process):
    ZMQ_SERVER_ADDRESS = None
    ZMQ_WORKER_REGISTRATOR_PORT = None
    worker_type = None

    def emit_event(self, *args, **kwargs):
        raise NotImplemented()

    def setup(self, worker_id=None, verbose=False):
        self._init_worker(worker_id, verbose)
        self._register_worker()

    def stop(self):
        self.stop_event.set()

    def is_stopped(self):
        return self.stop_event.is_set()

    def run(self):
        if self.verbose:
            print('Worker start %s' % self.id)

        heart_beat_process = multiprocessing.Process(target=self._heart_beat)
        heart_beat_process.daemon = True
        heart_beat_process.start()

        self._receive_tasks()

    def _init_worker(self, worker_id=None, verbose=False):
        self.uuid = str(uuid.uuid4())
        if worker_id is None:
            self.id = self.uuid
        else:
            self.id = worker_id
        self.created_dt = now()
        self.host_name = socket.gethostname()
        self.stop_event = multiprocessing.Event()
        self.verbose = verbose

    def _register_worker(self):
        context = zmq.Context()
        register_socket = context.socket(zmq.REQ)
        register_socket.connect("tcp://%s:%s" % (self.ZMQ_SERVER_ADDRESS, self.ZMQ_WORKER_REGISTRATOR_PORT))
        try:
            monits = {name: monit_class.description for name, monit_class in Monit.get_all_monits()}
            works = {name: work_class.description for name, work_class in Work.get_all_works()}

            worker_data = self._get_worker_data(data={
                'monits': monits,
                'works': works,
            })
            worker_data_json = json.dumps(worker_data, default=json_util.default)
            register_socket.send_string(worker_data_json)

            answer_json = register_socket.recv_string()
            answer_data = json.loads(answer_json, object_hook=json_util.object_hook)
            self.scheduler_port = answer_data['scheduler_port']
        finally:
            register_socket.close()

    def _receive_tasks(self):
        task_socket = self._get_task_socket()
        try:
            while not self.is_stopped():
                msg = task_socket.recv_string()
                msg = msg.decode('utf-8')
                data = json.loads(msg, object_hook=json_util.object_hook)
                if self._is_stop_worker_msg(data):
                    return

                # print('Worker %s receive task' % self.uuid, data)
                task_data = data['task']
                task_type = data['type']

                self._register_start_task(task_data, task_type)
                self._process_task(task_data, task_type)
        finally:
            self._emit_worker({'stop_dt': now()})
            task_socket.close()

    def _heart_beat(self):
        while not self.is_stopped():
            self._emit_worker()
            time.sleep(WORKER_HEART_BEAT_PERIOD)

    def _process_task(self, task_data, task_type):
        task_name, task_kwargs = self._prepare_task_options(task_data, task_type)

        result = TaskProcessor.process(task_name, task_type, **task_kwargs)
        task_data['result'] = result.get_dict()

        self._register_complete_task(task_data, task_type)

    @classmethod
    def _prepare_task_options(cls, task_data, task_type):
        task_name = cls._get_task_name(task_data, task_type)

        task_kwargs = deepcopy(task_data['options'])
        if task_data.get('host_address'):
            task_kwargs['host'] = task_data['host_address']

        return task_name, task_kwargs

    def _get_task_socket(self):
        context = zmq.Context()
        task_socket = context.socket(zmq.PULL)
        task_socket.connect("tcp://%s:%s" % (self.ZMQ_SERVER_ADDRESS, self.scheduler_port))
        if self.verbose:
            print('Worker connect to', self.ZMQ_SERVER_ADDRESS, self.scheduler_port)
        return task_socket

    def _register_start_task(self, task_data, task_type):
        task_name = self._get_task_name(task_data, task_type)

        if self.verbose:
            print("Worker %s. Received request: %s for %s. %s" % (self.id, task_name, task_data['host_address'], now()))

        self._emit_worker({'start_task': task_data})

        task_data['start_dt'] = now()
        task_data['worker'] = self._get_worker_data()['main']
        self._emit_task(task_data, task_type)

    def _register_complete_task(self, task_data, task_type):
        if task_type == TASK_TYPE_MONIT:
            self.emit_event(MONIT_STATUS_EVENT, json.dumps(task_data, default=json_util.default))
        if task_type == TASK_TYPE_WORK:
            self.emit_event(WORK_STATUS_EVENT, json.dumps(task_data, default=json_util.default))
        self._emit_task(task_data, task_type)
        self._emit_worker({'complete_task': task_data})

    def _get_worker_data(self, data=None):
        worker_data = {
            'main': {
                'id': str(self.id),
                'uuid': self.uuid,
                'created_dt': self.created_dt,
                'host_name': self.host_name,
                'type': self.worker_type,
            },
            'heart_beat_dt': now(),
        }
        if data:
            worker_data.update(data)
        return worker_data

    def _emit_worker(self, data=None):
        worker_data = self._get_worker_data(data=data)
        worker_data_json = json.dumps(worker_data, default=json_util.default)
        self.emit_event(WORKER_EVENT, worker_data_json)

    def _emit_task(self, task_data, task_type):
        data = {
            'task': task_data,
            'type': task_type,
        }
        task_json = json.dumps(data, default=json_util.default)
        self.emit_event(TASK_EVENT, task_json)

    def _is_stop_worker_msg(self, msg):
        return STOP_WORKER_MSG in msg and msg[STOP_WORKER_MSG] == self.uuid

    @staticmethod
    def _get_task_name(task_data, task_type):
        if task_type not in (TASK_TYPE_MONIT, TASK_TYPE_WORK):
            raise Exception('Unknown task type %s' % task_type)

        if task_type == TASK_TYPE_MONIT:
            task_name = task_data['monit_name']
        else:
            task_name = task_data['work_name']

        return task_name
