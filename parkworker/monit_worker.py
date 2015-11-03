# coding: utf-8
import time
import json
import multiprocessing
import socket
import uuid
import zmq

from parkworker.const import MONIT_WORKER_HEART_BEAT_PERIOD, MONIT_STATUS_EVENT, MONIT_WORKER_EVENT, MONIT_TASK_EVENT
from parkworker.utils import now, json_default
from parkworker.monits.base import Monit


class BaseMonitWorker(multiprocessing.Process):
    id = None
    uuid = None
    created_dt = None
    host_name = None
    tasks = None
    monit_scheduler_port = None

    ZMQ_SERVER_ADDRESS = None
    ZMQ_WORKER_REGISTRATOR_PORT = None
    worker_type = None

    def emit_event(self, *args, **kwargs):
        raise NotImplemented()

    def setup(self, worker_id=None):
        if worker_id is None:
            self.id = self.uuid
        else:
            self.id = worker_id

        self.uuid = str(uuid.uuid4())
        self.created_dt = now()
        self.host_name = socket.gethostname()
        self.tasks = dict()

        self.register_worker()

    def run(self):
        task_socket = self.get_task_socket()

        heart_beat_process = multiprocessing.Process(target=self._heart_beat)
        heart_beat_process.daemon = True
        heart_beat_process.start()

        print('Worker start %s' % self.id)

        try:
            while True:
                task_json = task_socket.recv_json()
                task = json.loads(task_json)
                monit_name = task['monit_name']
                host_address = task['host_address']
                task_options = task['options']

                print("Worker %s. Received request: %s for %s" % (self.id, monit_name, host_address))

                monit = Monit.get_monit(monit_name)()

                self._register_start_task(task)

                result = monit.check(
                    host=host_address,
                    **task_options
                )

                self._register_complete_task(task, result)

                # get new monitoring results
                self.emit_event(MONIT_STATUS_EVENT, json.dumps(task, default=json_default))
        finally:
            self._emit_worker({'stop_dt': now()})

    def get_task_socket(self):
        context = zmq.Context()
        task_socket = context.socket(zmq.PULL)
        task_socket.connect("tcp://%s:%s" % (self.ZMQ_SERVER_ADDRESS, self.monit_scheduler_port))
        print('MonitWorker connect to', self.ZMQ_SERVER_ADDRESS, self.monit_scheduler_port)
        return task_socket

    def register_worker(self):
        context = zmq.Context()
        register_socket = context.socket(zmq.REQ)
        register_socket.connect("tcp://%s:%s" % (self.ZMQ_SERVER_ADDRESS, self.ZMQ_WORKER_REGISTRATOR_PORT))
        try:
            monit_names = [n for n, _ in Monit.get_all_monits()]
            register_data = {
                'main': self._get_worker(),
                'heart_beat_dt': now(),
                'monit_names': monit_names,
            }
            register_data_json = json.dumps(register_data, default=json_default)
            register_socket.send_string(register_data_json)
            # print('register_worker send', register_data_json)
            keeper_answer = register_socket.recv_string()
            # print('register_worker got', keeper_answer)
            answer_data = json.loads(keeper_answer)
            self.monit_scheduler_port = answer_data['monit_scheduler_port']
        finally:
            register_socket.close()

    def _register_start_task(self, task):
        self._add_current_task(task)
        task['start_dt'] = now()
        task['worker'] = self._get_worker()
        self.emit_event(MONIT_TASK_EVENT, json.dumps(task, default=json_default))

    def _register_complete_task(self, task, result):
        self._rm_current_task(task)
        task['result'] = result.get_dict()
        self.emit_event(MONIT_TASK_EVENT, json.dumps(task, default=json_default))

    def _add_current_task(self, task):
        task_id = self._get_task_id(task)
        self.tasks[task_id] = task
        self._emit_worker({'tasks': list(self.tasks.keys())})

    def _rm_current_task(self, task):
        task_id = self._get_task_id(task)
        del self.tasks[task_id]
        self._emit_worker({'tasks': list(self.tasks.keys())})

    def _get_worker(self):
        return {
            'id': str(self.id),
            'uuid': self.uuid,
            'created_dt': self.created_dt,
            'host_name': self.host_name,
            'type': self.worker_type,
        }

    def _emit_worker(self, data=None):
        worker_data = {
            'main': self._get_worker(),
            'heart_beat_dt': now(),
        }

        if data:
            worker_data.update(data)

        worker_data_json = json.dumps(worker_data, default=json_default)
        self.emit_event(MONIT_WORKER_EVENT, worker_data_json)

    def _heart_beat(self):
        while True:
            self._emit_worker()
            time.sleep(MONIT_WORKER_HEART_BEAT_PERIOD)

    @staticmethod
    def _get_task_id(task):
        return task['_id']['$oid']


