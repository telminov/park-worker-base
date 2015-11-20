# coding: utf-8
import json
from unittest import TestCase
from unittest import mock
import datetime

from bson import json_util
from parkkeeper import factories
from parkworker import const
from parkworker.const import WORKER_EVENT, TASK_EVENT, MONIT_STATUS_EVENT, STOP_WORKER_MSG
from parkworker.utils import now
from parkworker.worker import BaseWorker, TaskProcessor


class WorkerTestCase(TestCase):

    def setUp(self):
        self.monit_task = factories.MonitTask(monit_name=factories.Monit().name)
        self.work_task = factories.WorkTask(work_name=factories.Work().name)

    def get_task_data(self):
        monit_task = factories.MonitTask()
        return monit_task.get_data()

    def get_stop_msg(self, worker):
        return json.dumps({const.STOP_WORKER_MSG: worker.uuid}).encode('utf-8')

    def create_worker(self):
        worker = BaseWorker()
        worker.ZMQ_SERVER_ADDRESS = 'test'
        worker.ZMQ_WORKER_REGISTRATOR_PORT = 321
        worker.scheduler_port = 123
        worker._init_worker()
        return worker

    def get_emit_args(self, emit_mock, topic_filter):
        args_json = [args[0][1] for args in emit_mock.call_args_list if args[0][0] == topic_filter][0]
        args = json.loads(args_json, object_hook=json_util.object_hook)
        return args

    @mock.patch.object(BaseWorker, '_register_worker')
    def test_setup(self, register_worker_mock):
        worker = BaseWorker()
        worker.setup()

        self.assertTrue(worker.id)
        self.assertTrue(worker.uuid)
        self.assertAlmostEqual(worker.created_dt, now(), delta=datetime.timedelta(seconds=1))
        self.assertTrue(worker.host_name)
        self.assertTrue(register_worker_mock.called)

    @mock.patch('parkworker.worker.zmq.Context')
    @mock.patch.object(BaseWorker, '_register_start_task')
    @mock.patch.object(BaseWorker, '_process_task')
    @mock.patch.object(BaseWorker, '_emit_worker')
    def test_receive_tasks(self, emit_worker_mock, process_task_mock, register_start_task_mock, context_mock):
        task_socket_mock = context_mock.return_value.socket.return_value

        task_data = self.monit_task.get_data()
        task_data_json = self.monit_task.get_json()
        task_data_json = task_data_json.encode('utf-8')

        worker = self.create_worker()
        task_socket_mock.recv_string.side_effect = (task_data_json, self.get_stop_msg(worker))

        worker._receive_tasks()

        call_args = mock.call(task_data['task'], task_data['type'])
        self.assertEqual(
            register_start_task_mock.call_args,
            call_args
        )
        self.assertEqual(
            process_task_mock.call_args,
            call_args
        )
        self.assertIn('stop_dt', emit_worker_mock.call_args[0][0])

    @mock.patch.object(BaseWorker, '_register_complete_task')
    @mock.patch.object(TaskProcessor, 'process')
    def test_process_task(self, task_process_mock, register_complete_task_mock):
        worker = self.create_worker()
        data = self.monit_task.get_data()
        self.assertFalse(data['task'].get('result'))

        worker._process_task(data['task'], data['type'])

        self.assertTrue(data['task']['result'])
        start_args = mock.call(self.monit_task.get_name(), self.monit_task.get_task_type(), host=self.monit_task.host_address)
        self.assertEqual(task_process_mock.call_args, start_args)
        self.assertEqual(register_complete_task_mock.call_args, mock.call(data['task'], data['type']))

    def test_prepare_task_options(self):
        data = self.monit_task.get_data()
        task_name, task_options = BaseWorker._prepare_task_options(data['task'], self.monit_task.get_task_type())
        self.assertEqual(self.monit_task.get_name(), task_name)
        self.assertEqual({'host': self.monit_task.host_address}, task_options)

    @mock.patch.object(BaseWorker, 'is_stopped')
    @mock.patch.object(BaseWorker, 'emit_event')
    @mock.patch('parkworker.worker.time.sleep')
    def test_heart_beat(self, sleep_mock, emit_mock, is_stopped_mock):
        is_stopped_mock.side_effect = (False, True)
        worker = BaseWorker()
        worker._init_worker()

        worker._heart_beat()

        self.assertEqual(WORKER_EVENT, emit_mock.call_args[0][0])
        worker_data = json.loads(emit_mock.call_args[0][1], object_hook=json_util.object_hook)
        self.assertAlmostEqual(
            now(),
            worker_data['heart_beat_dt'],
            delta=datetime.timedelta(seconds=1)
        )

    @mock.patch('parkworker.worker.zmq.Context')
    def test_register_worker(self, context_mock):
        answer_data = {'scheduler_port': 123}
        register_socket_mock = context_mock.return_value.socket.return_value
        register_socket_mock.recv_string.return_value = json.dumps(answer_data)

        worker = BaseWorker()
        worker._init_worker()
        worker._register_worker()

        self.assertTrue(register_socket_mock.send_string.called)
        self.assertEqual(worker.scheduler_port, answer_data['scheduler_port'])

    @mock.patch.object(BaseWorker, 'emit_event')
    def test_register_start_task(self, emit_mock):
        worker = self.create_worker()
        data = self.monit_task.get_data()

        worker._register_start_task(data['task'], data['type'])

        worker_data = self.get_emit_args(emit_mock, WORKER_EVENT)
        self.assertIn('start_task', worker_data)

        task_data = self.get_emit_args(emit_mock, TASK_EVENT)['task']
        self.assertAlmostEqual(
            task_data['start_dt'],
            now(),
            delta=datetime.timedelta(seconds=1)
        )
        self.assertEqual(
            task_data['worker']['uuid'],
            worker.uuid
        )

    @mock.patch.object(BaseWorker, 'emit_event')
    def test_register_complete_task(self, emit_mock):
        worker = self.create_worker()
        data = self.monit_task.get_data()

        worker._register_complete_task(data['task'], data['type'])

        worker_data = self.get_emit_args(emit_mock, WORKER_EVENT)
        self.assertIn('complete_task', worker_data)

        monit_status_data = self.get_emit_args(emit_mock, MONIT_STATUS_EVENT)
        self.assertTrue(monit_status_data)

        task_data = self.get_emit_args(emit_mock, TASK_EVENT)
        self.assertEqual(task_data, data)

    def test_get_worker_data(self):
        worker = self.create_worker()
        additional_data = {'test': 123}

        worker_data = worker._get_worker_data(additional_data)
        self.assertEqual(worker_data['main']['uuid'], worker.uuid)
        self.assertAlmostEqual(worker_data['heart_beat_dt'], now(), delta=datetime.timedelta(seconds=1))
        self.assertEqual(worker_data['test'], additional_data['test'])

    def test_is_stop_worker_msg(self):
        worker = self.create_worker()

        self.assertFalse(
            worker._is_stop_worker_msg({'test': '123'})
        )

        self.assertFalse(
            worker._is_stop_worker_msg({STOP_WORKER_MSG: '123'})
        )

        self.assertTrue(
            worker._is_stop_worker_msg({STOP_WORKER_MSG: worker.uuid})
        )

    def test_get_task_name(self):
        worker = self.create_worker()

        monit_data = self.monit_task.get_data()
        self.assertEqual(
            worker._get_task_name(monit_data['task'], monit_data['type']),
            self.monit_task.get_name()
        )

        work_data = self.work_task.get_data()
        self.assertEqual(
            worker._get_task_name(work_data['task'], work_data['type']),
            self.work_task.get_name()
        )