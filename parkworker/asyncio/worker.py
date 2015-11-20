# coding: utf-8
import asyncio
import zmq

from parkworker.asyncio.task_processor import AsyncTaskProcessor
from parkworker.const import WORKER_HEART_BEAT_PERIOD
from parkworker.utils import now
from parkworker.worker import BaseWorker


class AsyncBaseWorker(BaseWorker):

    async def async_recv_pull_msg(self, subscriber_socket: zmq.Socket) -> dict:
        raise NotImplemented()

    def run(self):
        if self.verbose:
            print('Async Worker start %s' % self.id)

        loop = asyncio.get_event_loop()
        try:
            loop.create_task(self._heart_beat())
            loop.create_task(self._receive_tasks())
            loop.run_forever()
        finally:
            loop.close()

    async def _receive_tasks(self):
        task_socket = self._get_task_socket()
        try:
            loop = asyncio.get_event_loop()
            while True:
                data = await self.async_recv_pull_msg(task_socket)
                task_data = data['task']
                task_type = data['type']

                self._register_start_task(task_data, task_type)
                loop.create_task(self._process_task(task_data, task_type))
        finally:
            self._emit_worker({'stop_dt': now()})
            task_socket.close()

    async def _heart_beat(self):
        while True:
            self._emit_worker()
            await asyncio.sleep(WORKER_HEART_BEAT_PERIOD)

    async def _process_task(self, task_data, task_type):
        task_name, task_kwargs = self._prepare_task_options(task_data, task_type)

        result = await AsyncTaskProcessor.async_process(task_name, task_type, **task_kwargs)
        task_data['result'] = result.get_dict()

        self._register_complete_task(task_data, task_type)