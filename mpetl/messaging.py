__author__ = 'Jorge R. Herskovic <jherskovic@gmail.com>'

import multiprocessing
import multiprocessing.connection
import threading
import collections
import random

from mpetl import _Sentinel

pipeline_message = collections.namedtuple("pipeline_message", ["destination", "data"])
registration_message = collections.namedtuple("registration_message", ["name", "queue"])
goodbye_message = collections.namedtuple("goodbye_message", ["name"])
_sentinel_singleton = _Sentinel()

class MessagingCenter(multiprocessing.Process):
    _queue_manager = None

    def __init__(self):
        multiprocessing.Process.__init__(self)
        self._known_pipelines = {}
        self._incoming = multiprocessing.Queue()
        self.daemon = True
        if MessagingCenter._queue_manager is None:
            MessagingCenter._queue_manager = multiprocessing.Manager()
            # MessagingCenter._queue_manager.start()

    def central_receiver(self):
        while True:
            msg = self._incoming.get()
            print(msg)
            if isinstance(msg, _Sentinel):
                for pipeline in self._known_pipelines.values():
                    pipeline.put(_sentinel_singleton)
                break

            if isinstance(msg, registration_message):
                self._known_pipelines[msg.name] = msg.queue
            elif isinstance(msg, pipeline_message):
                self._known_pipelines[msg.destination].put(msg.data)
            elif isinstance(msg, goodbye_message):
                print("Known:", self._known_pipelines)
                self._known_pipelines[msg.name].put(_sentinel_singleton)
                del self._known_pipelines[msg.name]

    def register_pipeline(self, name):
        """Takes a pipeline name and returns a queue that should be listened to for messages."""
        print("Requesting a Queue from the manager for", name)
        return_queue = self._queue_manager.Queue()
        print("My new queue is", return_queue)
        self._incoming.put(registration_message(name, return_queue))
        return return_queue

    @staticmethod
    def receive_message_in_process(internal_queue, queue):
        while True:
            item = internal_queue.get()
            if isinstance(item, _Sentinel):
                del internal_queue
                print("Goodbye cruel world")
                return
            queue.put(item)

    def send_message(self, name, data):
        self._incoming.put(pipeline_message(name, data))

    def register_pipeline_queue(self, name, queue):
        """Starts a background daemonic thread that receives messages and places them in the designated queue."""
        # Register with the central repository and receive a port number
        print("Registering", name)
        internal_queue = self.register_pipeline(name)
        print("Starting listener")
        new_listener = threading.Thread(target=self.receive_message_in_process,
                                        args=(internal_queue, queue),
                                        daemon=True)
        new_listener.start()
        return

    def close_pipeline_queue(self, name):
        self._incoming.put(goodbye_message(name))

    def run(self):
        self.central_receiver()

    def flush(self):
        """Ensures that all messages up to this point have been processed by the pipeline by sending, and recognizing,
        one specific message."""
        queue_name = '__*($#^%' + ''.join(random.choice('abcdefghijklmnopqrstuvwxyz') for i in range(50))
        temp_queue = multiprocessing.Queue()
        self.register_pipeline_queue(queue_name, temp_queue)
        self.send_message(queue_name, 0)
        temp_queue.get()
        temp_queue.close()
        self.close_pipeline_queue(queue_name)

