__author__ = 'Jorge R. Herskovic <jherskovic@gmail.com>'

import multiprocessing
import multiprocessing.connection
import threading
import collections

_pipeline_message = collections.namedtuple("pipeline_message", ["destination", "data"])
_registration_message = collections.namedtuple("registration_message", ["name", "queue"])
_goodbye_message = collections.namedtuple("goodbye_message", ["name"])

_queue_manager = multiprocessing.Manager()

class MessagingCenter(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
        self._known_pipelines = {}
        self._incoming = multiprocessing.Queue()
        self.daemon = True

    def central_receiver(self):
        while True:
            msg = self._incoming.get()

            if isinstance(msg, _registration_message):
                self._known_pipelines[msg.name] = msg.queue
            elif isinstance(msg, _pipeline_message):
                self._known_pipelines[msg.destination].put(msg.data)
            elif isinstance(msg, _goodbye_message):
                del self._known_pipelines[msg.name]

    def register_pipeline(self, name):
        """Takes a pipeline name and returns a queue that should be listened to for messages."""
        return_queue = _queue_manager.Queue()
        self._incoming.put(_registration_message(name, return_queue))
        return return_queue

    def receive_message_in_process(self, internal_queue, queue):
        while True:
            item = internal_queue.get()
            queue.put(item)

    def send_message(self, name, data):
        self._incoming.put(_pipeline_message(name, data))

    def register_pipeline_queue(self, name, queue):
        """Starts a background daemonic thread that receives messages and places them in the designated queue."""
        # Register with the central repository and receive a port number
        internal_queue = self.register_pipeline(name)
        new_listener = threading.Thread(target = self.receive_message_in_process, args=(internal_queue, queue),
                                        daemon=True)
        new_listener.start()
        return

    def run(self):
        self.central_receiver()


pipeline_messaging = MessagingCenter()
pipeline_messaging.start()
