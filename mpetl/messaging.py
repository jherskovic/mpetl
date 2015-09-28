from __future__ import with_statement
from __future__ import absolute_import
__author__ = u'Jorge R. Herskovic <jherskovic@gmail.com>'

import multiprocessing
import threading
import collections
import logging
import traceback
import weakref
from .util import SENTINEL, _random_string, dprint

pipeline_message = collections.namedtuple(u"pipeline_message", [u"destination", u"data"])
registration_message = collections.namedtuple(u"registration_message", [u"name", u"queue"])
goodbye_message = collections.namedtuple(u"goodbye_message", [u"name"])


class MessagingCenter(multiprocessing.Process):
    _queue_manager = None
    _queue_manager_lock = None

    def __init__(self):
        multiprocessing.Process.__init__(self)
        self._known_pipelines = {}
        self._incoming = multiprocessing.Queue()
        self.daemon = True
        if MessagingCenter._queue_manager is None:
            MessagingCenter._queue_manager = multiprocessing.Manager()
            MessagingCenter._queue_manager_lock = multiprocessing.Lock()
            # MessagingCenter._queue_manager.start()
        # self._finalizer = weakref.finalize(self, MessagingCenter._cleanup, self._known_pipelines)
        dprint(u"Finished setting up messaging center.")

    def _close_outgoing(self):
        for pipeline in self._known_pipelines.values():
            if pipeline is not None:
                pipeline.put(SENTINEL)

    def central_receiver(self):
        try:
            while True:
                msg = self._incoming.get()
                if msg == SENTINEL:
                    self._close_outgoing()
                    break

                if isinstance(msg, registration_message):
                    self._known_pipelines[msg.name] = msg.queue
                elif isinstance(msg, pipeline_message):
                    if self._known_pipelines[msg.destination] is not None:
                        self._known_pipelines[msg.destination].put(msg.data)
                    else:
                        logging.warning(u"Received message %r for a closed pipeline.", msg)
                elif isinstance(msg, goodbye_message):
                    if self._known_pipelines[msg.name] is not None:
                        self._known_pipelines[msg.name].put(SENTINEL)
                        self._known_pipelines[msg.name] = None
                    else:
                        logging.warning(u"Attempted to close a closed pipeline (%r).", msg.name)
        except EOFError:
            self._close_outgoing()
        except:
            logging.error(traceback.format_exc())

    def create_incoming_queue(self, name):
        u"""Takes a pipeline name and returns a queue that should be listened to for messages."""
        with MessagingCenter._queue_manager_lock:
            return_queue = self._queue_manager.Queue()
        self._incoming.put(registration_message(name, return_queue))
        return return_queue

    @staticmethod
    def receive_message_in_process(internal_queue, queue):
        try:
            while True:
                item = internal_queue.get()
                if item == SENTINEL:
                    break
                if queue() is not None:
                    queue().put(item)
        except EOFError:
            return

    def send_message(self, name, data):
        self._incoming.put(pipeline_message(name, data))

    def register_pipeline_queue(self, name, queue):
        u"""Starts a background daemonic thread that receives messages and places them in the designated queue."""
        # Register with the central repository and receive a port number
        internal_queue = self.create_incoming_queue(name)
        new_listener = threading.Thread(target=self.receive_message_in_process,
                                        args=(internal_queue, weakref.ref(queue)))
        new_listener.daemon = True
        new_listener.start()
        return

    def register_pipeline(self, name, pipeline):
        self.register_pipeline_queue(name, pipeline.input_queue)

    def forget_pipeline(self, name):
        self._incoming.put(goodbye_message(name))

    def run(self):
        self.central_receiver()

    def flush(self):
        u"""Ensures that all messages up to this point have been processed by the pipeline by sending, and recognizing,
        one specific message."""
        queue_name = u'__*($#^%' + _random_string()
        temp_queue = multiprocessing.Queue()
        self.register_pipeline_queue(queue_name, temp_queue)
        self.send_message(queue_name, 0)
        temp_queue.get()
        temp_queue.close()
        self.forget_pipeline(queue_name)

    def __del__(self):
        pipelines = self._known_pipelines
        dprint(u"Running cleanup for the MessagingCenter")
        if pipelines is not None:
            for p in pipelines.values():
                if p is not None:
                    p.put(SENTINEL)

