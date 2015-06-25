__author__ = 'Jorge Herskovic <jherskovic@gmail.com>'

from .pipeline import _Pipeline
from .messaging import MessagingCenter


# The following class is the one actually meant for instantiation by clients of this library.
# DO NOT use _Pipeline. Use Pipeline.
class Pipeline(_Pipeline):
    _messaging = None

    def __init__(self, name=None, max_size=-1):
        super().__init__(max_size)
        self._name = name

        # We only need messaging capabilities if we have named Pipelines; therefore we only check for (and start) the
        # messaging center if the Pipeline's name is set.
        if self._name:
            if Pipeline._messaging is None:
                Pipeline._messaging = MessagingCenter()
                Pipeline._messaging.start()

    def start(self):
        super().start()
        # Register this Pipeline with the central MessagingCenter.
        if self._name:
            Pipeline._messaging.register_pipeline_queue(self._name, self._queues[0])

    def join(self):
        super().join()
        # Any pipeline, even a non-named one, may be feeding other pipelines; therefore, after joining,
        # we'll make sure that the pipeline is flushed if there is one.
        if Pipeline._messaging:
            Pipeline._messaging.flush()

    @staticmethod
    def send_multiple(dest, obj_list):
        """Sends a list of picklable objects to another named pipeline."""
        if Pipeline._messaging:
            Pipeline._messaging.send_message(dest, obj_list)
        else:
            raise ValueError("There are no named pipelines.")

    @staticmethod
    def send(dest, obj):
        Pipeline.send_multiple(dest, [obj])

