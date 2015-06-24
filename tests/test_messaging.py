#!/usr/bin/env python

__author__ = 'Jorge R. Herskovic <jherskovic@gmail.com>'

import unittest
from mpetl import Pipeline
from mpetl.messaging import *

def generator(up_to):
    for i in range(up_to):
        yield i
    return

def conditional_routing(number, pipeline_msg):
    """Routes even numbers to a pipeline called "even" and odd numbers to a pipeline called "odd"
    """
    print("Got", number)
    if number % 2 == 0:
        pipeline_msg.send_message("even", [number])
    else:
        pipeline_msg.send_message("odd", [number])


def gathering_function(number):
    print("Gathering",number)
    return number


class test_messaging(unittest.TestCase):
    def setUp(self):
        self.messaging = MessagingCenter()
        self.messaging.start()

    def test_result(self):
        self.evens = Pipeline()
        self.evens.add_destination(gathering_function)
        self.evens.start()
        print("Evensstarted")

        self.odds = Pipeline()
        self.odds.add_destination(gathering_function)
        self.odds.start()
        print("Oddsstarted")

        self.original = Pipeline()
        self.original.add_origin(generator)
        self.original.add_destination(conditional_routing, pipeline_msg = self.messaging)
        self.original.start()
        print("OriginalStarted")

        self.messaging.register_pipeline_queue("even", self.evens._queues[0])
        self.messaging.register_pipeline_queue("odd", self.odds._queues[0])
        print("messaging_registered")

        self.original.feed(100)

        self.original.join()
        print("reading resutls")
        self.messaging.flush()

        self.odds.join()
        self.evens.join()
        evens = [x for x in self.evens.as_completed()]
        odds = [x for x in self.odds.as_completed()]
        print("evens:", evens)
        print("odds:", odds)

        self.messaging.close_pipeline_queue("odd")
        self.messaging.close_pipeline_queue("even")

if __name__ == '__main__':
    unittest.main()
