from __future__ import absolute_import
from tornado import gen, testing

from topika import connect_robust
from test import AMQP_URL
from test.test_amqp import TestCase as AMQPTestCase


class TestCase(AMQPTestCase):

    @gen.coroutine
    def create_connection(self, cleanup=True):
        client = yield connect_robust(AMQP_URL, loop=self.loop)

        if cleanup:
            self.addCleanup(self.wait_for, client.close)

        raise gen.Return(client)

    @testing.gen_test
    def test_set_qos(self):
        channel = yield self.create_channel()
        yield channel.set_qos(prefetch_count=1)
