from tornado import gen, testing
from unittest import skip

from test.test_amqp import TestCase as AMQPTestCase


class TestCase(AMQPTestCase):
    @testing.gen_test
    def create_channel(self, connection=None, cleanup=True, publisher_confirms=False, **kwargs):
        if connection is None:
            connection = yield self.create_connection()

        channel = yield connection.channel(publisher_confirms=publisher_confirms, **kwargs)

        if cleanup:
            self.addCleanup(channel.close)

        raise gen.Return(channel)

    test_simple_publish_and_receive = skip("skipped")(AMQPTestCase.test_simple_publish_and_receive)
    test_simple_publish_without_confirm = skip("skipped")(AMQPTestCase.test_simple_publish_without_confirm)
