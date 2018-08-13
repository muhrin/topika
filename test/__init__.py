from future.standard_library import install_aliases

# Enable urlparse.parse in python2/3
install_aliases()

import logging
import tempfile
import os
import shortuuid
from tornado import gen
from tornado.testing import gen_test, AsyncTestCase
from urllib.parse import urlparse

from topika import Connection, connect, Channel, Exchange

testfile = os.path.join(tempfile.gettempdir(), 'topkia_unittest.log')
try:
    os.remove(testfile)
except OSError:
    pass
print("Logging test to '{}'".format(testfile))
FORMAT = "[%(filename)s:%(lineno)s - %(funcName)s()] %(message)s"
logging.basicConfig(filename=testfile, level=logging.INFO, format=FORMAT)

AMQP_URL = os.getenv("AMQP_URL", "amqp://guest:guest@127.0.0.1")


# Was here from aio-pika
# if not AMQP_URL.path:
#     AMQP_URL.path = '/'


class BaseTestCase(AsyncTestCase):
    def setUp(self):
        super(BaseTestCase, self).setUp()
        self.loop = self.io_loop

    def get_random_name(self, *args):
        prefix = ['test']
        for item in args:
            prefix.append(item)
        prefix.append(shortuuid.uuid())

        return ".".join(prefix)

    @gen.coroutine
    def create_connection(self, cleanup=True):
        """
        :rtype:  Generator[Any, None, Connection]
        """
        client = yield connect(AMQP_URL, loop=self.loop)

        if cleanup:
            self.addCleanup(client.close)

        raise gen.Return(client)

    @gen.coroutine
    def create_channel(self, connection=None, cleanup=True, **kwargs):
        """
        :rtype: Generator[Any, None, Channel]
        """
        if connection is None:
            connection = yield self.create_connection()

        channel = yield connection.channel(**kwargs)

        if cleanup:
            self.addCleanup(channel.close)

        raise gen.Return(channel)

    #
    # @gen.coroutine
    # def declare_queue(self, *args, **kwargs):
    #     """
    #     :rtype: Generator[Any, None, Queue]
    #     """
    #     if 'channel' not in kwargs:
    #         channel = yield self.create_channel()
    #     else:
    #         channel = kwargs.pop('channel')
    #
    #     queue = yield channel.declare_queue(*args, **kwargs)
    #     self.addCleanup(queue.delete)
    #     raise gen.Return(queue)

    @gen.coroutine
    def declare_exchange(self, *args, **kwargs):
        """
        :rtype: Generator[Any, None, Exchange]
        """
        if 'channel' not in kwargs:
            channel = yield self.create_channel()
        else:
            channel = kwargs.pop('channel')

        exchange = yield channel.declare_exchange(*args, **kwargs)
        self.addCleanup(exchange.delete)
        raise gen.Return(exchange)
