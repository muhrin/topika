from __future__ import absolute_import
from __future__ import print_function

import functools
import logging
import tempfile
import os
import shortuuid
from tornado import gen
from tornado.testing import gen_test, AsyncTestCase
from furl import furl
from topika import Connection, connect, Channel, Exchange

for logger_name in ('pika.channel', 'pika.callback', 'pika.connection'):
    logging.getLogger(logger_name).setLevel(logging.INFO)

testfile = os.path.join(tempfile.gettempdir(), 'topkia_unittest.log')
try:
    os.remove(testfile)
except OSError:
    pass
FORMAT = "[%(filename)s:%(lineno)s - %(funcName)s()] %(message)s"
logging.basicConfig(filename=testfile, level=logging.INFO, format=FORMAT)

AMQP_URL = furl(os.getenv("AMQP_URL", "amqp://guest:guest@127.0.0.1"))


class BaseTestCase(AsyncTestCase):

    def setUp(self):
        super(BaseTestCase, self).setUp()
        self.loop = self.io_loop
        self._async_cleanups = []

    def tearDown(self):
        while self._async_cleanups:
            function, args, kwargs = self._async_cleanups.pop(-1)
            self.loop.run_sync(functools.partial(function, *args, **kwargs))

        super(BaseTestCase, self).tearDown()

    @gen.coroutine
    def wait_for(self, fn, *args, **kwargs):
        yield fn(*args, **kwargs)

    def addCleanup(self, function, *args, **kwargs):
        """
        Add a function, with arguments, to be called when the test is
        completed. If function is a coroutine function, it will run on the loop
        before it's cleaned.
        """
        if gen.is_coroutine_function(function):
            return self._async_cleanups.append((function, args, kwargs))

        return super(BaseTestCase, self).addCleanup(function, *args, **kwargs)

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
            self.addCleanup(self.wait_for, client.close)

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
            self.addCleanup(self.wait_for, channel.close)

        raise gen.Return(channel)

    @gen.coroutine
    def declare_queue(self, *args, **kwargs):
        """
        :rtype: Generator[Any, None, Queue]
        """
        if 'channel' not in kwargs:
            channel = yield self.create_channel()
        else:
            channel = kwargs.pop('channel')

        queue = yield channel.declare_queue(*args, **kwargs)
        self.addCleanup(self.wait_for, queue.delete)
        raise gen.Return(queue)

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
        self.addCleanup(self.wait_for, exchange.delete)
        raise gen.Return(exchange)
