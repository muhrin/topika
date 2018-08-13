"""blocking adapter test"""
from datetime import datetime
from datetime import timedelta
import functools
import logging
import socket
import threading
import time
import sys
import tornado.gen
from tornado.concurrent import Future
from tornado.gen import coroutine, Return
from tornado.testing import gen_test, AsyncTestCase
import unittest
import uuid


from forward_server import ForwardServer
from test_utils import retry_assertion

import pika
from pika.compat import as_bytes
import pika.connection
import pika.exceptions

import topika
import topika.common
import topika.channel

# Disable warning about access to protected member
# pylint: disable=W0212

# Disable warning Attribute defined outside __init__
# pylint: disable=W0201

# Disable warning Missing docstring
# pylint: disable=C0111

# Disable warning Too many public methods
# pylint: disable=R0904

# Disable warning Invalid variable name
# pylint: disable=C0103


LOGGER = logging.getLogger(__name__)

# PARAMS_URL_TEMPLATE = 'amqp://guest:guest@127.0.0.1:%(port)s/%%2f?socket_timeout=1'
PARAMS_URL_TEMPLATE = 'amqp://guest:guest@127.0.0.1:%(port)s/?socket_timeout=1'
DEFAULT_URL = PARAMS_URL_TEMPLATE % {'port': 5672}
DEFAULT_PARAMS = pika.URLParameters(DEFAULT_URL)
DEFAULT_TIMEOUT = 15


def setUpModule():
    logging.basicConfig(level=logging.DEBUG)


class BlockingTestCaseBase(AsyncTestCase):
    TIMEOUT = DEFAULT_TIMEOUT

    def __init__(self, methodName='runTest'):
        super(BlockingTestCaseBase, self).__init__(methodName)
        self._cleanup_coros = []

    def tearDown(self):
        self.io_loop.run_sync(self.doCoroCleanups)
        super(BlockingTestCaseBase, self).tearDown()

    @coroutine
    def _connect(self,
                 url=DEFAULT_URL,
                 connection_class=topika.Connection):
        connection = yield topika.connect(
            url,
            loop=None,
            connection_class=connection_class)
        self.addCoroCleanup(lambda: (yield connection.close()) if connection.is_open else None)

        @coroutine
        def clean():
            if connection.is_open:
                yield connection.close()

        self.addCoroCleanup(clean)

        # We use impl's timer directly in order to get a callback regardless
        # of BlockingConnection's event dispatch modality
        connection.loop.call_later(self.TIMEOUT, self._on_test_timeout)

        # Patch calls into I/O loop to fail test if exceptions are
        # leaked back through SelectConnection or the I/O loop.
        # self._instrument_io_loop_exception_leak_detection(connection)
        raise Return(connection)

    def _instrument_io_loop_exception_leak_detection(self, connection):
        """Instrument the given connection to detect and fail test when
        an exception is leaked through the I/O loop

        NOTE: BlockingConnection's underlying asynchronous connection adapter
        (SelectConnection) uses callbacks to communicate with its user (
        BlockingConnection in this case). If BlockingConnection leaks
        exceptions back into the I/O loop or the asynchronous connection
        adapter, we interrupt their normal workflow and introduce a high
        likelihood of state inconsistency.
        """
        # Patch calls into I/O loop to fail test if exceptions are
        # leaked back through SelectConnection or the I/O loop.
        real_poll = connection._connection.ioloop.poll

        def my_poll(*args, **kwargs):
            try:
                return real_poll(*args, **kwargs)
            except BaseException as exc:
                self.fail('Unwanted exception leaked into asynchronous layer '
                          'via ioloop.poll(): {!r}'.format(exc))

        connection._connection.ioloop.poll = my_poll
        self.addCleanup(setattr, connection._connection.ioloop, 'poll', real_poll)

        real_process_timeouts = connection._connection.ioloop.process_timeouts

        def my_process_timeouts(*args, **kwargs):
            try:
                return real_process_timeouts(*args, **kwargs)
            except AssertionError:
                # Our test timeout logic and unit test assert* routines rely
                # on being able to pass AssertionError
                raise
            except BaseException as exc:
                self.fail('Unwanted exception leaked into asynchronous layer '
                          'via ioloop.process_timeouts(): {!r}'.format(exc))

        connection._connection.ioloop.process_timeouts = my_process_timeouts
        # self.addCleanup(setattr, connection._connection.ioloop, 'process_timeouts',
        # real_process_timeouts)

    def _on_test_timeout(self):
        """Called when test times out"""
        LOGGER.info('%s TIMED OUT (%s)', datetime.utcnow(), self)
        self.stop()
        self.fail('Test timed out')

    @retry_assertion(TIMEOUT / 2)
    @coroutine
    def _assert_exact_message_count_with_retries(self,
                                                 channel,
                                                 queue,
                                                 expected_count):
        frame = yield channel.queue_declare(queue, passive=True)
        self.assertEqual(frame.method.message_count, expected_count)

    def addCleanup(self, coro_or_func, *args, **kwargs):
        if tornado.gen.is_coroutine_function(coro_or_func):
            self.addCoroCleanup(coro_or_func, *args, **kwargs)
        else:
            super(BlockingTestCaseBase, self).addCleanup(coro_or_func, *args, **kwargs)

    def addCoroCleanup(self, coro, *args, **kwargs):
        coro_ = topika.common.ensure_coroutine(coro)
        self._cleanup_coros.append((coro_, args, kwargs))

    @coroutine
    def doCoroCleanups(self):
        """Execute all cleanup functions. Normally called for you during
        tearDown."""
        try:
            result = self._resultForDoCleanups  # Python 2
        except AttributeError:
            result = self._outcome  # Python 3
        ok = True
        while self._cleanup_coros:
            coro, args, kwargs = self._cleanup_coros.pop(-1)
            try:
                yield coro(*args, **kwargs)
            except KeyboardInterrupt:
                raise
            except:
                ok = False
                result.addError(self, sys.exc_info())
        raise Return(ok)


class TestCreateAndCloseConnection(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingConnection: Create and close connection"""
        connection = yield self._connect()
        self.assertIsInstance(connection, topika.Connection)
        self.assertTrue(connection.is_open)
        self.assertFalse(connection.is_closed)

        yield connection.close()
        self.assertTrue(connection.is_closed)
        self.assertFalse(connection.is_open)


class TestMultiCloseConnection(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingConnection: Close connection twice"""
        connection = yield self._connect()
        self.assertIsInstance(connection, topika.Connection)
        self.assertTrue(connection.is_open)
        self.assertFalse(connection.is_closed)

        yield connection.close()
        self.assertTrue(connection.is_closed)
        self.assertFalse(connection.is_open)

        # Second close call shouldn't crash
        connection.close()


@unittest.skip("Need to find solution for python 3.5+")
class TestConnectionContextManagerClosesConnection(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingConnection: connection context manager closes connection"""
        conn = yield self._connect()
        with (yield conn) as connection:
            self.assertIsInstance(connection, topika.Connection)
            self.assertTrue(connection.is_open)

        self.assertTrue(connection.is_closed)


#
# class TestConnectionContextManagerClosesConnectionAndPassesOriginalException(BlockingTestCaseBase):
#     def test(self):
#         """BlockingConnection: connection context manager closes connection and passes original exception"""  # pylint: disable=C0301
#         class MyException(Exception):
#             pass
#
#         with self.assertRaises(MyException):
#             with self._connect() as connection:
#                 self.assertTrue(connection.is_open)
#
#                 raise MyException()
#
#         self.assertTrue(connection.is_closed)
#
#
# class TestConnectionContextManagerClosesConnectionAndPassesSystemException(BlockingTestCaseBase):
#     def test(self):
#         """BlockingConnection: connection context manager closes connection and passes system exception"""  # pylint: disable=C0301
#         with self.assertRaises(SystemExit):
#             with self._connect() as connection:
#                 self.assertTrue(connection.is_open)
#                 raise SystemExit()
#
#         self.assertTrue(connection.is_closed)
#

@unittest.skip("Looks like TornadoConnection does not have a .socket")
class TestLostConnectionResultsInIsClosedConnectionAndChannel(BlockingTestCaseBase):
    @gen_test
    def test(self):
        connection = yield self._connect()
        channel = yield connection.channel()

        # Simulate the server dropping the socket connection
        connection._connection.socket.shutdown(socket.SHUT_RDWR)

        with self.assertRaises(pika.exceptions.ChannelClosed):
            # Changing QoS should result in ChannelClosed
            yield channel.basic_qos()

        # Now check is_open/is_closed on channel and connection
        self.assertFalse(channel.is_open)
        self.assertTrue(channel.is_closed)
        self.assertFalse(connection.is_open)
        self.assertTrue(connection.is_closed)


class TestInvalidExchangeTypeRaisesChannelClosed(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingConnection: ConnectionClosed raised when creating exchange with invalid type"""  # pylint: disable=C0301
        # This test exploits behavior specific to RabbitMQ whereby the broker
        # closes the connection if an attempt is made to declare an exchange
        # with an invalid exchange type
        connection = yield self._connect()
        ch = yield connection.channel()

        exg_name = ("TestInvalidExchangeTypeRaisesConnectionClosed_" +
                    uuid.uuid1().hex)

        with self.assertRaises(pika.exceptions.ConnectionClosedByBroker) as ex_cm:
            # Attempt to create an exchange with invalid exchange type
            yield ch.exchange_declare(exg_name, exchange_type='ZZwwInvalid')

        self.assertEqual(ex_cm.exception.args[0], 503)


@unittest.skip("Need to look at")
class TestCreateAndCloseConnectionWithChannelAndConsumer(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingConnection: Create and close connection with channel and consumer"""  # pylint: disable=C0301
        connection = yield self._connect()

        ch = yield connection.channel()

        q_name = (
                'TestCreateAndCloseConnectionWithChannelAndConsumer_q' +
                uuid.uuid1().hex)

        body1 = 'a' * 1024

        # Declare a new queue
        yield ch.queue_declare(q_name, auto_delete=True)
        self.addCoroCleanup(lambda: (yield (yield self._connect()).channel()).queue_delete(q_name))

        # Publish the message to the queue by way of default exchange
        yield ch.publish(exchange='', routing_key=q_name, body=body1)

        # Create a consumer that uses automatic ack mode
        ch.basic_consume(q_name, lambda *x: None, auto_ack=True,
                         exclusive=False, arguments=None)

        yield connection.close()
        self.assertTrue(connection.is_closed)
        self.assertFalse(connection.is_open)

        self.assertFalse(connection._connection._channels)

        self.assertFalse(ch._consumer_infos)
        self.assertFalse(ch._impl._consumers)


class TestSuddenBrokerDisconnectBeforeChannel(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingConnection resets properly on TCP/IP drop during channel()
        """
        with ForwardServer(
                remote_addr=(DEFAULT_PARAMS.host, DEFAULT_PARAMS.port),
                local_linger_args=(1, 0)) as fwd:
            self.connection = yield self._connect(
                PARAMS_URL_TEMPLATE % {"port": fwd.server_address[1]})

        # Once outside the context, the connection is broken

        # BlockingConnection should raise StreamLostError
        with self.assertRaises(pika.exceptions.StreamLostError):
            yield self.connection.channel()

        self.assertTrue(self.connection.is_closed)
        self.assertFalse(self.connection.is_open)


class TestNoAccessToFileDescriptorAfterConnectionClosed(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingConnection no access file descriptor after ConnectionClosed
        """
        with ForwardServer(
                remote_addr=(DEFAULT_PARAMS.host, DEFAULT_PARAMS.port),
                local_linger_args=(1, 0)) as fwd:
            self.connection = yield self._connect(
                PARAMS_URL_TEMPLATE % {"port": fwd.server_address[1]})

        # Once outside the context, the connection is broken

        # BlockingConnection should raise StreamLostError
        with self.assertRaises(pika.exceptions.StreamLostError):
            yield self.connection.channel()

        self.assertTrue(self.connection.is_closed)
        self.assertFalse(self.connection.is_open)

        # Attempt to operate on the connection once again after ConnectionClosed
        with self.assertRaises(RuntimeError):
            yield self.connection.channel()


class TestConnectWithDownedBroker(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """ BlockingConnection to downed broker results in AMQPConnectionError

        """
        # Reserve a port for use in connect
        sock = socket.socket()
        self.addCleanup(sock.close)

        sock.bind(("127.0.0.1", 0))

        port = sock.getsockname()[1]

        sock.close()

        with self.assertRaises(pika.exceptions.AMQPConnectionError):
            self.connection = yield self._connect(PARAMS_URL_TEMPLATE % {"port": port})


# class TestDisconnectDuringConnectionStart(BlockingTestCaseBase):
#     @gen_test
#     def test(self):
#         """ BlockingConnection TCP/IP connection loss in CONNECTION_START
#         """
#         fwd = ForwardServer(
#             remote_addr=(DEFAULT_PARAMS.host, DEFAULT_PARAMS.port),
#             local_linger_args=(1, 0))
#
#         fwd.start()
#         #self.addCleanup(lambda: fwd.stop() if fwd.running else None)
#
#         class MySelectConnection(pika.SelectConnection):
#             assert hasattr(pika.SelectConnection, '_on_connection_start')
#
#             def _on_connection_start(self, *args, **kwargs):
#                 fwd.stop()
#                 return super(MySelectConnection, self)._on_connection_start(
#                     *args, **kwargs)
#
#         with self.assertRaises(pika.exceptions.ProbableAuthenticationError):
#             yield self._connect(
#                 PARAMS_URL_TEMPLATE % {"port": fwd.server_address[1]},
#                 impl_class=MySelectConnection)


# class TestDisconnectDuringConnectionTune(BlockingTestCaseBase):
#
#     def test(self):
#         """ BlockingConnection TCP/IP connection loss in CONNECTION_TUNE
#         """
#         fwd = ForwardServer(
#             remote_addr=(DEFAULT_PARAMS.host, DEFAULT_PARAMS.port),
#             local_linger_args=(1, 0))
#         fwd.start()
#         #self.addCleanup(lambda: fwd.stop() if fwd.running else None)
#
#         class MySelectConnection(pika.SelectConnection):
#             assert hasattr(pika.SelectConnection, '_on_connection_tune')
#
#             def _on_connection_tune(self, *args, **kwargs):
#                 fwd.stop()
#                 return super(MySelectConnection, self)._on_connection_tune(
#                     *args, **kwargs)
#
#         with self.assertRaises(pika.exceptions.ProbableAccessDeniedError):
#             self._connect(
#                 PARAMS_URL_TEMPLATE % {"port": fwd.server_address[1]},
#                 impl_class=MySelectConnection)
#
#
# class TestDisconnectDuringConnectionProtocol(BlockingTestCaseBase):
#     @gen_test
#     def test(self):
#         """ BlockingConnection TCP/IP connection loss in CONNECTION_PROTOCOL
#         """
#         fwd = ForwardServer(
#             remote_addr=(DEFAULT_PARAMS.host, DEFAULT_PARAMS.port),
#             local_linger_args=(1, 0))
#
#         fwd.start()
#         #self.addCleanup(lambda: fwd.stop() if fwd.running else None)
#
#         class MySelectConnection(pika.SelectConnection):
#             assert hasattr(pika.SelectConnection, '_on_connected')
#
#             def _on_connected(self, *args, **kwargs):
#                 fwd.stop()
#                 return super(MySelectConnection, self)._on_connected(
#                     *args, **kwargs)
#
#         with self.assertRaises(pika.exceptions.IncompatibleProtocolError):
#             self._connect(PARAMS_URL_TEMPLATE % {"port": fwd.server_address[1]},
#                           impl_class=MySelectConnection)
#

# class TestProcessDataEvents(BlockingTestCaseBase):
#
#     def test(self):
#         """BlockingConnection.process_data_events"""
#         connection = yield self._connect()
#
#         # Try with time_limit=0
#         start_time = time.time()
#         connection.process_data_events(time_limit=0)
#         elapsed = time.time() - start_time
#         self.assertLess(elapsed, 0.25)
#
#         # Try with time_limit=0.005
#         start_time = time.time()
#         connection.process_data_events(time_limit=0.005)
#         elapsed = time.time() - start_time
#         self.assertGreaterEqual(elapsed, 0.005)
#         self.assertLess(elapsed, 0.25)
#
#
class TestConnectionRegisterForBlockAndUnblock(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingConnection register for Connection.Blocked/Unblocked"""
        connection = yield self._connect()

        # NOTE: I haven't figured out yet how to coerce RabbitMQ to emit
        # Connection.Block and Connection.Unblock from the test, so we'll
        # just call the registration functions for now and simulate incoming
        # blocked/unblocked frames

        blocked_buffer = Future()
        connection.add_on_connection_blocked_callback(
            lambda conn, frame: blocked_buffer.set_result((conn, frame)))
        # Simulate dispatch of blocked connection
        blocked_frame = pika.frame.Method(
            0,
            pika.spec.Connection.Blocked('reason'))
        connection._connection._process_frame(blocked_frame)
        yield blocked_buffer
        conn, frame = blocked_buffer.result()
        self.assertIs(conn, connection)
        self.assertIs(frame, blocked_frame)

        unblocked_buffer = Future()
        connection.add_on_connection_unblocked_callback(
            lambda conn, frame: unblocked_buffer.set_result((conn, frame)))
        # Simulate dispatch of unblocked connection
        unblocked_frame = pika.frame.Method(0, pika.spec.Connection.Unblocked())
        connection._connection._process_frame(unblocked_frame)
        yield unblocked_buffer
        conn, frame = unblocked_buffer.result()
        self.assertIs(conn, connection)
        self.assertIs(frame, unblocked_frame)


@unittest.skip("Connection class doesn't pass on the blocked_connection_timeout part of the query")
class TestBlockedConnectionTimeout(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingConnection Connection.Blocked timeout """
        url = DEFAULT_URL + '&blocked_connection_timeout=0.001'
        conn = yield self._connect(url=url)

        # NOTE: I haven't figured out yet how to coerce RabbitMQ to emit
        # Connection.Block and Connection.Unblock from the test, so we'll
        # simulate it for now

        # Simulate Connection.Blocked
        conn._connection._on_connection_blocked(
            conn._connection,
            pika.frame.Method(0, pika.spec.Connection.Blocked('TestBlockedConnectionTimeout')))

        # Wait for connection teardown
        conn_closed = Future()
        conn.add_on_close_callback(lambda _, reason: conn_closed.set_result(reason))
        yield conn_closed

        self.assertEqual(
            conn_closed.result(),
            (pika.connection.InternalCloseReasons.BLOCKED_CONNECTION_TIMEOUT,
             'Blocked connection timeout expired'))


class TestAddCallbackThreadsafeFromSameThread(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingConnection.add_callback from same thread"""
        connection = yield self._connect()
        loop = connection.loop

        # Test timer completion
        start_time = time.time()
        rx_callback = Future()
        loop.add_callback(lambda: rx_callback.set_result(time.time()))

        yield rx_callback

        elapsed = time.time() - start_time
        self.assertLess(elapsed, 0.25)


class TestAddCallbackThreadsafeFromAnotherThread(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingConnection.add_callbackfrom another thread"""
        connection = yield self._connect()
        loop = connection.loop

        # Test timer completion
        start_time = time.time()
        rx_callback = Future()
        timer = threading.Timer(
            0,
            functools.partial(loop.add_callback,
                              lambda: rx_callback.set_result(time.time())))
        self.addCleanup(timer.cancel)
        timer.start()
        yield rx_callback

        elapsed = time.time() - start_time
        self.assertLess(elapsed, 0.25)


class TestAddTimeoutRemoveTimeout(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingConnection.add_timeout and remove_timeout"""
        connection = yield self._connect()
        loop = connection.loop

        # Test timer completion
        start_time = time.time()
        rx_callback = Future()
        timer_id = loop.call_later(
            0.005,
            lambda: rx_callback.set_result(time.time()))
        yield rx_callback

        elapsed = time.time() - start_time
        self.assertLess(elapsed, 0.25)

        # Test removing triggered timeout
        loop.remove_timeout(timer_id)

        # Test aborted timer
        rx_callback = Future()
        timer_id = loop.call_later(
            0.001,
            lambda: rx_callback.set_result(time.time()))
        loop.remove_timeout(timer_id)
        try:
            yield tornado.gen.with_timeout(timedelta(seconds=0.1), rx_callback)
        except tornado.gen.TimeoutError:
            pass
        self.assertFalse(rx_callback.done())


class TestViabilityOfMultipleTimeoutsWithSameDeadlineAndCallback(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """TornadoConnection viability of multiple timeouts with same deadline and callback"""
        connection = yield self._connect()
        loop = connection.loop

        rx_callback = Future()

        def callback():
            rx_callback.set_result(1)

        timer1 = loop.call_later(0, callback)
        timer2 = loop.call_later(0, callback)

        self.assertIsNot(timer1, timer2)

        loop.remove_timeout(timer1)

        # Wait for second timer to fire
        start_wait_time = time.time()
        yield tornado.gen.with_timeout(timedelta(seconds=0.25), rx_callback)

        self.assertEqual(rx_callback.result(), 1)


class TestRemoveTimeoutFromTimeoutCallback(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingConnection.remove_timeout from timeout callback"""
        connection = yield self._connect()
        loop = connection.loop

        # Test timer completion
        timer_id1 = loop.call_later(5, lambda: 0 / 0)

        rx_timer2 = Future()

        def on_timer2():
            loop.remove_timeout(timer_id1)
            loop.remove_timeout(timer_id2)
            rx_timer2.set_result(1)

        timer_id2 = loop.call_later(0, on_timer2)

        yield rx_timer2

        self.assertIsNone(timer_id1.callback)
        self.assertIsNone(timer_id2.callback)


class TestConnectionProperties(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """Test BlockingConnection properties"""
        connection = yield self._connect()

        self.assertTrue(connection.is_open)
        self.assertFalse(connection.is_closed)

        self.assertTrue(connection.basic_nack_supported)
        self.assertTrue(connection.consumer_cancel_notify_supported)
        self.assertTrue(connection.exchange_exchange_bindings_supported)
        self.assertTrue(connection.publisher_confirms_supported)

        yield connection.close()
        self.assertFalse(connection.is_open)
        self.assertTrue(connection.is_closed)


class TestCreateAndCloseChannel(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingChannel: Create and close channel"""
        connection = yield self._connect()

        ch = yield connection.channel()
        self.assertIsInstance(ch, topika.Channel)
        self.assertTrue(ch.is_open)
        self.assertFalse(ch.is_closed)
        self.assertIs(ch.connection, connection)

        yield ch.close()
        self.assertTrue(ch.is_closed)
        self.assertFalse(ch.is_open)


class TestExchangeDeclareAndDelete(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingChannel: Test exchange_declare and exchange_delete"""
        connection = yield self._connect()

        ch = yield connection.channel()

        name = "TestExchangeDeclareAndDelete_" + uuid.uuid1().hex

        # Declare a new exchange
        frame = yield ch.exchange_declare(name, exchange_type='direct')
        self.addCoroCleanup(lambda: (yield (yield connection.channel()).exchange_delete(name)))

        self.assertIsInstance(frame.method, pika.spec.Exchange.DeclareOk)

        # Check if it exists by declaring it passively
        frame = yield ch.exchange_declare(name, passive=True)
        self.assertIsInstance(frame.method, pika.spec.Exchange.DeclareOk)

        # Delete the exchange
        frame = yield ch.exchange_delete(name)
        self.assertIsInstance(frame.method, pika.spec.Exchange.DeleteOk)

        # Verify that it's been deleted
        with self.assertRaises(pika.exceptions.ChannelClosed) as cm:
            yield ch.exchange_declare(name, passive=True)

        self.assertEqual(cm.exception.args[0], 404)


class TestExchangeBindAndUnbind(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingChannel: Test exchange_bind and exchange_unbind"""
        connection = yield self._connect()

        ch = yield connection.channel()

        q_name = 'TestExchangeBindAndUnbind_q' + uuid.uuid1().hex
        src_exg_name = 'TestExchangeBindAndUnbind_src_exg_' + uuid.uuid1().hex
        dest_exg_name = 'TestExchangeBindAndUnbind_dest_exg_' + uuid.uuid1().hex
        routing_key = 'TestExchangeBindAndUnbind'

        # Place channel in publisher-acknowledgments mode so that we may test
        # whether the queue is reachable by publishing with mandatory=True
        res = yield ch.confirm_delivery()
        self.assertIsNone(res)

        # Declare both exchanges
        yield ch.exchange_declare(src_exg_name, exchange_type='direct')
        self.addCoroCleanup(lambda: (yield (yield connection.channel()).exchange_delete(src_exg_name)))
        yield ch.exchange_declare(dest_exg_name, exchange_type='direct')
        self.addCoroCleanup(lambda: (yield (yield connection.channel()).exchange_delete(dest_exg_name)))

        # Declare a new queue
        yield ch.queue_declare(q_name, auto_delete=True)
        self.addCoroCleanup(lambda: (yield (yield self._connect()).channel()).queue_delete(q_name))

        # Bind the queue to the destination exchange
        yield ch.queue_bind(q_name, exchange=dest_exg_name, routing_key=routing_key)

        # Verify that the queue is unreachable without exchange-exchange binding
        with self.assertRaises(pika.exceptions.UnroutableError):
            yield ch.publish(src_exg_name, routing_key, body='', mandatory=True)

        # Bind the exchanges
        frame = yield ch.exchange_bind(destination=dest_exg_name, source=src_exg_name,
                                       routing_key=routing_key)
        self.assertIsInstance(frame.method, pika.spec.Exchange.BindOk)

        # Publish a message via the source exchange
        yield ch.publish(src_exg_name, routing_key, body='TestExchangeBindAndUnbind',
                         mandatory=True)

        # Check that the queue now has one message
        yield self._assert_exact_message_count_with_retries(channel=ch,
                                                            queue=q_name,
                                                            expected_count=1)

        # Unbind the exchanges
        frame = yield ch.exchange_unbind(destination=dest_exg_name,
                                         source=src_exg_name,
                                         routing_key=routing_key)
        self.assertIsInstance(frame.method, pika.spec.Exchange.UnbindOk)

        # Verify that the queue is now unreachable via the source exchange
        with self.assertRaises(pika.exceptions.UnroutableError):
            yield ch.publish(src_exg_name, routing_key, body='', mandatory=True)


class TestQueueDeclareAndDelete(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingChannel: Test queue_declare and queue_delete"""
        connection = yield self._connect()

        ch = yield connection.channel()

        q_name = 'TestQueueDeclareAndDelete_' + uuid.uuid1().hex

        # Declare a new queue
        frame = yield ch.queue_declare(q_name, auto_delete=True)
        self.addCoroCleanup(lambda: (yield (yield (yield self._connect()).channel()).queue_delete(q_name)))

        self.assertIsInstance(frame.method, pika.spec.Queue.DeclareOk)

        # Check if it exists by declaring it passively
        frame = yield ch.queue_declare(q_name, passive=True)
        self.assertIsInstance(frame.method, pika.spec.Queue.DeclareOk)

        # Delete the queue
        frame = yield ch.queue_delete(q_name)
        self.assertIsInstance(frame.method, pika.spec.Queue.DeleteOk)

        # Verify that it's been deleted
        with self.assertRaises(pika.exceptions.ChannelClosed) as cm:
            yield ch.queue_declare(q_name, passive=True)

        self.assertEqual(cm.exception.args[0], 404)


class TestPassiveQueueDeclareOfUnknownQueueRaisesChannelClosed(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingChannel: ChannelClosed raised when passive-declaring unknown queue"""  # pylint: disable=C0301
        connection = yield self._connect()
        ch = yield connection.channel()

        q_name = ("TestPassiveQueueDeclareOfUnknownQueueRaisesChannelClosed_q_"
                  + uuid.uuid1().hex)

        with self.assertRaises(pika.exceptions.ChannelClosed) as ex_cm:
            yield ch.queue_declare(q_name, passive=True)

        self.assertEqual(ex_cm.exception.args[0], 404)


class TestQueueBindAndUnbindAndPurge(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingChannel: Test queue_bind and queue_unbind"""
        connection = yield self._connect()

        ch = yield connection.channel()

        q_name = 'TestQueueBindAndUnbindAndPurge_q' + uuid.uuid1().hex
        exg_name = 'TestQueueBindAndUnbindAndPurge_exg_' + uuid.uuid1().hex
        routing_key = 'TestQueueBindAndUnbindAndPurge'

        # Place channel in publisher-acknowledgments mode so that we may test
        # whether the queue is reachable by publishing with mandatory=True
        res = yield ch.confirm_delivery()
        self.assertIsNone(res)

        # Declare a new exchange
        yield ch.exchange_declare(exg_name, exchange_type='direct')
        self.addCoroCleanup(lambda: (yield (yield connection.channel()).exchange_delete(exg_name)))

        # Declare a new queue
        yield ch.queue_declare(q_name, auto_delete=True)
        self.addCoroCleanup(lambda: (yield (yield (yield self._connect()).channel()).queue_delete(q_name)))

        # Bind the queue to the exchange using routing key
        frame = yield ch.queue_bind(q_name, exchange=exg_name,
                                    routing_key=routing_key)
        self.assertIsInstance(frame.method, pika.spec.Queue.BindOk)

        # Check that the queue is empty
        frame = yield ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 0)

        # Deposit a message in the queue
        yield ch.publish(exg_name, routing_key, body='TestQueueBindAndUnbindAndPurge',
                         mandatory=True)

        # Check that the queue now has one message
        frame = yield ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 1)

        # Unbind the queue
        frame = yield ch.queue_unbind(queue=q_name, exchange=exg_name,
                                      routing_key=routing_key)
        self.assertIsInstance(frame.method, pika.spec.Queue.UnbindOk)

        # Verify that the queue is now unreachable via that binding
        with self.assertRaises(pika.exceptions.UnroutableError):
            yield ch.publish(exg_name, routing_key,
                             body='TestQueueBindAndUnbindAndPurge-2',
                             mandatory=True)

        # Purge the queue and verify that 1 message was purged
        frame = yield ch.queue_purge(q_name)
        self.assertIsInstance(frame.method, pika.spec.Queue.PurgeOk)
        self.assertEqual(frame.method.message_count, 1)

        # Verify that the queue is now empty
        frame = yield ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 0)


class TestBasicGet(BlockingTestCaseBase):

    def tearDown(self):
        LOGGER.info('%s TEARING DOWN (%s)', datetime.utcnow(), self)

    @gen_test
    def test(self):
        """BlockingChannel.basic_get"""
        LOGGER.info('%s STARTED (%s)', datetime.utcnow(), self)

        connection = yield self._connect()
        LOGGER.info('%s CONNECTED (%s)', datetime.utcnow(), self)

        ch = yield connection.channel()
        LOGGER.info('%s CREATED CHANNEL (%s)', datetime.utcnow(), self)

        q_name = 'TestBasicGet_q' + uuid.uuid1().hex

        # Place channel in publisher-acknowledgments mode so that the message
        # may be delivered synchronously to the queue by publishing it with
        # mandatory=True
        yield ch.confirm_delivery()
        LOGGER.info('%s ENABLED PUB-ACKS (%s)', datetime.utcnow(), self)

        # Declare a new queue
        yield ch.queue_declare(q_name, auto_delete=True)
        self.addCoroCleanup(lambda: (yield (yield (yield self._connect()).channel()).queue_delete(q_name)))
        LOGGER.info('%s DECLARED QUEUE (%s)', datetime.utcnow(), self)

        # Verify result of getting a message from an empty queue
        msg = yield ch.basic_get(q_name, auto_ack=False)
        self.assertTupleEqual(msg, (None, None, None))
        LOGGER.info('%s GOT FROM EMPTY QUEUE (%s)', datetime.utcnow(), self)

        body = 'TestBasicGet'
        # Deposit a message in the queue via default exchange
        yield ch.publish(exchange='', routing_key=q_name,
                         body=body,
                         mandatory=True)
        LOGGER.info('%s PUBLISHED (%s)', datetime.utcnow(), self)

        # Get the message
        (method, properties, body) = yield ch.basic_get(q_name, auto_ack=False)
        LOGGER.info('%s GOT FROM NON-EMPTY QUEUE (%s)', datetime.utcnow(), self)
        self.assertIsInstance(method, pika.spec.Basic.GetOk)
        self.assertEqual(method.delivery_tag, 1)
        self.assertFalse(method.redelivered)
        self.assertEqual(method.exchange, '')
        self.assertEqual(method.routing_key, q_name)
        self.assertEqual(method.message_count, 0)

        self.assertIsInstance(properties, pika.BasicProperties)
        self.assertIsNone(properties.headers)
        self.assertEqual(body, as_bytes(body))

        # Ack it
        yield ch.basic_ack(delivery_tag=method.delivery_tag)
        LOGGER.info('%s ACKED (%s)', datetime.utcnow(), self)

        # Verify that the queue is now empty
        self._assert_exact_message_count_with_retries(channel=ch,
                                                      queue=q_name,
                                                      expected_count=0)


class TestBasicReject(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingChannel.basic_reject"""
        connection = yield self._connect()

        ch = yield connection.channel()

        q_name = 'TestBasicReject_q' + uuid.uuid1().hex

        # Place channel in publisher-acknowledgments mode so that the message
        # may be delivered synchronously to the queue by publishing it with
        # mandatory=True
        yield ch.confirm_delivery()

        # Declare a new queue
        yield ch.queue_declare(q_name, auto_delete=True)
        self.addCoroCleanup(lambda: (yield (yield (yield self._connect()).channel()).queue_delete(q_name)))

        # Deposit two messages in the queue via default exchange
        yield ch.publish(exchange='', routing_key=q_name,
                         body='TestBasicReject1',
                         mandatory=True)
        yield ch.publish(exchange='', routing_key=q_name,
                         body='TestBasicReject2',
                         mandatory=True)

        # Get the messages
        (rx_method, _, rx_body) = yield ch.basic_get(q_name, auto_ack=False)
        self.assertEqual(rx_body, as_bytes('TestBasicReject1'))

        (rx_method, _, rx_body) = yield ch.basic_get(q_name, auto_ack=False)
        self.assertEqual(rx_body, as_bytes('TestBasicReject2'))

        # Nack the second message
        yield ch.basic_reject(rx_method.delivery_tag, requeue=True)

        # Verify that exactly one message is present in the queue, namely the
        # second one
        self._assert_exact_message_count_with_retries(channel=ch,
                                                      queue=q_name,
                                                      expected_count=1)
        (rx_method, _, rx_body) = yield ch.basic_get(q_name, auto_ack=False)
        self.assertEqual(rx_body, as_bytes('TestBasicReject2'))


class TestBasicRejectNoRequeue(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingChannel.basic_reject with requeue=False"""
        connection = yield self._connect()

        ch = yield connection.channel()

        q_name = 'TestBasicRejectNoRequeue_q' + uuid.uuid1().hex

        # Place channel in publisher-acknowledgments mode so that the message
        # may be delivered synchronously to the queue by publishing it with
        # mandatory=True
        yield ch.confirm_delivery()

        # Declare a new queue
        yield ch.queue_declare(q_name, auto_delete=True)
        self.addCoroCleanup(lambda: (yield (yield (yield self._connect()).channel()).queue_delete(q_name)))

        # Deposit two messages in the queue via default exchange
        yield ch.publish(exchange='', routing_key=q_name,
                         body='TestBasicRejectNoRequeue1',
                         mandatory=True)
        yield ch.publish(exchange='', routing_key=q_name,
                         body='TestBasicRejectNoRequeue2',
                         mandatory=True)

        # Get the messages
        (rx_method, _, rx_body) = yield ch.basic_get(q_name, auto_ack=False)
        self.assertEqual(rx_body,
                         as_bytes('TestBasicRejectNoRequeue1'))

        (rx_method, _, rx_body) = yield ch.basic_get(q_name, auto_ack=False)
        self.assertEqual(rx_body,
                         as_bytes('TestBasicRejectNoRequeue2'))

        # Nack the second message
        yield ch.basic_reject(rx_method.delivery_tag, requeue=False)

        # Verify that no messages are present in the queue
        self._assert_exact_message_count_with_retries(channel=ch,
                                                      queue=q_name,
                                                      expected_count=0)


class TestBasicNack(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingChannel.basic_nack single message"""
        connection = yield self._connect()

        ch = yield connection.channel()

        q_name = 'TestBasicNack_q' + uuid.uuid1().hex

        # Place channel in publisher-acknowledgments mode so that the message
        # may be delivered synchronously to the queue by publishing it with
        # mandatory=True
        yield ch.confirm_delivery()

        # Declare a new queue
        yield ch.queue_declare(q_name, auto_delete=True)
        self.addCoroCleanup(lambda: (yield (yield (yield self._connect()).channel()).queue_delete(q_name)))

        # Deposit two messages in the queue via default exchange
        yield ch.publish(exchange='', routing_key=q_name,
                         body='TestBasicNack1',
                         mandatory=True)
        yield ch.publish(exchange='', routing_key=q_name,
                         body='TestBasicNack2',
                         mandatory=True)

        # Get the messages
        (rx_method, _, rx_body) = yield ch.basic_get(q_name, auto_ack=False)
        self.assertEqual(rx_body, as_bytes('TestBasicNack1'))

        (rx_method, _, rx_body) = yield ch.basic_get(q_name, auto_ack=False)
        self.assertEqual(rx_body, as_bytes('TestBasicNack2'))

        # Nack the second message
        yield ch.basic_nack(rx_method.delivery_tag, multiple=False, requeue=True)

        # Verify that exactly one message is present in the queue, namely the
        # second one
        self._assert_exact_message_count_with_retries(channel=ch,
                                                      queue=q_name,
                                                      expected_count=1)
        (rx_method, _, rx_body) = yield ch.basic_get(q_name, auto_ack=False)
        self.assertEqual(rx_body, as_bytes('TestBasicNack2'))


class TestBasicNackNoRequeue(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingChannel.basic_nack with requeue=False"""
        connection = yield self._connect()

        ch = yield connection.channel()

        q_name = 'TestBasicNackNoRequeue_q' + uuid.uuid1().hex

        # Place channel in publisher-acknowledgments mode so that the message
        # may be delivered synchronously to the queue by publishing it with
        # mandatory=True
        yield ch.confirm_delivery()

        # Declare a new queue
        yield ch.queue_declare(q_name, auto_delete=True)
        self.addCoroCleanup(lambda: (yield (yield (yield self._connect()).channel()).queue_delete(q_name)))

        # Deposit two messages in the queue via default exchange
        yield ch.publish(exchange='', routing_key=q_name,
                         body='TestBasicNackNoRequeue1',
                         mandatory=True)
        yield ch.publish(exchange='', routing_key=q_name,
                         body='TestBasicNackNoRequeue2',
                         mandatory=True)

        # Get the messages
        (rx_method, _, rx_body) = yield ch.basic_get(q_name, auto_ack=False)
        self.assertEqual(rx_body,
                         as_bytes('TestBasicNackNoRequeue1'))

        (rx_method, _, rx_body) = yield ch.basic_get(q_name, auto_ack=False)
        self.assertEqual(rx_body,
                         as_bytes('TestBasicNackNoRequeue2'))

        # Nack the second message
        yield ch.basic_nack(rx_method.delivery_tag, requeue=False)

        # Verify that no messages are present in the queue
        self._assert_exact_message_count_with_retries(channel=ch,
                                                      queue=q_name,
                                                      expected_count=0)


class TestBasicNackMultiple(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingChannel.basic_nack multiple messages"""
        connection = yield self._connect()

        ch = yield connection.channel()

        q_name = 'TestBasicNackMultiple_q' + uuid.uuid1().hex

        # Place channel in publisher-acknowledgments mode so that the message
        # may be delivered synchronously to the queue by publishing it with
        # mandatory=True
        yield ch.confirm_delivery()

        # Declare a new queue
        yield ch.queue_declare(q_name, auto_delete=True)
        self.addCoroCleanup(lambda: (yield (yield (yield self._connect()).channel()).queue_delete(q_name)))

        # Deposit two messages in the queue via default exchange
        yield ch.publish(exchange='', routing_key=q_name,
                         body='TestBasicNackMultiple1',
                         mandatory=True)
        yield ch.publish(exchange='', routing_key=q_name,
                         body='TestBasicNackMultiple2',
                         mandatory=True)

        # Get the messages
        (rx_method, _, rx_body) = yield ch.basic_get(q_name, auto_ack=False)
        self.assertEqual(rx_body,
                         as_bytes('TestBasicNackMultiple1'))

        (rx_method, _, rx_body) = yield ch.basic_get(q_name, auto_ack=False)
        self.assertEqual(rx_body,
                         as_bytes('TestBasicNackMultiple2'))

        # Nack both messages via the "multiple" option
        ch.basic_nack(rx_method.delivery_tag, multiple=True, requeue=True)

        # Verify that both messages are present in the queue
        self._assert_exact_message_count_with_retries(channel=ch,
                                                      queue=q_name,
                                                      expected_count=2)
        (rx_method, _, rx_body) = yield ch.basic_get(q_name, auto_ack=False)
        self.assertEqual(rx_body,
                         as_bytes('TestBasicNackMultiple1'))
        (rx_method, _, rx_body) = yield ch.basic_get(q_name, auto_ack=False)
        self.assertEqual(rx_body,
                         as_bytes('TestBasicNackMultiple2'))


@unittest.skip("Doesn't make sense for event based implementation")
class TestBasicRecoverWithRequeue(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingChannel.basic_recover with requeue=True.

        NOTE: the requeue=False option is not supported by RabbitMQ broker as
        of this writing (using RabbitMQ 3.5.1)
        """
        connection = yield self._connect()

        ch = yield connection.channel()

        q_name = (
                'TestBasicRecoverWithRequeue_q' + uuid.uuid1().hex)

        # Place channel in publisher-acknowledgments mode so that the message
        # may be delivered synchronously to the queue by publishing it with
        # mandatory=True
        yield ch.confirm_delivery()

        # Declare a new queue
        yield ch.queue_declare(q_name, auto_delete=True)
        self.addCoroCleanup(lambda: (yield (yield (yield self._connect()).channel()).queue_delete(q_name)))

        # Deposit two messages in the queue via default exchange
        yield ch.publish(exchange='', routing_key=q_name,
                         body='TestBasicRecoverWithRequeue1',
                         mandatory=True)
        yield ch.publish(exchange='', routing_key=q_name,
                         body='TestBasicRecoverWithRequeue2',
                         mandatory=True)

        rx_messages = []
        num_messages = 0
        for msg in ch.consume(q_name, auto_ack=False):
            num_messages += 1

            if num_messages == 2:
                ch.basic_recover(requeue=True)

            if num_messages > 2:
                rx_messages.append(msg)

            if num_messages == 4:
                break
        else:
            self.fail('consumer aborted prematurely')

        # Get the messages
        (_, _, rx_body) = rx_messages[0]
        self.assertEqual(rx_body,
                         as_bytes('TestBasicRecoverWithRequeue1'))

        (_, _, rx_body) = rx_messages[1]
        self.assertEqual(rx_body,
                         as_bytes('TestBasicRecoverWithRequeue2'))


class TestTxCommit(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingChannel.tx_commit"""
        connection = yield self._connect()

        ch = yield connection.channel()

        q_name = 'TestTxCommit_q' + uuid.uuid1().hex

        # Declare a new queue
        yield ch.queue_declare(q_name, auto_delete=True)
        self.addCoroCleanup(lambda: (yield (yield (yield self._connect()).channel()).queue_delete(q_name)))

        # Select standard transaction mode
        frame = yield ch.tx_select()
        self.assertIsInstance(frame.method, pika.spec.Tx.SelectOk)

        # Deposit a message in the queue via default exchange
        yield ch.publish(exchange='', routing_key=q_name,
                         body='TestTxCommit1',
                         mandatory=True)

        # Verify that queue is still empty
        frame = yield ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 0)

        # Commit the transaction
        ch.tx_commit()

        # Verify that the queue has the expected message
        frame = yield ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 1)

        (_, _, rx_body) = yield ch.basic_get(q_name, auto_ack=False)
        self.assertEqual(rx_body, as_bytes('TestTxCommit1'))


class TestTxRollback(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingChannel.tx_commit"""
        connection = yield self._connect()

        ch = yield connection.channel()

        q_name = 'TestTxRollback_q' + uuid.uuid1().hex

        # Declare a new queue
        yield ch.queue_declare(q_name, auto_delete=True)
        self.addCoroCleanup(lambda: (yield (yield (yield self._connect()).channel()).queue_delete(q_name)))

        # Select standard transaction mode
        frame = yield ch.tx_select()
        self.assertIsInstance(frame.method, pika.spec.Tx.SelectOk)

        # Deposit a message in the queue via default exchange
        yield ch.publish(exchange='', routing_key=q_name,
                         body='TestTxRollback1',
                         mandatory=True)

        # Verify that queue is still empty
        frame = yield ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 0)

        # Roll back the transaction
        ch.tx_rollback()

        # Verify that the queue continues to be empty
        frame = yield ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 0)


class TestBasicConsumeFromUnknownQueueRaisesChannelClosed(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """ChannelClosed raised when consuming from unknown queue"""
        connection = yield self._connect()
        ch = yield connection.channel()

        q_name = ("TestBasicConsumeFromUnknownQueueRaisesChannelClosed_q_" +
                  uuid.uuid1().hex)

        with self.assertRaises(pika.exceptions.ChannelClosed) as ex_cm:
            yield ch.basic_consume(q_name, lambda *args: None)

        self.assertEqual(ex_cm.exception.args[0], 404)


class TestPublishAndBasicPublishWithPubacksUnroutable(BlockingTestCaseBase):
    @gen_test
    def test(self):  # pylint: disable=R0914
        """BlockingChannel.publish amd basic_publish unroutable message with pubacks"""  # pylint: disable=C0301
        connection = yield self._connect()

        ch = yield connection.channel()

        exg_name = ('TestPublishAndBasicPublishUnroutable_exg_' +
                    uuid.uuid1().hex)
        routing_key = 'TestPublishAndBasicPublishUnroutable'

        # Place channel in publisher-acknowledgments mode so that publishing
        # with mandatory=True will be synchronous
        res = yield ch.confirm_delivery()
        self.assertIsNone(res)

        # Declare a new exchange
        yield ch.exchange_declare(exg_name, exchange_type='direct')
        self.addCoroCleanup(lambda: (yield (yield (connection.channel()).exchange_delete(exg_name))))

        # Verify unroutable message handling using basic_publish
        res = yield ch.basic_publish(exg_name, routing_key=routing_key, body='',
                                     mandatory=True)
        self.assertEqual(res, False)

        # Verify unroutable message handling using publish
        msg2_headers = dict(
            test_name='TestPublishAndBasicPublishWithPubacksUnroutable')
        msg2_properties = pika.spec.BasicProperties(headers=msg2_headers)
        with self.assertRaises(pika.exceptions.UnroutableError) as cm:
            yield ch.publish(exg_name, routing_key=routing_key, body='',
                             properties=msg2_properties, mandatory=True)
        (msg,) = cm.exception.messages
        self.assertIsInstance(msg, topika.channel.ReturnedMessage)
        self.assertIsInstance(msg.method, pika.spec.Basic.Return)
        self.assertEqual(msg.method.reply_code, 312)
        self.assertEqual(msg.method.exchange, exg_name)
        self.assertEqual(msg.method.routing_key, routing_key)
        self.assertIsInstance(msg.properties, pika.BasicProperties)
        self.assertEqual(msg.properties.headers, msg2_headers)
        self.assertEqual(msg.body, as_bytes(''))


class TestConfirmDeliveryAfterUnroutableMessage(BlockingTestCaseBase):
    @gen_test
    def test(self):  # pylint: disable=R0914
        """BlockingChannel.confirm_delivery following unroutable message"""
        connection = yield self._connect()

        ch = yield connection.channel()

        exg_name = ('TestConfirmDeliveryAfterUnroutableMessage_exg_' +
                    uuid.uuid1().hex)
        routing_key = 'TestConfirmDeliveryAfterUnroutableMessage'

        # Declare a new exchange
        yield ch.exchange_declare(exg_name, exchange_type='direct')
        self.addCoroCleanup(lambda: (yield (yield connection.channel()).exchange_delete(exg_name)))

        # Register on-return callback
        returned_messages = []
        ch.add_on_return_callback(lambda *args: returned_messages.append(args))

        # Emit unroutable message without pubacks
        res = yield ch.basic_publish(exg_name, routing_key=routing_key, body='',
                                     mandatory=True)
        self.assertEqual(res, True)

        # Select delivery confirmations
        yield ch.confirm_delivery()

        # Verify that unroutable message was dispatched
        ((channel, method, properties, body,),) = returned_messages
        self.assertIs(channel, ch)
        self.assertIsInstance(method, pika.spec.Basic.Return)
        self.assertEqual(method.reply_code, 312)
        self.assertEqual(method.exchange, exg_name)
        self.assertEqual(method.routing_key, routing_key)
        self.assertIsInstance(properties, pika.BasicProperties)
        self.assertEqual(body, as_bytes(''))


class TestUnroutableMessagesReturnedInNonPubackMode(BlockingTestCaseBase):
    @gen_test
    def test(self):  # pylint: disable=R0914
        """BlockingChannel: unroutable messages is returned in non-puback mode"""  # pylint: disable=C0301
        connection = yield self._connect()

        ch = yield connection.channel()

        exg_name = (
                'TestUnroutableMessageReturnedInNonPubackMode_exg_'
                + uuid.uuid1().hex)
        routing_key = 'TestUnroutableMessageReturnedInNonPubackMode'

        # Declare a new exchange
        yield ch.exchange_declare(exg_name, exchange_type='direct')
        self.addCoroCleanup(lambda: (yield (yield connection.channel()).exchange_delete(exg_name)))

        # Register on-return callback
        all_returned = Future()
        returned_messages = []
        num_expected = 2

        def message_returned(*args):
            returned_messages.append(args)
            if len(returned_messages) == num_expected:
                all_returned.set_result(True)

        ch.add_on_return_callback(message_returned)

        # Emit unroutable messages without pubacks
        yield ch.publish(exg_name, routing_key=routing_key, body='msg1',
                         mandatory=True)

        yield ch.publish(exg_name, routing_key=routing_key, body='msg2',
                         mandatory=True)

        # Process I/O until Basic.Return are dispatched
        yield all_returned

        # Verify returned messages
        (channel, method, properties, body,) = returned_messages[0]
        self.assertIs(channel, ch)
        self.assertIsInstance(method, pika.spec.Basic.Return)
        self.assertEqual(method.reply_code, 312)
        self.assertEqual(method.exchange, exg_name)
        self.assertEqual(method.routing_key, routing_key)
        self.assertIsInstance(properties, pika.BasicProperties)
        self.assertEqual(body, as_bytes('msg1'))

        (channel, method, properties, body,) = returned_messages[1]
        self.assertIs(channel, ch)
        self.assertIsInstance(method, pika.spec.Basic.Return)
        self.assertEqual(method.reply_code, 312)
        self.assertEqual(method.exchange, exg_name)
        self.assertEqual(method.routing_key, routing_key)
        self.assertIsInstance(properties, pika.BasicProperties)
        self.assertEqual(body, as_bytes('msg2'))


class TestUnroutableMessageReturnedInPubackMode(BlockingTestCaseBase):
    @gen_test
    def test(self):  # pylint: disable=R0914
        """BlockingChannel: unroutable messages is returned in puback mode"""
        connection = yield self._connect()

        ch = yield connection.channel()

        exg_name = (
                'TestUnroutableMessageReturnedInPubackMode_exg_'
                + uuid.uuid1().hex)
        routing_key = 'TestUnroutableMessageReturnedInPubackMode'

        # Declare a new exchange
        yield ch.exchange_declare(exg_name, exchange_type='direct')
        self.addCoroCleanup(lambda: (yield (yield connection.channel()).exchange_delete(exg_name)))

        # Select delivery confirmations
        yield ch.confirm_delivery()

        # Register on-return callback
        all_returned = Future()
        returned_messages = []
        num_expected = 2

        def message_returned(*args):
            returned_messages.append(args)
            if len(returned_messages) == num_expected:
                all_returned.set_result(True)

        ch.add_on_return_callback(message_returned)

        # Emit unroutable messages with pubacks
        res = yield ch.basic_publish(exg_name, routing_key=routing_key, body='msg1',
                                     mandatory=True)
        self.assertEqual(res, False)

        res = yield ch.basic_publish(exg_name, routing_key=routing_key, body='msg2',
                                     mandatory=True)
        self.assertEqual(res, False)

        # Verify returned messages
        (channel, method, properties, body,) = returned_messages[0]
        self.assertIs(channel, ch)
        self.assertIsInstance(method, pika.spec.Basic.Return)
        self.assertEqual(method.reply_code, 312)
        self.assertEqual(method.exchange, exg_name)
        self.assertEqual(method.routing_key, routing_key)
        self.assertIsInstance(properties, pika.BasicProperties)
        self.assertEqual(body, as_bytes('msg1'))

        (channel, method, properties, body,) = returned_messages[1]
        self.assertIs(channel, ch)
        self.assertIsInstance(method, pika.spec.Basic.Return)
        self.assertEqual(method.reply_code, 312)
        self.assertEqual(method.exchange, exg_name)
        self.assertEqual(method.routing_key, routing_key)
        self.assertIsInstance(properties, pika.BasicProperties)
        self.assertEqual(body, as_bytes('msg2'))


class TestBasicPublishDeliveredWhenPendingUnroutable(BlockingTestCaseBase):
    @gen_test
    def test(self):  # pylint: disable=R0914
        """BlockingChannel.basic_publish msg delivered despite pending unroutable message"""  # pylint: disable=C0301
        connection = yield self._connect()

        ch = yield connection.channel()

        q_name = ('TestBasicPublishDeliveredWhenPendingUnroutable_q' +
                  uuid.uuid1().hex)
        exg_name = ('TestBasicPublishDeliveredWhenPendingUnroutable_exg_' +
                    uuid.uuid1().hex)
        routing_key = 'TestBasicPublishDeliveredWhenPendingUnroutable'

        # Declare a new exchange
        yield ch.exchange_declare(exg_name, exchange_type='direct')
        self.addCoroCleanup(lambda: (yield (yield connection.channel()).exchange_delete(exg_name)))

        # Declare a new queue
        yield ch.queue_declare(q_name, auto_delete=True)
        self.addCoroCleanup(lambda: (yield (yield (yield self._connect()).channel()).queue_delete(q_name)))

        # Bind the queue to the exchange using routing key
        ch.queue_bind(q_name, exchange=exg_name, routing_key=routing_key)

        # Attempt to send an unroutable message in the queue via basic_publish
        res = yield ch.basic_publish(exg_name, routing_key='',
                                     body='unroutable-message',
                                     mandatory=True)
        self.assertEqual(res, True)

        # Flush connection to force Basic.Return
        yield (yield connection.channel()).close()

        # Deposit a routable message in the queue
        res = yield ch.basic_publish(exg_name, routing_key=routing_key,
                                     body='routable-message',
                                     mandatory=True)
        self.assertEqual(res, True)

        # Wait for the queue to get the routable message
        self._assert_exact_message_count_with_retries(channel=ch,
                                                      queue=q_name,
                                                      expected_count=1)

        msg = yield ch.basic_get(q_name)

        # Check the first message
        self.assertIsInstance(msg, tuple)
        rx_method, rx_properties, rx_body = msg
        self.assertIsInstance(rx_method, pika.spec.Basic.GetOk)
        self.assertEqual(rx_method.delivery_tag, 1)
        self.assertFalse(rx_method.redelivered)
        self.assertEqual(rx_method.exchange, exg_name)
        self.assertEqual(rx_method.routing_key, routing_key)

        self.assertIsInstance(rx_properties, pika.BasicProperties)
        self.assertEqual(rx_body, as_bytes('routable-message'))

        # Ack the message
        yield ch.basic_ack(delivery_tag=rx_method.delivery_tag, multiple=False)

        # Verify that the queue is now empty
        self._assert_exact_message_count_with_retries(channel=ch,
                                                      queue=q_name,
                                                      expected_count=0)


class TestPublishAndConsumeWithPubacksAndQosOfOne(BlockingTestCaseBase):
    @gen_test
    def test(self):  # pylint: disable=R0914,R0915
        """BlockingChannel.basic_publish, publish, basic_consume, QoS, \
        Basic.Cancel from broker
        """
        connection = yield self._connect()

        ch = yield connection.channel()

        q_name = 'TestPublishAndConsumeAndQos_q' + uuid.uuid1().hex
        exg_name = 'TestPublishAndConsumeAndQos_exg_' + uuid.uuid1().hex
        routing_key = 'TestPublishAndConsumeAndQos'

        # Place channel in publisher-acknowledgments mode so that publishing
        # with mandatory=True will be synchronous
        res = yield ch.confirm_delivery()
        self.assertIsNone(res)

        # Declare a new exchange
        yield ch.exchange_declare(exg_name, exchange_type='direct')
        self.addCoroCleanup(lambda: (yield (yield connection.channel()).exchange_delete(exg_name)))

        # Declare a new queue
        yield ch.queue_declare(q_name, auto_delete=True)
        self.addCoroCleanup(lambda: (yield (yield (yield self._connect()).channel()).queue_delete(q_name)))

        # Bind the queue to the exchange using routing key
        yield ch.queue_bind(q_name, exchange=exg_name, routing_key=routing_key)

        # Deposit a message in the queue via basic_publish
        msg1_headers = dict(
            test_name='TestPublishAndConsumeWithPubacksAndQosOfOne')
        msg1_properties = pika.spec.BasicProperties(headers=msg1_headers)
        res = yield ch.basic_publish(exg_name, routing_key=routing_key,
                                     body='via-basic_publish',
                                     properties=msg1_properties,
                                     mandatory=True)
        self.assertEqual(res, True)

        # Deposit another message in the queue via publish
        yield ch.publish(exg_name, routing_key, body='via-publish',
                         mandatory=True)

        # Check that the queue now has two messages
        frame = yield ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 2)

        # Configure QoS for one message
        yield ch.basic_qos(prefetch_size=0, prefetch_count=1, all_channels=False)

        # Create a consumer
        rx_messages = []
        consumer_tag = yield ch.basic_consume(
            q_name,
            lambda *args: rx_messages.append(args),
            auto_ack=False,
            exclusive=False,
            arguments=None)

        # Wait for first message to arrive
        while not rx_messages:
            yield

        self.assertEqual(len(rx_messages), 1)

        # Check the first message
        msg = rx_messages[0]
        self.assertIsInstance(msg, tuple)
        rx_ch, rx_method, rx_properties, rx_body = msg
        self.assertIs(rx_ch, ch)
        self.assertIsInstance(rx_method, pika.spec.Basic.Deliver)
        self.assertEqual(rx_method.consumer_tag, consumer_tag)
        self.assertEqual(rx_method.delivery_tag, 1)
        self.assertFalse(rx_method.redelivered)
        self.assertEqual(rx_method.exchange, exg_name)
        self.assertEqual(rx_method.routing_key, routing_key)

        self.assertIsInstance(rx_properties, pika.BasicProperties)
        self.assertEqual(rx_properties.headers, msg1_headers)
        self.assertEqual(rx_body, as_bytes('via-basic_publish'))

        # Ack the message so that the next one can arrive (we configured QoS
        # with prefetch_count=1)
        ch.basic_ack(delivery_tag=rx_method.delivery_tag, multiple=False)

        # Get the second message
        while len(rx_messages) < 2:
            yield

        self.assertEqual(len(rx_messages), 2)

        msg = rx_messages[1]
        self.assertIsInstance(msg, tuple)
        rx_ch, rx_method, rx_properties, rx_body = msg
        self.assertIs(rx_ch, ch)
        self.assertIsInstance(rx_method, pika.spec.Basic.Deliver)
        self.assertEqual(rx_method.consumer_tag, consumer_tag)
        self.assertEqual(rx_method.delivery_tag, 2)
        self.assertFalse(rx_method.redelivered)
        self.assertEqual(rx_method.exchange, exg_name)
        self.assertEqual(rx_method.routing_key, routing_key)

        self.assertIsInstance(rx_properties, pika.BasicProperties)
        self.assertEqual(rx_body, as_bytes('via-publish'))

        ch.basic_ack(delivery_tag=rx_method.delivery_tag, multiple=False)

        # Verify that the queue is now empty
        self._assert_exact_message_count_with_retries(channel=ch,
                                                      queue=q_name,
                                                      expected_count=0)

        # Attempt to consume again with a short timeout
        yield tornado.gen.sleep(0.005)
        self.assertEqual(len(rx_messages), 2)

        # Delete the queue and wait for consumer cancellation
        rx_cancellations = []
        ch.add_on_cancel_callback(rx_cancellations.append)
        yield ch.queue_delete(q_name)

        self.assertEqual(len(rx_cancellations), 1)
        frame, = rx_cancellations
        self.assertEqual(frame.method.consumer_tag, consumer_tag)


class TestBasicConsumeWithAckFromAnotherThread(BlockingTestCaseBase):
    @gen_test
    def test(self):  # pylint: disable=R0914,R0915
        """BlockingChannel.basic_consume with ack from another thread and \
        requesting basic_ack via add_callback
        """
        # This test simulates processing of a message on another thread and
        # then requesting an ACK to be dispatched on the connection's thread
        # via BlockingConnection.add_callback

        connection = yield self._connect()
        loop = connection.loop

        ch = yield connection.channel()

        q_name = 'TestBasicConsumeWithAckFromAnotherThread_q' + uuid.uuid1().hex
        exg_name = ('TestBasicConsumeWithAckFromAnotherThread_exg' +
                    uuid.uuid1().hex)
        routing_key = 'TestBasicConsumeWithAckFromAnotherThread'

        # Place channel in publisher-acknowledgments mode so that publishing
        # with mandatory=True will be synchronous (for convenience)
        res = yield ch.confirm_delivery()
        self.assertIsNone(res)

        # Declare a new exchange
        yield ch.exchange_declare(exg_name, exchange_type='direct')
        self.addCoroCleanup(lambda: (yield (yield connection.channel()).exchange_delete(exg_name)))

        # Declare a new queue
        yield ch.queue_declare(q_name, auto_delete=True)
        self.addCoroCleanup(lambda: (yield (yield (yield self._connect()).channel()).queue_delete(q_name)))

        # Bind the queue to the exchange using routing key
        ch.queue_bind(q_name, exchange=exg_name, routing_key=routing_key)

        # Publish 2 messages with mandatory=True for synchronous processing
        yield ch.publish(exg_name, routing_key, body='msg1', mandatory=True)
        yield ch.publish(exg_name, routing_key, body='last-msg', mandatory=True)

        # Configure QoS for one message so that the 2nd message will be
        # delivered only after the 1st one is ACKed
        yield ch.basic_qos(prefetch_size=0, prefetch_count=1, all_channels=False)

        # Create a consumer
        rx_messages = []
        consumer_cancelled = Future()

        @coroutine
        def ackAndEnqueueMessageViaAnotherThread(rx_ch,
                                                 rx_method,
                                                 rx_properties,  # pylint: disable=W0613
                                                 rx_body):
            LOGGER.debug(
                '%s: Got message body=%r; delivery-tag=%r',
                datetime.now(), rx_body, rx_method.delivery_tag)

            # Request ACK dispatch via add_callback from other
            # thread; if last message, cancel consumer so that start_consuming
            # can return

            @coroutine
            def processOnConnectionThread():
                LOGGER.debug('%s: ACKing message body=%r; delivery-tag=%r',
                             datetime.now(),
                             rx_body,
                             rx_method.delivery_tag)
                yield ch.basic_ack(delivery_tag=rx_method.delivery_tag,
                                   multiple=False)
                rx_messages.append(rx_body)

                # NOTE on python3, `b'last-msg' != 'last-msg'`
                if rx_body == b'last-msg':
                    LOGGER.debug('%s: Canceling consumer consumer-tag=%r',
                                 datetime.now(),
                                 rx_method.consumer_tag)
                    yield rx_ch.basic_cancel(rx_method.consumer_tag)
                    consumer_cancelled.set_result(True)

            # Spawn a thread to initiate ACKing
            timer = threading.Timer(0,
                                    lambda: loop.add_callback(
                                        processOnConnectionThread))
            self.addCleanup(timer.cancel)
            timer.start()

        consumer_tag = yield ch.basic_consume(
            q_name,
            ackAndEnqueueMessageViaAnotherThread,
            auto_ack=False,
            exclusive=False,
            arguments=None)

        # Wait for both messages
        LOGGER.debug('%s: calling start_consuming(); consumer tag=%r',
                     datetime.now(),
                     consumer_tag)
        yield consumer_cancelled
        LOGGER.debug('%s: Returned from start_consuming(); consumer tag=%r',
                     datetime.now(),
                     consumer_tag)

        self.assertEqual(len(rx_messages), 2)
        self.assertEqual(rx_messages[0], b'msg1')
        self.assertEqual(rx_messages[1], b'last-msg')


@unittest.skip("Doesn't make sense for event based implementation")
class TestConsumeGeneratorWithAckFromAnotherThread(BlockingTestCaseBase):
    @gen_test
    def test(self):  # pylint: disable=R0914,R0915
        """BlockingChannel.consume and requesting basic_ack from another \
        thread via add_callback
        """
        connection = yield self._connect()
        loop = connection.loop

        ch = yield connection.channel()

        q_name = ('TestConsumeGeneratorWithAckFromAnotherThread_q' +
                  uuid.uuid1().hex)
        exg_name = ('TestConsumeGeneratorWithAckFromAnotherThread_exg' +
                    uuid.uuid1().hex)
        routing_key = 'TestConsumeGeneratorWithAckFromAnotherThread'

        # Place channel in publisher-acknowledgments mode so that publishing
        # with mandatory=True will be synchronous (for convenience)
        res = yield ch.confirm_delivery()
        self.assertIsNone(res)

        # Declare a new exchange
        yield ch.exchange_declare(exg_name, exchange_type='direct')
        self.addCoroCleanup(lambda: (yield (yield connection.channel()).exchange_delete(exg_name)))

        # Declare a new queue
        yield ch.queue_declare(q_name, auto_delete=True)
        self.addCoroCleanup(lambda: (yield (yield (yield self._connect()).channel()).queue_delete(q_name)))

        # Bind the queue to the exchange using routing key
        ch.queue_bind(q_name, exchange=exg_name, routing_key=routing_key)

        # Publish 2 messages with mandatory=True for synchronous processing
        yield ch.publish(exg_name, routing_key, body='msg1', mandatory=True)
        yield ch.publish(exg_name, routing_key, body='last-msg', mandatory=True)

        # Configure QoS for one message so that the 2nd message will be
        # delivered only after the 1st one is ACKed
        yield ch.basic_qos(prefetch_size=0, prefetch_count=1, all_channels=False)

        # Create a consumer
        rx_messages = []

        def ackAndEnqueueMessageViaAnotherThread(rx_ch,
                                                 rx_method,
                                                 rx_properties,  # pylint: disable=W0613
                                                 rx_body):
            LOGGER.debug(
                '%s: Got message body=%r; delivery-tag=%r',
                datetime.now(), rx_body, rx_method.delivery_tag)

            # Request ACK dispatch via add_callback from other
            # thread; if last message, cancel consumer so that consumer
            # generator completes

            def processOnConnectionThread():
                LOGGER.debug('%s: ACKing message body=%r; delivery-tag=%r',
                             datetime.now(),
                             rx_body,
                             rx_method.delivery_tag)
                ch.basic_ack(delivery_tag=rx_method.delivery_tag,
                             multiple=False)
                rx_messages.append(rx_body)

                # NOTE on python3, `b'last-msg' != 'last-msg'`
                if rx_body == b'last-msg':
                    LOGGER.debug('%s: Canceling consumer consumer-tag=%r',
                                 datetime.now(),
                                 rx_method.consumer_tag)
                    # NOTE Need to use cancel() for the consumer generator
                    # instead of basic_cancel()
                    rx_ch.cancel()

            # Spawn a thread to initiate ACKing
            timer = threading.Timer(0,
                                    lambda: loop.add_callback(
                                        processOnConnectionThread))
            self.addCleanup(timer.cancel)
            timer.start()

        for method, properties, body in ch.consume(q_name, auto_ack=False):
            ackAndEnqueueMessageViaAnotherThread(rx_ch=ch,
                                                 rx_method=method,
                                                 rx_properties=properties,
                                                 rx_body=body)

        self.assertEqual(len(rx_messages), 2)
        self.assertEqual(rx_messages[0], b'msg1')
        self.assertEqual(rx_messages[1], b'last-msg')


class TestTwoBasicConsumersOnSameChannel(BlockingTestCaseBase):
    @gen_test
    def test(self):  # pylint: disable=R0914
        """BlockingChannel: two basic_consume consumers on same channel
        """
        connection = yield self._connect()

        ch = yield connection.channel()

        exg_name = 'TestPublishAndConsumeAndQos_exg_' + uuid.uuid1().hex
        q1_name = 'TestTwoBasicConsumersOnSameChannel_q1' + uuid.uuid1().hex
        q2_name = 'TestTwoBasicConsumersOnSameChannel_q2' + uuid.uuid1().hex
        q1_routing_key = 'TestTwoBasicConsumersOnSameChannel1'
        q2_routing_key = 'TestTwoBasicConsumersOnSameChannel2'

        # Place channel in publisher-acknowledgments mode so that publishing
        # with mandatory=True will be synchronous
        yield ch.confirm_delivery()

        # Declare a new exchange
        yield ch.exchange_declare(exg_name, exchange_type='direct')
        self.addCoroCleanup(lambda: (yield (yield connection.channel()).exchange_delete(exg_name)))

        # Declare the two new queues and bind them to the exchange
        yield ch.queue_declare(q1_name, auto_delete=True)
        self.addCoroCleanup(lambda: (yield (yield (yield self._connect()).channel()).queue_delete(q1_name)))
        ch.queue_bind(q1_name, exchange=exg_name, routing_key=q1_routing_key)

        yield ch.queue_declare(q2_name, auto_delete=True)
        self.addCoroCleanup(lambda: (yield (yield (yield self._connect()).channel()).queue_delete(q2_name)))
        ch.queue_bind(q2_name, exchange=exg_name, routing_key=q2_routing_key)

        # Deposit messages in the queues
        q1_tx_message_bodies = ['q1_message+%s' % (i,)
                                for i in pika.compat.xrange(100)]
        for message_body in q1_tx_message_bodies:
            yield ch.publish(exg_name, q1_routing_key, body=message_body,
                             mandatory=True)

        q2_tx_message_bodies = ['q2_message+%s' % (i,)
                                for i in pika.compat.xrange(150)]
        for message_body in q2_tx_message_bodies:
            yield ch.publish(exg_name, q2_routing_key, body=message_body,
                             mandatory=True)

        # Create the consumers
        q1_rx_messages = []
        q1_consumer_tag = yield ch.basic_consume(
            q1_name,
            lambda *args: q1_rx_messages.append(args),
            auto_ack=False,
            exclusive=False,
            arguments=None)

        q2_rx_messages = []
        q2_consumer_tag = yield ch.basic_consume(
            q2_name,
            lambda *args: q2_rx_messages.append(args),
            auto_ack=False,
            exclusive=False,
            arguments=None)

        # Wait for all messages to be delivered
        while (len(q1_rx_messages) < len(q1_tx_message_bodies) or
               len(q2_rx_messages) < len(q2_tx_message_bodies)):
            yield

        self.assertEqual(len(q2_rx_messages), len(q2_tx_message_bodies))

        # Verify the messages
        def validate_messages(rx_messages,
                              routing_key,
                              consumer_tag,
                              tx_message_bodies):
            self.assertEqual(len(rx_messages), len(tx_message_bodies))

            for msg, expected_body in zip(rx_messages, tx_message_bodies):
                self.assertIsInstance(msg, tuple)
                rx_ch, rx_method, rx_properties, rx_body = msg
                self.assertIs(rx_ch, ch)
                self.assertIsInstance(rx_method, pika.spec.Basic.Deliver)
                self.assertEqual(rx_method.consumer_tag, consumer_tag)
                self.assertFalse(rx_method.redelivered)
                self.assertEqual(rx_method.exchange, exg_name)
                self.assertEqual(rx_method.routing_key, routing_key)

                self.assertIsInstance(rx_properties, pika.BasicProperties)
                self.assertEqual(rx_body, as_bytes(expected_body))

        # Validate q1 consumed messages
        validate_messages(rx_messages=q1_rx_messages,
                          routing_key=q1_routing_key,
                          consumer_tag=q1_consumer_tag,
                          tx_message_bodies=q1_tx_message_bodies)

        # Validate q2 consumed messages
        validate_messages(rx_messages=q2_rx_messages,
                          routing_key=q2_routing_key,
                          consumer_tag=q2_consumer_tag,
                          tx_message_bodies=q2_tx_message_bodies)


class TestBasicPublishWithoutPubacks(BlockingTestCaseBase):
    @gen_test
    def test(self):  # pylint: disable=R0914,R0915
        """BlockingChannel.basic_publish without pubacks"""
        connection = yield self._connect()

        ch = yield connection.channel()

        q_name = 'TestBasicPublishWithoutPubacks_q' + uuid.uuid1().hex
        exg_name = 'TestBasicPublishWithoutPubacks_exg_' + uuid.uuid1().hex
        routing_key = 'TestBasicPublishWithoutPubacks'

        # Declare a new exchange
        yield ch.exchange_declare(exg_name, exchange_type='direct')
        self.addCoroCleanup(lambda: (yield (yield connection.channel()).exchange_delete(exg_name)))

        # Declare a new queue
        yield ch.queue_declare(q_name, auto_delete=True)
        self.addCoroCleanup(lambda: (yield (yield (yield self._connect()).channel()).queue_delete(q_name)))

        # Bind the queue to the exchange using routing key
        ch.queue_bind(q_name, exchange=exg_name, routing_key=routing_key)

        # Deposit a message in the queue via basic_publish and mandatory=True
        msg1_headers = dict(
            test_name='TestBasicPublishWithoutPubacks')
        msg1_properties = pika.spec.BasicProperties(headers=msg1_headers)
        res = yield ch.basic_publish(exg_name, routing_key=routing_key,
                                     body='via-basic_publish_mandatory=True',
                                     properties=msg1_properties,
                                     mandatory=True)
        self.assertEqual(res, True)

        # Deposit a message in the queue via basic_publish and mandatory=False
        res = yield ch.basic_publish(exg_name, routing_key=routing_key,
                                     body='via-basic_publish_mandatory=False',
                                     mandatory=False)
        self.assertEqual(res, True)

        # Wait for the messages to arrive in queue
        self._assert_exact_message_count_with_retries(channel=ch,
                                                      queue=q_name,
                                                      expected_count=2)

        # Create a consumer. Not passing a 'callback' to test client-generated
        # consumer tags
        rx_messages = []
        consumer_tag = yield ch.basic_consume(
            q_name,
            lambda *args: rx_messages.append(args),
            auto_ack=False,
            exclusive=False,
            arguments=None)

        # Wait for first message to arrive
        while not rx_messages:
            yield  # Give the connection time to consume

        self.assertGreaterEqual(len(rx_messages), 1)

        # Check the first message
        msg = rx_messages[0]
        self.assertIsInstance(msg, tuple)
        rx_ch, rx_method, rx_properties, rx_body = msg
        self.assertIs(rx_ch, ch)
        self.assertIsInstance(rx_method, pika.spec.Basic.Deliver)
        self.assertEqual(rx_method.consumer_tag, consumer_tag)
        self.assertEqual(rx_method.delivery_tag, 1)
        self.assertFalse(rx_method.redelivered)
        self.assertEqual(rx_method.exchange, exg_name)
        self.assertEqual(rx_method.routing_key, routing_key)

        self.assertIsInstance(rx_properties, pika.BasicProperties)
        self.assertEqual(rx_properties.headers, msg1_headers)
        self.assertEqual(rx_body, as_bytes('via-basic_publish_mandatory=True'))

        # Ack the message so that the next one can arrive (we configured QoS
        # with prefetch_count=1)
        yield ch.basic_ack(delivery_tag=rx_method.delivery_tag, multiple=False)

        # Get the second message
        while len(rx_messages) < 2:
            yield  # Give the connection time to consume

        self.assertEqual(len(rx_messages), 2)

        msg = rx_messages[1]
        self.assertIsInstance(msg, tuple)
        rx_ch, rx_method, rx_properties, rx_body = msg
        self.assertIs(rx_ch, ch)
        self.assertIsInstance(rx_method, pika.spec.Basic.Deliver)
        self.assertEqual(rx_method.consumer_tag, consumer_tag)
        self.assertEqual(rx_method.delivery_tag, 2)
        self.assertFalse(rx_method.redelivered)
        self.assertEqual(rx_method.exchange, exg_name)
        self.assertEqual(rx_method.routing_key, routing_key)

        self.assertIsInstance(rx_properties, pika.BasicProperties)
        self.assertEqual(rx_body, as_bytes('via-basic_publish_mandatory=False'))

        ch.basic_ack(delivery_tag=rx_method.delivery_tag, multiple=False)

        # Verify that the queue is now empty
        self._assert_exact_message_count_with_retries(channel=ch,
                                                      queue=q_name,
                                                      expected_count=0)

        # Attempt to consume again with a short timeout
        yield tornado.gen.sleep(0.005)
        self.assertEqual(len(rx_messages), 2)


@unittest.skip("Doesn't make sense for event based implementation")
class TestPublishFromBasicConsumeCallback(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingChannel.basic_publish from basic_consume callback
        """
        connection = yield self._connect()

        ch = yield connection.channel()

        src_q_name = (
                'TestPublishFromBasicConsumeCallback_src_q' + uuid.uuid1().hex)
        dest_q_name = (
                'TestPublishFromBasicConsumeCallback_dest_q' + uuid.uuid1().hex)

        # Place channel in publisher-acknowledgments mode so that publishing
        # with mandatory=True will be synchronous
        yield ch.confirm_delivery()

        # Declare source and destination queues
        yield ch.queue_declare(src_q_name, auto_delete=True)
        self.addCoroCleanup(lambda: (yield (yield (yield self._connect()).channel()).queue_delete(src_q_name)))
        yield ch.queue_declare(dest_q_name, auto_delete=True)
        self.addCoroCleanup(lambda: (yield (yield (yield self._connect()).channel()).queue_delete(dest_q_name)))

        # Deposit a message in the source queue
        yield ch.publish('',
                         routing_key=src_q_name,
                         body='via-publish',
                         mandatory=True)

        # Create a consumer
        def on_consume(channel, method, props, body):
            channel.publish(
                '', routing_key=dest_q_name, body=body,
                properties=props, mandatory=True)
            channel.basic_ack(method.delivery_tag)

        yield ch.basic_consume(src_q_name,
                               on_consume,
                               auto_ack=False,
                               exclusive=False,
                               arguments=None)

        # Consume from destination queue
        for _, _, rx_body in ch.consume(dest_q_name, auto_ack=True):
            self.assertEqual(rx_body, as_bytes('via-publish'))
            break
        else:
            self.fail('failed to consume a messages from destination q')


@unittest.skip("Doesn't make sense for event based implementation")
class TestStopConsumingFromBasicConsumeCallback(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingChannel.stop_consuming from basic_consume callback
        """
        connection = yield self._connect()

        ch = yield connection.channel()

        q_name = (
                'TestStopConsumingFromBasicConsumeCallback_q' + uuid.uuid1().hex)

        # Place channel in publisher-acknowledgments mode so that publishing
        # with mandatory=True will be synchronous
        yield ch.confirm_delivery()

        # Declare the queue
        yield ch.queue_declare(q_name, auto_delete=False)
        self.addCoroCleanup(lambda: (yield (yield connection.channel()).queue_delete(q_name)))

        # Deposit two messages in the queue
        yield ch.publish('',
                         routing_key=q_name,
                         body='via-publish1',
                         mandatory=True)

        yield ch.publish('',
                         routing_key=q_name,
                         body='via-publish2',
                         mandatory=True)

        # Create a consumer
        got_message = Future()

        @coroutine
        def on_consume(channel, method, props, body):  # pylint: disable=W0613
            yield channel.basic_ack(method.delivery_tag)
            yield channel.basic_cancel()  # Stop consuming for all consumers
            got_message.set_result(True)

        yield ch.basic_consume(q_name,
                               on_consume,
                               auto_ack=False,
                               exclusive=False,
                               arguments=None)

        yield got_message

        yield ch.close()

        ch = yield connection.channel()

        # Verify that only the second message is present in the queue
        _, _, rx_body = yield ch.basic_get(q_name)
        self.assertEqual(rx_body, as_bytes('via-publish2'))

        msg = yield ch.basic_get(q_name)
        self.assertTupleEqual(msg, (None, None, None))


@unittest.skip("Doesn't make sense for event based implementation")
class TestCloseChannelFromBasicConsumeCallback(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingChannel.close from basic_consume callback
        """
        connection = yield self._connect()

        ch = yield connection.channel()

        q_name = (
                'TestCloseChannelFromBasicConsumeCallback_q' + uuid.uuid1().hex)

        # Place channel in publisher-acknowledgments mode so that publishing
        # with mandatory=True will be synchronous
        yield ch.confirm_delivery()

        # Declare the queue
        yield ch.queue_declare(q_name, auto_delete=False)
        self.addCoroCleanup(lambda: (yield (yield (connection.channel()).queue_delete(q_name))))

        # Deposit two messages in the queue
        yield ch.publish('',
                         routing_key=q_name,
                         body='via-publish1',
                         mandatory=True)

        yield ch.publish('',
                         routing_key=q_name,
                         body='via-publish2',
                         mandatory=True)

        # Create a consumer
        def on_consume(channel, method, props, body):  # pylint: disable=W0613
            channel.close()

        yield ch.basic_consume(q_name,
                               on_consume,
                               auto_ack=False,
                               exclusive=False,
                               arguments=None)

        ch.start_consuming()

        self.assertTrue(ch.is_closed)

        # Verify that both messages are present in the queue
        ch = yield connection.channel()
        _, _, rx_body = yield ch.basic_get(q_name)
        self.assertEqual(rx_body, as_bytes('via-publish1'))
        _, _, rx_body = yield ch.basic_get(q_name)
        self.assertEqual(rx_body, as_bytes('via-publish2'))


@unittest.skip("Need to look at")
class TestCloseConnectionFromBasicConsumeCallback(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingConnection.close from basic_consume callback
        """
        connection = yield self._connect()

        ch = yield connection.channel()

        q_name = (
                'TestCloseConnectionFromBasicConsumeCallback_q' + uuid.uuid1().hex)

        # Place channel in publisher-acknowledgments mode so that publishing
        # with mandatory=True will be synchronous
        yield ch.confirm_delivery()

        # Declare the queue
        yield ch.queue_declare(q_name, auto_delete=False)
        self.addCoroCleanup(lambda: (yield (yield (yield self._connect()).channel()).queue_delete(q_name)))

        # Deposit two messages in the queue
        yield ch.publish('',
                         routing_key=q_name,
                         body='via-publish1',
                         mandatory=True)

        yield ch.publish('',
                         routing_key=q_name,
                         body='via-publish2',
                         mandatory=True)

        # Create a consumer
        def on_consume(channel, method, props, body):  # pylint: disable=W0613
            connection.close()

        ch.basic_consume(q_name,
                         on_consume,
                         auto_ack=False,
                         exclusive=False,
                         arguments=None)

        ch.start_consuming()

        self.assertTrue(ch.is_closed)
        self.assertTrue(connection.is_closed)

        # Verify that both messages are present in the queue
        ch = yield (yield self._connect()).channel()
        _, _, rx_body = yield ch.basic_get(q_name)
        self.assertEqual(rx_body, as_bytes('via-publish1'))
        _, _, rx_body = yield ch.basic_get(q_name)
        self.assertEqual(rx_body, as_bytes('via-publish2'))


@unittest.skip("Need to look at")
class TestStartConsumingRaisesChannelClosedOnSameChannelFailure(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """start_consuming() exits with ChannelClosed exception on same channel failure
        """
        connection = yield self._connect()
        loop = connection.loop

        # Fail test if exception leaks back ito I/O loop
        self._instrument_io_loop_exception_leak_detection(connection)

        ch = yield connection.channel()

        q_name = (
                'TestStartConsumingPassesChannelClosedOnSameChannelFailure_q' +
                uuid.uuid1().hex)

        # Declare the queue
        yield ch.queue_declare(q_name, auto_delete=False)
        self.addCoroCleanup(lambda: (yield (yield (yield self._connect()).channel()).queue_delete(q_name)))

        yield ch.basic_consume(q_name,
                               lambda *args, **kwargs: None,
                               auto_ack=False,
                               exclusive=False,
                               arguments=None)

        # Schedule a callback that will cause a channel error on the consumer's
        # channel by publishing to an unknown exchange. This will cause the
        # broker to close our channel.
        loop.add_callback(
            lambda: (yield ch.basic_publish(
                exchange=q_name,
                routing_key='123',
                body=b'Nope this is wrong')))

        self.assertFalse(ch._impl.is_closed_by_broker)

        channel_closed = Future()
        ch.add_on_close_callback(lambda: channel_closed.set_result(True))
        yield channel_closed

        self.assertTrue(ch._impl.is_closed_by_broker)


class TestStartConsumingReturnsAfterCancelFromBroker(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """start_consuming() returns after Cancel from broker
        """
        connection = yield self._connect()

        ch = yield connection.channel()

        q_name = (
                'TestStartConsumingExitsOnCancelFromBroker_q' + uuid.uuid1().hex)

        # Declare the queue
        yield ch.queue_declare(q_name, auto_delete=False)
        self.addCoroCleanup(lambda: (yield (yield (yield self._connect()).channel()).queue_delete(q_name)))

        consumer_tag = yield ch.basic_consume(q_name,
                                              lambda *args, **kwargs: None,
                                              auto_ack=False,
                                              exclusive=False,
                                              arguments=None)

        # Delete the queue. This will cause the broker to cancel our consumer
        yield (yield (yield self._connect()).channel()).queue_delete(q_name)

        self.assertNotIn(consumer_tag, ch._impl._consumers)


@unittest.skip('No consume')
class TestNonPubAckPublishAndConsumeHugeMessage(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingChannel.publish/consume huge message"""
        connection = yield self._connect()

        ch = yield connection.channel()

        q_name = 'TestPublishAndConsumeHugeMessage_q' + uuid.uuid1().hex
        body = 'a' * 1000000

        # Declare a new queue
        yield ch.queue_declare(q_name, auto_delete=False)
        self.addCoroCleanup(lambda: (yield (yield (yield self._connect()).channel()).queue_delete(q_name)))

        # Publish a message to the queue by way of default exchange
        yield ch.publish(exchange='', routing_key=q_name, body=body)
        LOGGER.info('Published message body size=%s', len(body))

        # Consume the message
        for rx_method, rx_props, rx_body in ch.consume(q_name, auto_ack=False,
                                                       exclusive=False,
                                                       arguments=None):
            self.assertIsInstance(rx_method, pika.spec.Basic.Deliver)
            self.assertEqual(rx_method.delivery_tag, 1)
            self.assertFalse(rx_method.redelivered)
            self.assertEqual(rx_method.exchange, '')
            self.assertEqual(rx_method.routing_key, q_name)

            self.assertIsInstance(rx_props, pika.BasicProperties)
            self.assertEqual(rx_body, as_bytes(body))

            # Ack the message
            ch.basic_ack(delivery_tag=rx_method.delivery_tag, multiple=False)

            break

        # There shouldn't be any more events now
        self.assertFalse(ch._queue_consumer_generator.pending_events)

        # Verify that the queue is now empty
        yield ch.close()
        ch = yield connection.channel()
        self._assert_exact_message_count_with_retries(channel=ch,
                                                      queue=q_name,
                                                      expected_count=0)


@unittest.skip('No consume')
class TestNonPubAckPublishAndConsumeManyMessages(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingChannel non-pub-ack publish/consume many messages"""
        connection = yield self._connect()

        ch = yield connection.channel()

        q_name = ('TestNonPubackPublishAndConsumeManyMessages_q' +
                  uuid.uuid1().hex)
        body = 'b' * 1024

        num_messages_to_publish = 500

        # Declare a new queue
        yield ch.queue_declare(q_name, auto_delete=False)
        self.addCoroCleanup(lambda: (yield (yield (yield self._connect()).channel()).queue_delete(q_name)))

        for _ in pika.compat.xrange(num_messages_to_publish):
            # Publish a message to the queue by way of default exchange
            yield ch.publish(exchange='', routing_key=q_name, body=body)

        # Consume the messages
        num_consumed = 0
        for rx_method, rx_props, rx_body in ch.consume(q_name,
                                                       auto_ack=False,
                                                       exclusive=False,
                                                       arguments=None):
            num_consumed += 1
            self.assertIsInstance(rx_method, pika.spec.Basic.Deliver)
            self.assertEqual(rx_method.delivery_tag, num_consumed)
            self.assertFalse(rx_method.redelivered)
            self.assertEqual(rx_method.exchange, '')
            self.assertEqual(rx_method.routing_key, q_name)

            self.assertIsInstance(rx_props, pika.BasicProperties)
            self.assertEqual(rx_body, as_bytes(body))

            # Ack the message
            ch.basic_ack(delivery_tag=rx_method.delivery_tag, multiple=False)

            if num_consumed >= num_messages_to_publish:
                break

        # There shouldn't be any more events now
        self.assertFalse(ch._queue_consumer_generator.pending_events)

        yield ch.close()

        self.assertIsNone(ch._queue_consumer_generator)

        # Verify that the queue is now empty
        ch = yield connection.channel()
        self._assert_exact_message_count_with_retries(channel=ch,
                                                      queue=q_name,
                                                      expected_count=0)


class TestBasicCancelWithNonAckableConsumer(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingChannel user cancels non-ackable consumer via basic_cancel"""
        connection = yield self._connect()

        ch = yield connection.channel()

        q_name = (
                'TestBasicCancelWithNonAckableConsumer_q' + uuid.uuid1().hex)

        body1 = 'a' * 1024
        body2 = 'b' * 2048

        # Declare a new queue
        yield ch.queue_declare(q_name, auto_delete=False)
        self.addCoroCleanup(lambda: (yield (yield (yield self._connect()).channel()).queue_delete(q_name)))

        # Publish two messages to the queue by way of default exchange
        yield ch.publish(exchange='', routing_key=q_name, body=body1)
        yield ch.publish(exchange='', routing_key=q_name, body=body2)

        # Wait for queue to contain both messages
        self._assert_exact_message_count_with_retries(channel=ch,
                                                      queue=q_name,
                                                      expected_count=2)

        # Create a consumer that uses automatic ack mode
        messages = []
        consumer_tag = yield ch.basic_consume(q_name, lambda *args: messages.append(args), auto_ack=True,
                                              exclusive=False, arguments=None)

        # Wait for all messages to be sent by broker to client
        self._assert_exact_message_count_with_retries(channel=ch,
                                                      queue=q_name,
                                                      expected_count=0)

        # Cancel the consumer
        yield ch.basic_cancel(consumer_tag)

        # Both messages should have been on their way when we cancelled
        self.assertEqual(len(messages), 2)

        _, _, _, rx_body1 = messages[0]
        self.assertEqual(rx_body1, as_bytes(body1))

        _, _, _, rx_body2 = messages[1]
        self.assertEqual(rx_body2, as_bytes(body2))

        yield ch.close()

        ch = yield connection.channel()

        # Verify that the queue is now empty
        frame = yield ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 0)


@unittest.skip("Don't understand this test")
class TestBasicCancelWithAckableConsumer(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingChannel user cancels ackable consumer via basic_cancel"""
        connection = yield self._connect()

        ch = yield connection.channel()

        q_name = (
                'TestBasicCancelWithAckableConsumer_q' + uuid.uuid1().hex)

        body1 = 'a' * 1024
        body2 = 'b' * 2048

        # Declare a new queue
        yield ch.queue_declare(q_name, auto_delete=False)
        self.addCoroCleanup(lambda: (yield (yield (yield self._connect()).channel()).queue_delete(q_name)))

        # Publish two messages to the queue by way of default exchange
        yield ch.publish(exchange='', routing_key=q_name, body=body1)
        yield ch.publish(exchange='', routing_key=q_name, body=body2)

        # Wait for queue to contain both messages
        self._assert_exact_message_count_with_retries(channel=ch,
                                                      queue=q_name,
                                                      expected_count=2)

        # Create an ackable consumer
        messages = []
        consumer_tag = yield ch.basic_consume(q_name, lambda *args: messages.append(args), auto_ack=False,
                                              exclusive=False, arguments=None)

        # Wait for all messages to be sent by broker to client
        self._assert_exact_message_count_with_retries(channel=ch,
                                                      queue=q_name,
                                                      expected_count=0)

        # Cancel the consumer
        yield ch.basic_cancel(consumer_tag)

        # Both messages should have been on their way when we cancelled
        self.assertEqual(0, len(messages))

        yield ch.close()

        ch = yield connection.channel()

        # Verify that canceling the ackable consumer restored both messages
        self._assert_exact_message_count_with_retries(channel=ch,
                                                      queue=q_name,
                                                      expected_count=2)


class TestUnackedMessageAutoRestoredToQueueOnChannelClose(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingChannel unacked message restored to q on channel close """
        connection = yield self._connect()

        ch = yield connection.channel()

        q_name = ('TestUnackedMessageAutoRestoredToQueueOnChannelClose_q' +
                  uuid.uuid1().hex)

        body1 = 'a' * 1024
        body2 = 'b' * 2048

        # Declare a new queue
        yield ch.queue_declare(q_name, auto_delete=False)
        self.addCoroCleanup(lambda: (yield (yield (yield self._connect()).channel()).queue_delete(q_name)))

        # Publish two messages to the queue by way of default exchange
        yield ch.publish(exchange='', routing_key=q_name, body=body1)
        yield ch.publish(exchange='', routing_key=q_name, body=body2)

        # Consume the events, but don't ack
        rx_messages = []
        yield ch.basic_consume(q_name, lambda *args: rx_messages.append(args),
                               auto_ack=False, exclusive=False, arguments=None)
        while len(rx_messages) != 2:
            connection.process_data_events(time_limit=None)

        self.assertEqual(rx_messages[0][1].delivery_tag, 1)
        self.assertEqual(rx_messages[1][1].delivery_tag, 2)

        # Verify no more ready messages in queue
        frame = yield ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 0)

        # Closing channel should restore messages back to queue
        yield ch.close()

        # Verify that there are two messages in q now
        ch = yield connection.channel()

        self._assert_exact_message_count_with_retries(channel=ch,
                                                      queue=q_name,
                                                      expected_count=2)


@unittest.skip("Basic consume problem")
class TestNoAckMessageNotRestoredToQueueOnChannelClose(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingChannel unacked message restored to q on channel close """
        connection = yield self._connect()

        ch = yield connection.channel()

        q_name = ('TestNoAckMessageNotRestoredToQueueOnChannelClose_q' +
                  uuid.uuid1().hex)

        body1 = 'a' * 1024
        body2 = 'b' * 2048

        # Declare a new queue
        yield ch.queue_declare(q_name, auto_delete=False)
        self.addCoroCleanup(lambda: (yield (yield (yield self._connect()).channel()).queue_delete(q_name)))

        # Publish two messages to the queue by way of default exchange
        yield ch.publish(exchange='', routing_key=q_name, body=body1)
        yield ch.publish(exchange='', routing_key=q_name, body=body2)

        # Consume, but don't ack
        num_messages = 0
        for rx_method, _, _ in ch.consume(q_name, auto_ack=True, exclusive=False):
            num_messages += 1

            self.assertEqual(rx_method.delivery_tag, num_messages)

            if num_messages == 2:
                break
        else:
            self.fail('expected 2 messages, but consumed %i' % (num_messages,))

        # Verify no more ready messages in queue
        frame = yield ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 0)

        # Closing channel should not restore no-ack messages back to queue
        yield ch.close()

        # Verify that there are no messages in q now
        ch = yield connection.channel()
        frame = yield ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 0)


@unittest.skip("Need to look at")
class TestConsumeGeneratorInactivityTimeout(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingChannel consume returns 3-tuple of None values on inactivity timeout """
        connection = yield self._connect()

        ch = yield connection.channel()

        q_name = ('TestConsumeGeneratorInactivityTimeout_q' + uuid.uuid1().hex)

        # Declare a new queue
        yield ch.queue_declare(q_name, auto_delete=True)

        # Expect to get only (None, None, None) upon inactivity timeout, since
        # there are no messages in queue
        for msg in ch.consume(q_name, inactivity_timeout=0.1):
            self.assertEqual(msg, (None, None, None))
            break
        else:
            self.fail('expected (None, None, None), but iterator stopped')


@unittest.skip("Need to look at")
class TestConsumeGeneratorInterruptedByCancelFromBroker(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingChannel consume generator is interrupted broker's Cancel """
        connection = yield self._connect()

        self.assertTrue(connection.consumer_cancel_notify_supported)

        ch = yield connection.channel()

        q_name = ('TestConsumeGeneratorInterruptedByCancelFromBroker_q' +
                  uuid.uuid1().hex)

        # Declare a new queue
        yield ch.queue_declare(q_name, auto_delete=True)

        queue_deleted = False
        for _ in ch.consume(q_name, auto_ack=False, inactivity_timeout=0.001):
            if not queue_deleted:
                # Delete the queue to force Basic.Cancel from the broker
                ch.queue_delete(q_name)
                queue_deleted = True

        self.assertIsNone(ch._queue_consumer_generator)


@unittest.skip("Need to look at")
class TestConsumeGeneratorCancelEncountersCancelFromBroker(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingChannel consume generator cancel called when broker's Cancel is enqueued """
        connection = yield self._connect()

        self.assertTrue(connection.consumer_cancel_notify_supported)

        ch = yield connection.channel()

        q_name = ('TestConsumeGeneratorCancelEncountersCancelFromBroker_q' +
                  uuid.uuid1().hex)

        # Declare a new queue
        yield ch.queue_declare(q_name, auto_delete=True)

        for _ in ch.consume(q_name, auto_ack=False, inactivity_timeout=0.001):
            # Delete the queue to force Basic.Cancel from the broker
            yield ch.queue_delete(q_name)

            # Wait for server's Basic.Cancel
            while not ch._queue_consumer_generator.pending_events:
                connection.process_data_events()

            # Confirm it's Basic.Cancel
            self.assertIsInstance(ch._queue_consumer_generator.pending_events[0],
                                  blocking_connection._ConsumerCancellationEvt)

            # Now attempt to cancel the consumer generator
            yield ch.cancel()

            self.assertIsNone(ch._queue_consumer_generator)


@unittest.skip("Need to look at")
class TestConsumeGeneratorPassesChannelClosedOnSameChannelFailure(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """consume() exits with ChannelClosed exception on same channel failure
        """
        connection = yield self._connect()
        loop = connection.loop

        # Fail test if exception leaks back ito I/O loop
        self._instrument_io_loop_exception_leak_detection(connection)

        ch = yield connection.channel()

        q_name = (
                'TestConsumeGeneratorPassesChannelClosedOnSameChannelFailure_q' +
                uuid.uuid1().hex)

        # Declare the queue
        yield ch.queue_declare(q_name, auto_delete=False)
        self.addCoroCleanup(lambda: (yield (yield (yield self._connect()).channel()).queue_delete(q_name)))

        # Schedule a callback that will cause a channel error on the consumer's
        # channel by publishing to an unknown exchange. This will cause the
        # broker to close our channel.
        loop.add_callback(
            lambda: ch.basic_publish(
                exchange=q_name,
                routing_key='123',
                body=b'Nope this is wrong'))

        self.assertFalse(ch._impl.is_closed_by_broker)

        with self.assertRaises(pika.exceptions.ChannelClosed):
            for _ in ch.consume(q_name):
                pass

        self.assertTrue(ch._impl.is_closed_by_broker)


class TestChannelFlow(BlockingTestCaseBase):
    @gen_test
    def test(self):
        """BlockingChannel Channel.Flow activate and deactivate """
        connection = yield self._connect()

        ch = yield connection.channel()

        q_name = ('TestChannelFlow_q' + uuid.uuid1().hex)

        # Declare a new queue
        yield ch.queue_declare(q_name, auto_delete=False)
        self.addCoroCleanup(lambda: (yield (yield (yield self._connect()).channel()).queue_delete(q_name)))

        # Verify zero active consumers on the queue
        frame = yield ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.consumer_count, 0)

        # Create consumer
        yield ch.basic_consume(q_name, lambda *args: None)

        # Verify one active consumer on the queue now
        frame = yield ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.consumer_count, 1)

        # Activate flow from default state (active by default)
        active = yield ch.flow(True)
        self.assertEqual(active, True)

        # Verify still one active consumer on the queue now
        frame = yield ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.consumer_count, 1)


if __name__ == '__main__':
    unittest.main()
