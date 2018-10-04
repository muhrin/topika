from __future__ import print_function
from __future__ import absolute_import
from builtins import bytes
import os
from tornado import gen, testing, concurrent
import uuid
import logging
import pika.exceptions
from sys import version_info
import shortuuid
import time
import unittest
from unittest import skipIf
from six.moves import range

try:
    from unittest import mock
except ImportError:
    from mock import mock

from topika.exceptions import ChannelClosed

import topika
import topika.exceptions
from copy import copy
from topika import connect, Message, DeliveryMode
from topika.exceptions import MessageProcessError, ProbableAuthenticationError
from topika.exchange import ExchangeType
from topika.tools import wait
from . import BaseTestCase, AMQP_URL

log = logging.getLogger(__name__)
skip_for_py34 = skipIf(version_info < (3, 5), "async/await syntax supported only for python 3.5+")


class TestCase(BaseTestCase):

    @testing.gen_test
    def test_channel_close(self):
        client = yield self.create_connection()

        self.get_random_name("test_connection")
        self.get_random_name()

        self.__closed = False

        def on_close(ch):
            log.info("Close called")
            self.__closed = True

        channel = yield client.channel()
        channel.add_close_callback(on_close)
        yield channel.close()

        yield gen.sleep(0.5)

        self.assertTrue(self.__closed)

        with self.assertRaises(RuntimeError):
            yield channel.initialize()

        yield self.create_channel(connection=client)

    @testing.gen_test
    def test_delete_queue_and_exchange(self):
        queue_name = self.get_random_name("test_connection")
        exchange = self.get_random_name()

        channel = yield self.create_channel()
        yield channel.declare_exchange(exchange, auto_delete=True)
        yield channel.declare_queue(queue_name, auto_delete=True)

        yield channel.queue_delete(queue_name)
        yield channel.exchange_delete(exchange)

    @testing.gen_test
    def test_temporary_queue(self):
        channel = yield self.create_channel()
        queue = yield channel.declare_queue(auto_delete=True)

        self.assertNotEqual(queue.name, '')

        body = os.urandom(32)

        yield channel.default_exchange.publish(Message(body=body), routing_key=queue.name)

        message = yield queue.get()

        self.assertEqual(message.body, body)

        yield channel.queue_delete(queue.name)

    @testing.gen_test
    def test_internal_exchange(self):
        client = yield self.create_connection()

        routing_key = self.get_random_name()
        exchange_name = self.get_random_name("internal", "exchange")

        channel = yield client.channel()
        exchange = yield self.declare_exchange(exchange_name, auto_delete=True, internal=True, channel=channel)
        queue = yield self.declare_queue(auto_delete=True, channel=channel)

        yield queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        with self.assertRaises(ValueError):
            f = exchange.publish(Message(body, content_type='text/plain', headers={'foo': 'bar'}), routing_key)
            yield f

        yield queue.unbind(exchange, routing_key)

    @testing.gen_test
    def test_declare_exchange_with_passive_flag(self):
        client = yield self.create_connection()

        exchange_name = self.get_random_name()
        channel = yield client.channel()

        with self.assertRaises(topika.exceptions.ChannelClosed):
            yield self.declare_exchange(exchange_name, auto_delete=True, passive=True, channel=channel)

        channel1 = yield client.channel()
        channel2 = yield client.channel()

        yield self.declare_exchange(exchange_name, auto_delete=True, passive=False, channel=channel1)

        # Check ignoring different exchange options
        yield self.declare_exchange(exchange_name, auto_delete=False, passive=True, channel=channel2)

    @testing.gen_test
    def test_simple_publish_and_receive(self):
        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        channel = yield self.create_channel()
        exchange = yield self.declare_exchange('direct', auto_delete=True, channel=channel)
        queue = yield self.declare_queue(queue_name, auto_delete=True, channel=channel)

        yield queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        result = yield exchange.publish(Message(body, content_type='text/plain', headers={'foo': 'bar'}), routing_key)
        self.assertTrue(result)

        incoming_message = yield queue.get(timeout=5)
        incoming_message.ack()

        self.assertEqual(incoming_message.body, body)

        yield queue.unbind(exchange, routing_key)

    @testing.gen_test
    def test_simple_publish_without_confirm(self):
        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        channel = yield self.create_channel(publisher_confirms=False)
        exchange = yield self.declare_exchange('direct', auto_delete=True, channel=channel)
        queue = yield self.declare_queue(queue_name, auto_delete=True, channel=channel)

        yield queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        result = yield exchange.publish(Message(body, content_type='text/plain', headers={'foo': 'bar'}), routing_key)
        self.assertIsNone(result)

        incoming_message = yield queue.get(timeout=5)
        incoming_message.ack()

        self.assertEqual(incoming_message.body, body)

        yield queue.unbind(exchange, routing_key)

    @testing.gen_test
    def test_simple_publish_and_receive_delivery_mode_explicitly_none(self):
        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        channel = yield self.create_channel()
        exchange = yield self.declare_exchange('direct', auto_delete=True, channel=channel)
        queue = yield self.declare_queue(queue_name, auto_delete=True, channel=channel)

        yield queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        yield exchange.publish(
            Message(body, content_type='text/plain', headers={'foo': 'bar'}, delivery_mode=None), routing_key)

        incoming_message = yield queue.get(timeout=5)
        incoming_message.ack()

        self.assertEqual(incoming_message.body, body)

        yield queue.unbind(exchange, routing_key)

    @testing.gen_test
    def test_simple_publish_and_receive_to_bound_exchange(self):
        routing_key = self.get_random_name()
        src_name = self.get_random_name("source", "exchange")
        dest_name = self.get_random_name("destination", "exchange")

        channel = yield self.create_channel()
        src_exchange = yield self.declare_exchange(src_name, auto_delete=True, channel=channel)
        dest_exchange = yield self.declare_exchange(dest_name, auto_delete=True, channel=channel)
        queue = yield self.declare_queue(auto_delete=True, channel=channel)

        yield queue.bind(dest_exchange, routing_key)

        yield dest_exchange.bind(src_exchange, routing_key)
        self.addCleanup(dest_exchange.unbind, src_exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        yield src_exchange.publish(Message(body, content_type='text/plain', headers={'foo': 'bar'}), routing_key)

        incoming_message = yield queue.get(timeout=5)
        incoming_message.ack()

        self.assertEqual(incoming_message.body, body)

        yield queue.unbind(dest_exchange, routing_key)

    @testing.gen_test
    def test_incoming_message_info(self):
        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        channel = yield self.create_channel()
        exchange = yield self.declare_exchange('direct', auto_delete=True, channel=channel)
        queue = yield self.declare_queue(queue_name, auto_delete=True, channel=channel)

        yield queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        self.maxDiff = None

        info = {
            'headers': {
                "foo": "bar"
            },
            'content_type': "application/json",
            'content_encoding': "text",
            'delivery_mode': DeliveryMode.PERSISTENT.value,
            'priority': 0,
            'correlation_id': b'1',
            'reply_to': 'test',
            'expiration': 1.5,
            'message_id': shortuuid.uuid(),
            'timestamp': int(time.time()),
            'type': '0',
            'user_id': 'guest',
            'app_id': 'test',
            'body_size': len(body)
        }

        msg = Message(
            body=body,
            headers={'foo': 'bar'},
            content_type='application/json',
            content_encoding='text',
            delivery_mode=DeliveryMode.PERSISTENT,
            priority=0,
            correlation_id=1,
            reply_to='test',
            expiration=1.5,
            message_id=info['message_id'],
            timestamp=info['timestamp'],
            type='0',
            user_id='guest',
            app_id='test')

        yield exchange.publish(msg, routing_key)

        incoming_message = yield queue.get(timeout=5)
        incoming_message.ack()

        info['synchronous'] = incoming_message.synchronous
        info['routing_key'] = incoming_message.routing_key
        info['redelivered'] = incoming_message.redelivered
        info['exchange'] = incoming_message.exchange
        info['delivery_tag'] = incoming_message.delivery_tag
        info['consumer_tag'] = incoming_message.consumer_tag
        info['cluster_id'] = incoming_message.cluster_id

        self.assertEqual(incoming_message.body, body)
        self.assertDictEqual(incoming_message.info(), info)

        yield queue.unbind(exchange, routing_key)

    @testing.gen_test
    def test_context_process(self):
        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        channel = yield self.create_channel()
        exchange = yield self.declare_exchange('direct', auto_delete=True, channel=channel)
        queue = yield self.declare_queue(queue_name, auto_delete=True, channel=channel)

        yield queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        yield exchange.publish(Message(body, content_type='text/plain', headers={'foo': 'bar'}), routing_key)

        incoming_message = yield queue.get(timeout=5)

        with self.assertRaises(AssertionError):
            with incoming_message.process(requeue=True):
                raise AssertionError

        self.assertEqual(incoming_message.locked, True)

        incoming_message = yield queue.get(timeout=5)

        with incoming_message.process():
            pass

        self.assertEqual(incoming_message.body, body)

        yield exchange.publish(Message(body, content_type='text/plain', headers={'foo': 'bar'}), routing_key)

        incoming_message = yield queue.get(timeout=5)

        with self.assertRaises(MessageProcessError):
            with incoming_message.process():
                incoming_message.reject(requeue=True)

        self.assertEqual(incoming_message.locked, True)

        incoming_message = yield queue.get(timeout=5)

        with incoming_message.process(ignore_processed=True):
            incoming_message.reject(requeue=False)

        self.assertEqual(incoming_message.body, body)

        yield exchange.publish(Message(body, content_type='text/plain', headers={'foo': 'bar'}), routing_key)

        incoming_message = yield queue.get(timeout=5)
        with self.assertRaises(AssertionError):
            with incoming_message.process(requeue=True, reject_on_redelivered=True):
                raise AssertionError

        incoming_message = yield queue.get(timeout=5)
        with self.assertRaises(AssertionError):
            with incoming_message.process(requeue=True, reject_on_redelivered=True):
                raise AssertionError

        self.assertEqual(incoming_message.locked, True)

        yield queue.unbind(exchange, routing_key)

    @testing.gen_test
    def test_context_process_redelivery(self):
        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        channel = yield self.create_channel()
        exchange = yield self.declare_exchange('direct', auto_delete=True, channel=channel)
        queue = yield self.declare_queue(queue_name, auto_delete=True, channel=channel)

        yield queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        yield exchange.publish(Message(body, content_type='text/plain', headers={'foo': 'bar'}), routing_key)

        incoming_message = yield queue.get(timeout=5)

        with self.assertRaises(AssertionError):
            with incoming_message.process(requeue=True, reject_on_redelivered=True):
                raise AssertionError

        incoming_message = yield queue.get(timeout=5)

        with mock.patch('topika.message.LOGGER') as message_logger:
            with self.assertRaises(Exception):
                with incoming_message.process(requeue=True, reject_on_redelivered=True):
                    raise Exception

            self.assertTrue(message_logger.info.called)
            self.assertEqual(message_logger.info.mock_calls[0][1][1].body, incoming_message.body)

        self.assertEqual(incoming_message.body, body)

        yield queue.unbind(exchange, routing_key)

    @testing.gen_test
    def test_no_ack_redelivery(self):
        client = yield self.create_connection()

        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        channel = yield client.channel()
        exchange = yield channel.declare_exchange('direct', auto_delete=True)
        queue = yield channel.declare_queue(queue_name, auto_delete=False)

        yield queue.bind(exchange, routing_key)

        # publish 2 messages
        for _ in range(2):
            body = bytes(shortuuid.uuid(), 'utf-8')
            msg = Message(body)
            yield exchange.publish(msg, routing_key)

        # ack 1 message out of 2
        first_message = yield queue.get(timeout=5)

        last_message = yield queue.get(timeout=5)
        last_message.ack()

        # close channel, not acked message should be redelivered
        yield channel.close()

        channel = yield client.channel()
        exchange = yield channel.declare_exchange('direct', auto_delete=True)
        queue = yield channel.declare_queue(queue_name, auto_delete=False)

        # receive not acked message
        message = yield queue.get(timeout=5)
        self.assertEqual(message.body, first_message.body)
        message.ack()

        yield queue.unbind(exchange, routing_key)

    @testing.gen_test
    def test_ack_multiple(self):
        client = yield self.create_connection()

        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        channel = yield client.channel()
        exchange = yield channel.declare_exchange('direct', auto_delete=True)
        queue = yield channel.declare_queue(queue_name, auto_delete=False)

        yield queue.bind(exchange, routing_key)

        # publish 2 messages
        for _ in range(2):
            body = bytes(shortuuid.uuid(), 'utf-8')
            msg = Message(body)
            yield exchange.publish(msg, routing_key)

        # ack only last mesage with multiple flag, first message should be acked too
        yield queue.get(timeout=5)
        last_message = yield queue.get(timeout=5)
        last_message.ack(multiple=True)

        # close channel, no messages should be redelivered
        yield channel.close()

        channel = yield client.channel()
        exchange = yield channel.declare_exchange('direct', auto_delete=True)
        queue = yield channel.declare_queue(queue_name, auto_delete=False)

        with self.assertRaises(topika.exceptions.QueueEmpty):
            yield queue.get()

        yield queue.unbind(exchange, routing_key)
        yield queue.delete()
        yield wait((client.close(), client.closing))

    @testing.gen_test
    def test_ack_twice(self):
        client = yield self.create_connection()

        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        channel = yield client.channel()
        exchange = yield channel.declare_exchange('direct', auto_delete=True)
        queue = yield channel.declare_queue(queue_name, auto_delete=True)

        yield queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        yield exchange.publish(Message(body, content_type='text/plain', headers={'foo': 'bar'}), routing_key)

        incoming_message = yield queue.get(timeout=5)
        incoming_message.ack()

        with self.assertRaises(MessageProcessError):
            incoming_message.ack()

        self.assertEqual(incoming_message.body, body)
        yield queue.unbind(exchange, routing_key)
        yield queue.delete()
        yield wait((client.close(), client.closing))

    @testing.gen_test
    def test_reject_twice(self):
        client = yield self.create_connection()

        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        channel = yield client.channel()
        exchange = yield channel.declare_exchange('direct', auto_delete=True)
        queue = yield channel.declare_queue(queue_name, auto_delete=True)

        yield queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        yield exchange.publish(Message(body, content_type='text/plain', headers={'foo': 'bar'}), routing_key)

        incoming_message = yield queue.get(timeout=5)
        incoming_message.reject(requeue=False)

        with self.assertRaises(MessageProcessError):
            incoming_message.reject(requeue=False)

        self.assertEqual(incoming_message.body, body)
        yield queue.unbind(exchange, routing_key)
        yield queue.delete()
        yield wait((client.close(), client.closing))

    @testing.gen_test
    def test_consuming(self):
        client = yield self.create_connection()

        queue_name = self.get_random_name("tc2")
        routing_key = self.get_random_name()

        channel = yield client.channel()
        exchange = yield channel.declare_exchange('direct', auto_delete=True)
        queue = yield channel.declare_queue(queue_name, auto_delete=True)

        yield queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        message_future = concurrent.Future()

        @gen.coroutine
        def handle(message):
            message.ack()
            self.assertEqual(message.body, body)
            self.assertEqual(message.routing_key, routing_key)
            message_future.set_result(True)

        yield queue.consume(handle)

        yield exchange.publish(Message(body, content_type='text/plain', headers={'foo': 'bar'}), routing_key)

        if not message_future.done():
            yield message_future

        yield queue.unbind(exchange, routing_key)
        yield exchange.delete()
        yield wait((client.close(), client.closing))

    @testing.gen_test
    def test_consuming_not_coroutine(self):
        client = yield self.create_connection()

        queue_name = self.get_random_name("tc2")
        routing_key = self.get_random_name()

        channel = yield client.channel()
        exchange = yield channel.declare_exchange('direct', auto_delete=True)
        queue = yield channel.declare_queue(queue_name, auto_delete=True)

        yield queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        f = concurrent.Future()

        def handle(message):
            message.ack()
            self.assertEqual(message.body, body)
            self.assertEqual(message.routing_key, routing_key)
            f.set_result(True)

        yield queue.consume(handle)

        yield exchange.publish(Message(body, content_type='text/plain', headers={'foo': 'bar'}), routing_key)

        if not f.done():
            yield f

        yield queue.unbind(exchange, routing_key)
        yield exchange.delete()
        yield wait((client.close(), client.closing))

    @testing.gen_test
    def test_ack_reject(self):
        client = yield self.create_connection()

        queue_name = self.get_random_name("test_connection3")
        routing_key = self.get_random_name()

        channel = yield client.channel()
        exchange = yield channel.declare_exchange('direct', auto_delete=True)
        queue = yield channel.declare_queue(queue_name, auto_delete=True)

        yield queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        yield exchange.publish(Message(body, content_type='text/plain', headers={'foo': 'bar'}), routing_key)

        incoming_message = yield queue.get(timeout=5, no_ack=True)

        with self.assertRaises(TypeError):
            incoming_message.ack()

        yield exchange.publish(Message(body, content_type='text/plain', headers={'foo': 'bar'}), routing_key)

        incoming_message = yield queue.get(timeout=5)

        incoming_message.reject()

        yield exchange.publish(Message(body, content_type='text/plain', headers={'foo': 'bar'}), routing_key)

        incoming_message = yield queue.get(timeout=5, no_ack=True)

        with self.assertRaises(TypeError):
            yield incoming_message.reject()

        self.assertEqual(incoming_message.body, body)

        yield queue.unbind(exchange, routing_key)
        yield queue.delete()
        yield wait((client.close(), client.closing))

    @testing.gen_test
    def test_purge_queue(self):
        queue_name = self.get_random_name("test_connection4")
        routing_key = self.get_random_name()

        channel = yield self.create_channel()
        exchange = yield channel.declare_exchange('direct', auto_delete=True)
        queue = yield channel.declare_queue(queue_name, auto_delete=True)

        yield queue.bind(exchange, routing_key)

        try:
            body = bytes(shortuuid.uuid(), 'utf-8')

            yield exchange.publish(Message(body, content_type='text/plain', headers={'foo': 'bar'}), routing_key)

            yield queue.purge()

            with self.assertRaises(gen.TimeoutError):
                yield queue.get(timeout=1)
        except:
            yield queue.unbind(exchange, routing_key)
            yield queue.delete()

    @testing.gen_test
    def test_connection_refused(self):
        with self.assertRaises(topika.exceptions.AMQPConnectionError):
            yield connect('amqp://guest:guest@localhost:9999')

    @testing.gen_test
    def test_wrong_credentials(self):
        amqp_url = AMQP_URL.copy()
        amqp_url.username = uuid.uuid4().hex
        amqp_url.password = uuid.uuid4().hex

        with self.assertRaises(pika.exceptions.ConnectionClosedByBroker):
            yield connect(amqp_url)

    @testing.gen_test
    def test_set_qos(self):
        channel = yield self.create_channel()
        yield channel.set_qos(prefetch_count=1, all_channels=True)

    @testing.gen_test
    def test_exchange_delete(self):
        channel = yield self.create_channel()
        exchange = yield channel.declare_exchange("test", auto_delete=True)
        yield exchange.delete()

    @testing.gen_test
    def test_dlx(self):
        suffix = self.get_random_name()
        routing_key = "%s_routing_key" % suffix
        dlx_routing_key = "%s_dlx_routing_key" % suffix

        channel = yield self.create_channel()

        f = concurrent.Future()

        @gen.coroutine
        def dlx_handle(message):
            message.ack()
            self.assertEqual(message.body, body)
            self.assertEqual(message.routing_key, dlx_routing_key)
            f.set_result(True)

        direct_exchange = yield self.declare_exchange('direct', channel=channel, auto_delete=True)  # type:
        # topika.Exchange
        dlx_exchange = yield channel.declare_exchange('dlx', ExchangeType.DIRECT, auto_delete=True)

        direct_queue = yield channel.declare_queue(
            "%s_direct_queue" % suffix,
            auto_delete=True,
            arguments={
                'x-message-ttl': 300,
                'x-dead-letter-exchange': 'dlx',
                'x-dead-letter-routing-key': dlx_routing_key
            })

        dlx_queue = yield channel.declare_queue("%s_dlx_queue" % suffix, auto_delete=True)

        yield dlx_queue.consume(dlx_handle)
        yield dlx_queue.bind(dlx_exchange, dlx_routing_key)
        yield direct_queue.bind(direct_exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        try:
            yield direct_exchange.publish(
                Message(
                    body, content_type='text/plain', headers={
                        'x-message-ttl': 100,
                        'x-dead-letter-exchange': 'dlx',
                    }), routing_key)

            if not f.done():
                yield f
        finally:
            yield dlx_queue.unbind(dlx_exchange, routing_key)
            yield direct_queue.unbind(direct_exchange, routing_key)
            yield direct_queue.delete()
            yield direct_exchange.delete()
            yield dlx_exchange.delete()

    @testing.gen_test
    def test_connection_close(self):
        """ Try setting an invalid delivery mode on a message """
        client = yield self.create_connection()

        routing_key = self.get_random_name()

        channel = yield client.channel()  # type: topika.Channel
        exchange = yield channel.declare_exchange('direct', auto_delete=True)

        try:
            with self.assertRaises(pika.exceptions.ChannelClosedByBroker):
                msg = Message(bytes(shortuuid.uuid(), 'utf-8'))
                msg.delivery_mode = 8

                yield exchange.publish(msg, routing_key)

            channel = yield client.channel()
            exchange = yield channel.declare_exchange('direct', auto_delete=True)
        finally:
            yield exchange.delete()
            yield wait((client.close(), client.closing))

    @testing.gen_test
    def test_basic_return(self):
        client = yield self.create_connection()

        channel = yield client.channel()  # type: topika.Channel

        f = concurrent.Future()

        channel.add_on_return_callback(f.set_result)

        body = bytes(shortuuid.uuid(), 'utf-8')

        yield channel.default_exchange.publish(
            Message(body, content_type='text/plain', headers={'foo': 'bar'}), self.get_random_name("test_basic_return"))

        returned = yield f

        self.assertEqual(returned.body, body)

        # handler with exception
        f = concurrent.Future()

        yield channel.close()

        channel = yield client.channel()  # type: topika.Channel

        def bad_handler(message):
            try:
                raise ValueError
            finally:
                f.set_result(message)

        channel.add_on_return_callback(bad_handler)

        body = bytes(shortuuid.uuid(), 'utf-8')

        yield channel.default_exchange.publish(
            Message(body, content_type='text/plain', headers={'foo': 'bar'}), self.get_random_name("test_basic_return"))

        returned = yield f

        self.assertEqual(returned.body, body)

        yield wait((client.close(), client.closing))

    @testing.gen_test
    def test_expiration(self):
        client = yield self.create_connection()

        channel = yield client.channel()  # type: topika.Channel

        dlx_queue = yield channel.declare_queue(self.get_random_name("test_dlx"))  # type: topika.Queue

        dlx_exchange = yield channel.declare_exchange(self.get_random_name("dlx"),)  # type: topika.Exchange

        yield dlx_queue.bind(dlx_exchange, routing_key=dlx_queue.name)

        queue = yield channel.declare_queue(
            self.get_random_name("test_expiration"),
            arguments={
                "x-message-ttl": 10000,
                "x-dead-letter-exchange": dlx_exchange.name,
                "x-dead-letter-routing-key": dlx_queue.name,
            })  # type: topika.Queue

        body = bytes(shortuuid.uuid(), 'utf-8')

        yield channel.default_exchange.publish(
            Message(body, content_type='text/plain', headers={'foo': 'bar'}, expiration=0.5), queue.name)

        f = concurrent.Future()

        yield dlx_queue.consume(f.set_result, no_ack=True)

        message = yield f

        self.assertEqual(message.body, body)
        self.assertEqual(message.headers['x-death'][0]['original-expiration'], '500')

        yield wait((client.close(), client.closing))

    @testing.gen_test
    def test_add_close_callback(self):
        client = yield self.create_connection()

        shared_list = []

        def share(f):
            shared_list.append(f)

        client.add_close_callback(share)
        yield client.close()

        self.assertEqual(len(shared_list), 1)

    @testing.gen_test
    def test_big_message(self):
        client = yield self.create_connection()

        queue_name = self.get_random_name("test_big")
        routing_key = self.get_random_name()

        channel = yield client.channel()
        exchange = yield channel.declare_exchange('direct', auto_delete=True)
        queue = yield channel.declare_queue(queue_name, auto_delete=True)

        yield queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8') * 9999999

        yield exchange.publish(Message(body, content_type='text/plain', headers={'foo': 'bar'}), routing_key)

        incoming_message = yield queue.get(timeout=5)
        incoming_message.ack()

        self.assertEqual(incoming_message.body, body)
        yield queue.unbind(exchange, routing_key)
        yield queue.delete()
        yield wait((client.close(), client.closing))

    @testing.gen_test
    def test_unexpected_channel_close(self):
        client = yield self.create_connection()

        channel = yield client.channel()

        with self.assertRaises(ChannelClosed):
            yield channel.declare_queue("amq.restricted_queue_name", auto_delete=True)

        yield wait((client.close(), client.closing))

    @testing.gen_test
    def test_declaration_result(self):
        client = yield self.create_connection()

        channel = yield client.channel()

        queue = yield channel.declare_queue(auto_delete=True)

        self.assertEqual(queue.declaration_result.message_count, 0)
        self.assertEqual(queue.declaration_result.consumer_count, 0)

        yield wait((client.close(), client.closing))

    @testing.gen_test
    def test_declaration_result_with_consumers(self):
        client = yield self.create_connection()

        channel1 = yield client.channel()

        queue_name = self.get_random_name("queue", "declaration-result")
        queue1 = yield channel1.declare_queue(queue_name, auto_delete=True)
        yield queue1.consume(print)

        channel2 = yield client.channel()

        queue2 = yield channel2.declare_queue(queue_name, passive=True)

        self.assertEqual(queue2.declaration_result.consumer_count, 1)

        yield wait((client.close(), client.closing))

    @testing.gen_test
    def test_declaration_result_with_messages(self):
        client = yield self.create_connection()

        channel1 = yield client.channel()
        channel2 = yield client.channel()

        queue_name = self.get_random_name("queue", "declaration-result")
        queue1 = yield channel1.declare_queue(queue_name, auto_delete=True)

        yield channel1.default_exchange.publish(Message(body=b'test'), routing_key=queue1.name)

        queue2 = yield channel2.declare_queue(queue_name, passive=True)
        yield queue2.get()
        yield queue2.delete()

        self.assertEqual(queue2.declaration_result.consumer_count, 0)
        self.assertEqual(queue2.declaration_result.message_count, 1)

        yield wait((client.close(), client.closing))

    @testing.gen_test
    def test_queue_empty_exception(self):

        client = yield self.create_connection()
        queue_name = self.get_random_name("test_get_on_empty_queue")
        channel = yield client.channel()
        queue = yield channel.declare_queue(queue_name, auto_delete=True)

        with self.assertRaises(topika.exceptions.QueueEmpty):
            yield queue.get(timeout=5)

        yield channel.default_exchange.publish(
            Message(b'test'),
            queue_name,
        )

        message = yield queue.get(timeout=5)
        self.assertEqual(message.body, b'test')

        # test again for #110
        with self.assertRaises(topika.exceptions.QueueEmpty):
            yield queue.get(timeout=5)

        yield queue.delete()
        yield wait((client.close(), client.closing))

    @testing.gen_test
    def test_queue_empty_fail_false(self):

        client = yield self.create_connection()
        queue_name = self.get_random_name("test_get_on_empty_queue")
        channel = yield client.channel()
        queue = yield channel.declare_queue(queue_name, auto_delete=True)

        result = yield queue.get(fail=False)
        self.assertIsNone(result)

        yield queue.delete()
        yield wait((client.close(), client.closing))

    @testing.gen_test
    def test_message_nack(self):

        client = yield self.create_connection()
        queue_name = self.get_random_name("test_nack_queue")
        body = uuid.uuid4().bytes
        channel = yield client.channel()
        queue = yield channel.declare_queue(queue_name, auto_delete=True)

        yield channel.default_exchange.publish(Message(body=body), routing_key=queue_name)

        message = yield queue.get()  # type: topika.IncomingMessage

        self.assertEqual(message.body, body)
        message.nack(requeue=True)

        message = yield queue.get()

        self.assertTrue(message.redelivered)
        self.assertEqual(message.body, body)
        message.ack()

        yield queue.delete()
        yield wait((client.close(), client.closing))

    @testing.gen_test
    def test_on_return_raises(self):
        client = yield self.create_connection()
        queue_name = self.get_random_name("test_on_return_raises")
        body = uuid.uuid4().bytes

        with self.assertRaises(RuntimeError):
            yield client.channel(publisher_confirms=False, on_return_raises=True)

        channel = yield client.channel(publisher_confirms=True, on_return_raises=True)

        for _ in range(100):
            with self.assertRaises(topika.exceptions.UnroutableError):
                yield channel.default_exchange.publish(
                    Message(body=body),
                    routing_key=queue_name,
                )

        yield client.close()

    @testing.gen_test
    def test_transaction_when_publisher_confirms_error(self):
        channel = yield self.create_channel(publisher_confirms=True)
        with self.assertRaises(RuntimeError):
            channel.transaction()

    @testing.gen_test
    def test_transaction_simple_commit(self):
        channel = yield self.create_channel(publisher_confirms=False)
        tx = channel.transaction()
        yield tx.select()
        yield tx.commit()

    @testing.gen_test
    def test_transaction_simple_rollback(self):
        channel = yield self.create_channel(publisher_confirms=False)
        tx = channel.transaction()
        yield tx.select()
        yield tx.rollback()

    # @skip_for_py34
    # @testing.gen_test
    # def test_transaction_simple_async_commit(self):
    #     from ._async_await_cases import test_transaction_simple_async_commit as func
    #     return func(self)
    #
    # @skip_for_py34
    # @testing.gen_test
    # def test_transaction_simple_async_rollback(self):
    #     from ._async_await_cases import test_transaction_simple_async_rollback as func
    #     return func(self)
    #
    # @skip_for_py34
    # @testing.gen_test
    # def test_async_for_queue(self):
    #     from ._async_await_cases import test_async_for_queue as func
    #     return func(self)
    #
    # @skip_for_py34
    # @testing.gen_test
    # def test_async_for_queue_context(self):
    #     from ._async_await_cases import test_async_for_queue_async_context as func
    #     return func(self)
    #
    # @skip_for_py34
    # @testing.gen_test
    # def test_async_with_connection(self):
    #     from ._async_await_cases import test_async_connection_context as func
    #     return func(self)


class MessageTestCase(unittest.TestCase):

    def test_message_copy(self):
        msg1 = Message(bytes(shortuuid.uuid(), 'utf-8'))
        msg2 = copy(msg1)

        msg1.lock()

        self.assertFalse(msg2.locked)

    def test_message_info(self):
        body = bytes(shortuuid.uuid(), 'utf-8')

        info = {
            'headers': {
                "foo": "bar"
            },
            'content_type': "application/json",
            'content_encoding': "text",
            'delivery_mode': DeliveryMode.PERSISTENT.value,
            'priority': 0,
            'correlation_id': b'1',
            'reply_to': 'test',
            'expiration': 1.5,
            'message_id': shortuuid.uuid(),
            'timestamp': int(time.time()),
            'type': '0',
            'user_id': 'guest',
            'app_id': 'test',
            'body_size': len(body)
        }

        msg = Message(
            body=body,
            headers={'foo': 'bar'},
            content_type='application/json',
            content_encoding='text',
            delivery_mode=DeliveryMode.PERSISTENT,
            priority=0,
            correlation_id=1,
            reply_to='test',
            expiration=1.5,
            message_id=info['message_id'],
            timestamp=info['timestamp'],
            type='0',
            user_id='guest',
            app_id='test')

        self.assertDictEqual(info, msg.info())
