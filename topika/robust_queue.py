from __future__ import absolute_import
from collections import namedtuple
from logging import getLogger
from tornado import gen
from types import FunctionType

import shortuuid

from . import compat
from .channel import Channel
from .queue import Queue
from . import tools

log = getLogger(__name__)

DeclarationResult = namedtuple('DeclarationResult', ('message_count', 'consumer_count'))


class RobustQueue(Queue):

    def __init__(self, loop, future_store, channel, name, durable, exclusive, auto_delete, arguments):
        """
        :type loop: :class:`tornado.ioloop.IOLoop`
        :type future_store: :class:`topika.FutureStore`
        :type channel: :class:`pika.Channel`
        :type name: str
        :type durable: bool
        :type exclusive: bool
        :type auto_delete: bool
        :type arguments: dict
        """

        super(RobustQueue, self).__init__(loop, future_store, channel, name or "amq_%s" % shortuuid.uuid(), durable,
                                          exclusive, auto_delete, arguments)

        self._consumers = {}
        self._bindings = {}

    @gen.coroutine
    def on_reconnect(self, channel):
        """
        :type channel: :class:`Channel`
        """
        self._futures.reject_all(compat.ConnectionError("Auto Reconnect Error"))
        self._channel = channel._channel

        yield self.declare()

        for item, kwargs in self._bindings.items():
            exchange, routing_key = item
            yield self.bind(exchange, routing_key, **kwargs)

        for consumer_tag, kwargs in tuple(self._consumers.items()):
            yield self.consume(consumer_tag=consumer_tag, **kwargs)

    @gen.coroutine
    def bind(self, exchange, routing_key=None, arguments=None, timeout=None):
        """ A binding is a relationship between an exchange and a queue. This can be
        simply read as: the queue is interested in messages from this exchange.

        Bindings can take an extra routing_key parameter. To avoid the confusion
        with a basic_publish parameter we're going to call it a binding key.

        :param exchange: :class:`topika.exchange.Exchange` instance
        :type exchange: :class:`ExchangeType_`
        :param routing_key: routing key
        :type routing_key: str
        :param arguments: additional arguments (will be passed to `pika`)
        :type arguments: dict or NoneType
        :param timeout: execution timeout
        :type timeout: int
        :raises tornado.gen.TimeoutError: when the binding timeout period has elapsed.
        :rtype: :class:`tornado.concurrent.Future`
        """
        kwargs = dict(arguments=arguments, timeout=timeout)

        result = yield super(RobustQueue, self).bind(exchange=exchange, routing_key=routing_key, **kwargs)

        self._bindings[(exchange, routing_key)] = kwargs

        raise gen.Return(result)

    @gen.coroutine
    def unbind(self, exchange, routing_key, arguments=None, timeout=None):
        """
        :param exchange:  The exchange to unbind
        :type exchange: :class:`ExchangeType_`
        :param routing_key: The routing key
        :type routing_key: str
        :param arguments:
        :param timeout:
        :return:
        """
        result = yield super(RobustQueue, self).unbind(exchange, routing_key, arguments, timeout)
        self._bindings.pop((exchange, routing_key), None)

        raise gen.Return(result)

    @tools.coroutine
    def consume(self, callback, no_ack=False, exclusive=False, arguments=None, consumer_tag=None, timeout=None):
        """ Start to consuming the :class:`Queue`.

        :param callback: Consuming callback. Could be a coroutine.
        :type callback: :class:`FunctionType`
        :param no_ack: if :class:`True` you don't need to call :func:`topika.message.IncomingMessage.ack`
        :type no_ack: bool
        :param exclusive: Makes this queue exclusive. Exclusive queues may only be accessed by the current
                          connection, and are deleted when that connection closes. Passive declaration of an
                          exclusive queue by other connections are not allowed.
        :type exclusive: bool
        :param arguments: extended arguments for pika
        :type arguments: Optiona[dict]
        :param consumer_tag: optional consumer tag
        :param timeout: :class:`tornado.gen.TimeoutError` will be raises when the
                        Future was not finished after this time.

        :raises tornado.gen.TimeoutError: when the consuming timeout period has elapsed.
        :rtype: class:`Generator[Any, None, ConsumerTag]`
        """
        kwargs = dict(callback=callback, no_ack=no_ack, exclusive=exclusive, arguments=arguments)

        consumer_tag = yield super(RobustQueue, self).consume(consumer_tag=consumer_tag, **kwargs)

        self._consumers[consumer_tag] = kwargs

        raise gen.Return(consumer_tag)

    @gen.coroutine
    def cancel(self, consumer_tag, timeout=None):
        result = yield super(RobustQueue, self).cancel(consumer_tag, timeout)
        self._consumers.pop(consumer_tag, None)
        raise gen.Return(result)


__all__ = 'RobustQueue',
