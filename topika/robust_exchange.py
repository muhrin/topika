from __future__ import absolute_import
from logging import getLogger
from tornado import gen

from . import compat
from .common import FutureStore
from .exchange import Exchange, ExchangeType
from .channel import Channel

_LOGGER = getLogger(__name__)


class RobustExchange(Exchange):
    """ Exchange abstraction """

    def __init__(self,
                 loop,
                 future_store,
                 channel,
                 publish_method,
                 name=None,
                 type=ExchangeType.DIRECT,
                 passive=False,
                 durable=False,
                 auto_delete=False,
                 internal=False,
                 arguments=None):
        """
        :type loop: :class:`tornado.ioloop.IOLoop`
        :type future_store: :class:`FutureStore`
        :type channel: :class:`Channel`
        :type name: str
        :type type: ExchangeType
        :type auto_delete: Optional[bool]
        :type durable: Optional[bool]
        :type internal: Optional[bool]
        :type passive: Optional[bool]
        :type arguments: dict or NoneType
        """
        super(RobustExchange, self).__init__(
            loop=loop,
            future_store=future_store,
            channel=channel,
            publish_method=publish_method,
            name=name,
            type=type,
            auto_delete=auto_delete,
            durable=durable,
            internal=internal,
            passive=passive,
            arguments=arguments,
        )

        self._bindings = dict()

    @gen.coroutine
    def on_reconnect(self, channel):
        """
        :type channel: :class:`Channel`
        """
        self._futures.reject_all(compat.ConnectionError("Auto Reconnect Error"))
        self._channel = channel._channel

        yield self.declare()

        for exchange, kwargs in self._bindings.items():
            yield self.bind(exchange, **kwargs)

    @gen.coroutine
    def bind(self, exchange, routing_key='', arguments=None, timeout=None):
        """
        :param exchange: :class:`topika.exchange.Exchange` instance
        :type exchange: ExchangeType_
        :param routing_key: routing key
        :type routing_key: str
        :param arguments: additional arguments (will be passed to `pika`)
        :param timeout: execution timeout
        :type timeout: int
        :rtype: :class:`tornado.concurrent.Future`
        """
        result = yield super(RobustExchange, self).bind(
            exchange, routing_key=routing_key, arguments=arguments, timeout=timeout)

        self._bindings[exchange] = dict(routing_key=routing_key, arguments=arguments)

        raise gen.Return(result)

    @gen.coroutine
    def unbind(self, exchange, routing_key='', arguments=None, timeout=None):
        """ Remove exchange-to-exchange binding for this :class:`Exchange` instance

        :param exchange: :class:`topika.exchange.Exchange` instance
        :type exchange: ExchangeType_
        :param routing_key: routing key
        :type routing_key: str
        :param arguments: additional arguments (will be passed to `pika`)
        :type arguments: dict
        :param timeout: execution timeout
        :type timeout: int
        :rtype: :class:`tornado.concurrent.Future`
        """
        result = yield super(RobustExchange, self).unbind(exchange, routing_key, arguments=arguments, timeout=timeout)
        self._bindings.pop(exchange, None)
        raise gen.Return(result)


__all__ = ('Exchange',)
