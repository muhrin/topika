import collections
import contextlib
import logging
import pika
import pika.exceptions
from tornado.concurrent import Future
from tornado.gen import coroutine, Return, WaitIterator

from . import common

__all__ = ('Channel',)

LOGGER = logging.getLogger(__name__)

DeliveryInfo = collections.namedtuple('DeliveryInfo', ['tag', 'future'])

ReturnedMessage = collections.namedtuple('ReturnedMessage', ['method', 'properties', 'body'])


class Channel(object):
    # Used as value_class with _CallbackResult for receiving Basic.GetOk args
    _RxMessageArgs = collections.namedtuple(
        'BlockingChannel__RxMessageArgs',
        [
            'channel',  # implementation pika.Channel instance
            'method',  # Basic.GetOk
            'properties',  # pika.spec.BasicProperties
            'body'  # str, unicode, or bytes (python 3.x)
        ])

    def __init__(self, pika_channel, connection, future_store):
        """
        Create a new instance of the Channel.  Don't call this directly, this should
        be constructed by the connection.

        :param pika_channel: The pika channel object
        :type pika_channel: :class:`pika.Channel`
        :param connection: The tornado connection
        :type connection: :class:`TornadoConnection`
        :param future_store: The future store to use
        :type future_store: :class:`topika.common.FutureStore`
        """
        self._impl = pika_channel
        self._connection = connection
        self._future_store = future_store

        # Whether RabbitMQ delivery confirmation has been enabled
        self._delivery_confirmation = False
        # These are only used when delivery confirmation is enabled
        self._num_published = 0
        # Information on sent messages, used to correlate to ack/nack or returned
        # messages
        self._delivery_info = collections.deque()

        # Holds a ReturnedMessage object representing a message received via
        # Basic.Return in publisher-acknowledgments mode.
        self._puback_return = None

        # self._impl.add_on_cancel_callback(self._on_consumer_cancelled_by_broker)

        # self._impl.add_callback(
        #     self._basic_consume_ok_result.signal_once,
        #     replies=[pika.spec.Basic.ConsumeOk],
        #     one_shot=False)

        self._impl.add_on_close_callback(self._on_channel_closed)
        # Used when the user initiates a close channel operation
        self._close_future = None

        # self._impl.add_callback(
        #     self._basic_getempty_result.set_value_once,
        #     replies=[pika.spec.Basic.GetEmpty],
        #     one_shot=False)

        LOGGER.info("Created channel=%s", self.channel_number)

    def __int__(self):
        """Return the channel object as its channel number

        NOTE: inherited from legacy BlockingConnection; might be error-prone;
        use `channel_number` property instead.

        :rtype: int

        """
        return self.channel_number

    def __repr__(self):
        return '<%s impl=%r>' % (self.__class__.__name__, self._impl)

    # These can only potentially work on python 3.5+ with async_generator
    # @coroutine
    # def __enter__(self):
    #     raise Return(self)
    #
    # @coroutine
    # def __exit__(self, exc_type, exc_val, exc_tb):
    #     try:
    #         yield self.close()
    #     except (pika.exceptions.ConnectionClosed, pika.exceptions.ChannelClosed):
    #         pass
    #     raise Return(False)

    def _cleanup(self):
        """Clean up members that might inhibit garbage collection"""
        self._num_published = 0
        self._delivery_info = collections.deque()

    @property
    def channel_number(self):
        """Channel number"""
        return self._impl.channel_number

    @property
    def connection(self):
        """The channel's BlockingConnection instance"""
        return self._connection

    @property
    def is_closed(self):
        """Returns True if the channel is closed.

        :rtype: bool

        """
        return self._impl.is_closed

    @property
    def is_open(self):
        """Returns True if the channel is open.

        :rtype: bool

        """
        return self._impl.is_open

    def _on_puback_message_returned(self, channel, method, properties, body):
        """Called as the result of Basic.Return from broker in
        publisher-acknowledgements mode. Saves the info as a ReturnedMessage
        instance in self._puback_return.

        :param pika.Channel channel: our self._impl channel
        :param pika.spec.Basic.Return method:
        :param pika.spec.BasicProperties properties: message properties
        :param body: returned message body; empty string if no body
        :type body: str, unicode

        """
        assert channel is self._impl, (
            channel.channel_number, self.channel_number)

        assert isinstance(method, pika.spec.Basic.Return), method
        assert isinstance(properties, pika.spec.BasicProperties), (
            properties)

        LOGGER.warning(
            "Published message was returned: _delivery_confirmation=%s; "
            "channel=%s; method=%r; properties=%r; body_size=%d; "
            "body_prefix=%.255r", self._delivery_confirmation,
            channel.channel_number, method, properties,
            len(body) if body is not None else None, body)

        self._puback_return = ReturnedMessage(method, properties, body)

    def _on_channel_closed(self, _channel, reason):
        """Callback from impl notifying us that the channel has been closed.
        This may be as the result of user-, broker-, or internal connection
        clean-up initiated closing or meta-closing of the channel.

        See `pika.Channel.add_on_close_callback()` for additional documentation.

        :param pika.Channel _channel: (unused)
        :param ChannelClosed reason: The reason for the channel closing
        """
        closed_by_broker = isinstance(reason, pika.exceptions.ChannelClosedByBroker)

        LOGGER.debug('_on_channel_closed: by_broker=%s; (%s) %s; %r',
                     closed_by_broker,
                     reason.reply_code,
                     reason.reply_text,
                     self)

        if closed_by_broker:
            # Set exceptions on all the pending published message futures
            self._cleanup()
        else:
            if self._close_future:
                self._close_future.set_result(reason)
        self._future_store.reject_all(reason)

    @coroutine
    def close(self, reply_code=0, reply_text="Normal shutdown"):
        """Will invoke a clean shutdown of the channel with the AMQP Broker.

        :param int reply_code: The reply code to close the channel with
        :param str reply_text: The reply text to close the channel with

        """
        LOGGER.debug('Channel.close(%s, %s)', reply_code, reply_text)

        self._raise_if_not_open()
        self._close_future = Future()

        try:
            # Close the channel
            self._impl.close(reply_code=reply_code, reply_text=reply_text)
            yield self._close_future
        except Exception as e:
            raise
        finally:
            self._cleanup()

    @coroutine
    def flow(self, active):
        """Turn Channel flow control off and on.

        NOTE: RabbitMQ doesn't support active=False; per
        https://www.rabbitmq.com/specification.html: "active=false is not
        supported by the server. Limiting prefetch with basic.qos provides much
        better control"

        For more information, please reference:

        http://www.rabbitmq.com/amqp-0-9-1-reference.html#channel.flow

        :param bool active: Turn flow on (True) or off (False)

        :returns: True if broker will start or continue sending; False if not
        :rtype: bool

        """
        with self._pending_future() as flow_ok_result:
            self._impl.flow(active=active,
                            callback=flow_ok_result.set_result)
            yield flow_ok_result
            raise Return(flow_ok_result.result())

    def add_on_cancel_callback(self, callback):
        """Pass a callback function that will be called when Basic.Cancel
        is sent by the broker. The callback function should receive a method
        frame parameter.

        :param callable callback: a callable for handling broker's Basic.Cancel
            notification with the call signature: callback(method_frame)
            where method_frame is of type `pika.frame.Method` with method of
            type `spec.Basic.Cancel`

        """
        self._impl.add_on_cancel_callback(callback)

    def add_on_close_callback(self, callback):
        """Pass a callback function that will be called when the channel is
        closed. The callback function will receive the channel, the
        reply_code (int) and the reply_text (string) describing why the channel was
        closed.

        If the channel is closed by broker via Channel.Close, the callback will
        receive the reply_code/reply_text provided by the broker.

        If channel closing is initiated by user (either directly of indirectly
        by closing a connection containing the channel) and closing
        concludes gracefully without Channel.Close from the broker and without
        loss of connection, the callback will receive 0 as reply_code and empty
        string as reply_text.

        If channel was closed due to loss of connection, the callback will
        receive reply_code and reply_text representing the loss of connection.

        :param callable callback: The callback, having the signature:
            callback(Channel, int reply_code, str reply_text)

        """
        self._impl.add_on_close_callback(common._CallbackWrapper(self, callback))

    def add_on_return_callback(self, callback):
        """Pass a callback function that will be called when a published
        message is rejected and returned by the server via `Basic.Return`.

        :param callable callback: The method to call on callback with the
            signature callback(channel, method, properties, body), where
            channel: pika.Channel
            method: pika.spec.Basic.Return
            properties: pika.spec.BasicProperties
            body: str, unicode, or bytes (python 3.x)

        """
        self._impl.add_on_return_callback(common._CallbackWrapper(self, callback))

    @coroutine
    def basic_consume(self,
                      queue,
                      on_message_callback,
                      auto_ack=False,
                      exclusive=False,
                      consumer_tag=None,
                      arguments=None):
        """Sends the AMQP command Basic.Consume to the broker and binds messages
        for the consumer_tag to the consumer callback. If you do not pass in
        a consumer_tag, one will be automatically generated for you. Returns
        the consumer tag.

        NOTE: the consumer callbacks are dispatched only in the scope of
        specially-designated methods: see
        `BlockingConnection.process_data_events` and
        `BlockingChannel.start_consuming`.

        For more information about Basic.Consume, see:
        http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.consume

        :param queue: The queue from which to consume
        :type queue: str or unicode
        :param callable on_message_callback: Required function for dispatching messages
            to user, having the signature:
            on_message_callback(channel, method, properties, body)
                channel: BlockingChannel
                method: spec.Basic.Deliver
                properties: spec.BasicProperties
                body: str or unicode
        :param bool auto_ack: if set to True, automatic acknowledgement mode will be used
                              (see http://www.rabbitmq.com/confirms.html). This corresponds
                              with the 'no_ack' parameter in the basic.consume AMQP 0.9.1
                              method
        :param bool exclusive: Don't allow other consumers on the queue
        :param consumer_tag: You may specify your own consumer tag; if left
          empty, a consumer tag will be generated automatically
        :type consumer_tag: str or unicode
        :param dict arguments: Custom key/value pair arguments for the consumer
        :returns: consumer tag
        :rtype: str

        :raises pika.exceptions.DuplicateConsumerTag: if consumer with given
            consumer_tag is already present.

        """
        if not callable(on_message_callback):
            raise ValueError('callback on_message_callback must be callable; got %r'
                             % on_message_callback)

        with self._pending_reply(pika.spec.Basic.ConsumeOk) as ok_result:
            tag = self._impl.basic_consume(
                on_message_callback=common._CallbackWrapper(self, on_message_callback),
                queue=queue,
                auto_ack=auto_ack,
                exclusive=exclusive,
                consumer_tag=consumer_tag,
                arguments=arguments)

            yield ok_result
            raise Return(tag)

    @coroutine
    def basic_cancel(self, consumer_tag=""):
        """This method cancels a consumer. This does not affect already
        delivered messages, but it does mean the server will not send any more
        messages for that consumer. The client may receive an arbitrary number
        of messages in between sending the cancel method and receiving the
        cancel-ok reply.

        NOTE: When cancelling an auto_ack=False consumer, this implementation
        automatically Nacks and suppresses any incoming messages that have not
        yet been dispatched to the consumer's callback. However, when cancelling
        a auto_ack=True consumer, this method will return any pending messages
        that arrived before broker confirmed the cancellation.

        :param str consumer_tag: Identifier for the consumer; the result of
            passing a consumer_tag that was created on another channel is
            undefined (bad things will happen)

        :returns: (NEW IN pika 0.10.0) empty sequence for a auto_ack=False
            consumer; for a auto_ack=True consumer, returns a (possibly empty)
            sequence of pending messages that arrived before broker confirmed
            the cancellation (this is done instead of via consumer's callback in
            order to prevent reentrancy/recursion. Each message is four-tuple:
            (channel, method, properties, body)
                channel: BlockingChannel
                method: spec.Basic.Deliver
                properties: spec.BasicProperties
                body: str or unicode
        """
        with self._pending_future() as cancel_ok_result:
            # Cancel the consumer; impl takes care of rejecting any
            # additional deliveries that arrive for a auto_ack=False
            # consumer
            self._impl.basic_cancel(
                consumer_tag=consumer_tag,
                callback=cancel_ok_result.set_result)

            # Flush output and wait for Basic.Cancel-ok or
            # broker-initiated Basic.Cancel
            yield cancel_ok_result

    @coroutine
    def basic_ack(self, delivery_tag=0, multiple=False):
        """Acknowledge one or more messages. When sent by the client, this
        method acknowledges one or more messages delivered via the Deliver or
        Get-Ok methods. When sent by server, this method acknowledges one or
        more messages published with the Publish method on a channel in
        confirm mode. The acknowledgement can be for a single message or a
        set of messages up to and including a specific message.

        :param int delivery_tag: The server-assigned delivery tag
        :param bool multiple: If set to True, the delivery tag is treated as
                              "up to and including", so that multiple messages
                              can be acknowledged with a single method. If set
                              to False, the delivery tag refers to a single
                              message. If the multiple field is 1, and the
                              delivery tag is zero, this indicates
                              acknowledgement of all outstanding messages.
        """
        self._impl.basic_ack(delivery_tag=delivery_tag, multiple=multiple)

    @coroutine
    def basic_nack(self, delivery_tag=None, multiple=False, requeue=True):
        """This method allows a client to reject one or more incoming messages.
        It can be used to interrupt and cancel large incoming messages, or
        return untreatable messages to their original queue.

        :param int delivery-tag: The server-assigned delivery tag
        :param bool multiple: If set to True, the delivery tag is treated as
                              "up to and including", so that multiple messages
                              can be acknowledged with a single method. If set
                              to False, the delivery tag refers to a single
                              message. If the multiple field is 1, and the
                              delivery tag is zero, this indicates
                              acknowledgement of all outstanding messages.
        :param bool requeue: If requeue is true, the server will attempt to
                             requeue the message. If requeue is false or the
                             requeue attempt fails the messages are discarded or
                             dead-lettered.

        """
        self._impl.basic_nack(delivery_tag=delivery_tag, multiple=multiple,
                              requeue=requeue)

    @coroutine
    def basic_get(self, queue, auto_ack=False):
        """Get a single message from the AMQP broker. Returns a sequence with
        the method frame, message properties, and body.

        :param queue: Name of queue from which to get a message
        :type queue: str or unicode
        :param bool auto_ack: Tell the broker to not expect a reply
        :returns: a three-tuple; (None, None, None) if the queue was empty;
            otherwise (method, properties, body); NOTE: body may be None
        :rtype: (None, None, None)|(spec.Basic.GetOk,
                                    spec.BasicProperties,
                                    str or unicode or None)
        """
        # NOTE: nested with for python 2.6 compatibility
        with self._pending_future() as get_ok_result:
            with self._pending_reply(pika.spec.Basic.GetEmpty) as basic_getempty_result:
                self._impl.basic_get(queue=queue,
                                     auto_ack=auto_ack,
                                     callback=lambda ch, meth, props, body:
                                     get_ok_result.set_result(self._RxMessageArgs(ch, meth, props, body)))

                # Get whichever finishes first
                yield WaitIterator(get_ok_result, basic_getempty_result).next()

                if get_ok_result.done():
                    evt = get_ok_result.result()
                    raise Return((evt.method, evt.properties, evt.body))
                else:
                    assert basic_getempty_result.done(), (
                        "wait completed without GetOk and GetEmpty")
                    raise Return((None, None, None))

    @coroutine
    def basic_publish(self, exchange, routing_key, body,
                      properties=None,
                      mandatory=False,
                      immediate=False):
        """Publish to the channel with the given exchange, routing key and body.
        Returns a boolean value indicating the success of the operation.

        This is the legacy BlockingChannel method for publishing. See also
        `BlockingChannel.publish` that provides more information about failures.

        For more information on basic_publish and what the parameters do, see:

            http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.publish

        NOTE: mandatory and immediate may be enabled even without delivery
          confirmation, but in the absence of delivery confirmation the
          synchronous implementation has no way to know how long to wait for
          the Basic.Return or lack thereof.

        :param exchange: The exchange to publish to
        :type exchange: str or unicode
        :param routing_key: The routing key to bind on
        :type routing_key: str or unicode
        :param body: The message body; empty string if no body
        :type body: bytes
        :param pika.spec.BasicProperties properties: message properties
        :param bool mandatory: The mandatory flag
        :param bool immediate: The immediate flag

        :returns: True if delivery confirmation is not enabled (NEW in pika
            0.10.0); otherwise returns False if the message could not be
            delivered (Basic.nack and/or Basic.Return) and True if the message
            was delivered (Basic.ack and no Basic.Return)
        """
        try:
            yield self.publish(exchange, routing_key, body, properties,
                               mandatory, immediate)
        except (pika.exceptions.NackError, pika.exceptions.UnroutableError):
            raise Return(False)
        else:
            raise Return(True)

    @coroutine
    def publish(self, exchange, routing_key, body,
                properties=None, mandatory=False, immediate=False):
        """Publish to the channel with the given exchange, routing key, and
        body.

        For more information on basic_publish and what the parameters do, see:

            http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.publish

        NOTE: mandatory and immediate may be enabled even without delivery
          confirmation, but in the absence of delivery confirmation the
          synchronous implementation has no way to know how long to wait for
          the Basic.Return.

        :param exchange: The exchange to publish to
        :type exchange: str or unicode
        :param routing_key: The routing key to bind on
        :type routing_key: str or unicode
        :param body: The message body; empty string if no body
        :type body: bytes
        :param pika.spec.BasicProperties properties: message properties
        :param bool mandatory: The mandatory flag
        :param bool immediate: The immediate flag

        :raises UnroutableError: raised when a message published in
            publisher-acknowledgments mode (see
            `BlockingChannel.confirm_delivery`) is returned via `Basic.Return`
            followed by `Basic.Ack`.
        :raises NackError: raised when a message published in
            publisher-acknowledgements mode is Nack'ed by the broker. See
            `BlockingChannel.confirm_delivery`.

        """
        if self._delivery_confirmation:
            # In publisher-acknowledgments mode

            with self._pending_future() as delivery_future:
                self._num_published += 1
                self._delivery_info.append(DeliveryInfo(self._num_published, delivery_future))

                self._impl.basic_publish(exchange=exchange,
                                         routing_key=routing_key,
                                         body=body,
                                         properties=properties,
                                         mandatory=mandatory,
                                         immediate=immediate)

                result = yield delivery_future

            if result == pika.spec.Basic.Nack.NAME:
                # Broker was unable to process the message due to internal
                # error
                LOGGER.warning(
                    "Message was Nack'ed by broker: nack=%r; channel=%s; "
                    "exchange=%s; routing_key=%s; mandatory=%r; "
                    "immediate=%r", result, self.channel_number,
                    exchange, routing_key, mandatory, immediate)
                if self._puback_return is not None:
                    returned_messages = [self._puback_return]
                    self._puback_return = None
                else:
                    returned_messages = []
                raise pika.exceptions.NackError(returned_messages)

            else:
                assert result == pika.spec.Basic.Ack.NAME, result

                if self._puback_return is not None:
                    # Unroutable message was returned
                    messages = [self._puback_return]
                    self._puback_return = None
                    raise pika.exceptions.UnroutableError(messages)
        else:
            # In non-publisher-acknowledgments mode
            self._impl.basic_publish(exchange=exchange,
                                     routing_key=routing_key,
                                     body=body,
                                     properties=properties,
                                     mandatory=mandatory,
                                     immediate=immediate)

    @coroutine
    def basic_qos(self, prefetch_size=0, prefetch_count=0, all_channels=False):
        """Specify quality of service. This method requests a specific quality
        of service. The QoS can be specified for the current channel or for all
        channels on the connection. The client can request that messages be sent
        in advance so that when the client finishes processing a message, the
        following message is already held locally, rather than needing to be
        sent down the channel. Prefetching gives a performance improvement.

        :param int prefetch_size:  This field specifies the prefetch window
                                   size. The server will send a message in
                                   advance if it is equal to or smaller in size
                                   than the available prefetch size (and also
                                   falls into other prefetch limits). May be set
                                   to zero, meaning "no specific limit",
                                   although other prefetch limits may still
                                   apply. The prefetch-size is ignored if the
                                   no-ack option is set in the consumer.
        :param int prefetch_count: Specifies a prefetch window in terms of whole
                                   messages. This field may be used in
                                   combination with the prefetch-size field; a
                                   message will only be sent in advance if both
                                   prefetch windows (and those at the channel
                                   and connection level) allow it. The
                                   prefetch-count is ignored if the no-ack
                                   option is set in the consumer.
        :param bool all_channels: Should the QoS apply to all channels

        """
        with self._pending_future() as qos_ok_result:
            self._impl.basic_qos(callback=qos_ok_result.set_result,
                                 prefetch_size=prefetch_size,
                                 prefetch_count=prefetch_count,
                                 all_channels=all_channels)

            result = yield qos_ok_result
            raise Return(result)

    @coroutine
    def basic_reject(self, delivery_tag=None, requeue=True):
        """Reject an incoming message. This method allows a client to reject a
        message. It can be used to interrupt and cancel large incoming messages,
        or return untreatable messages to their original queue.

        :param int delivery_tag: The server-assigned delivery tag
        :param bool requeue: If requeue is true, the server will attempt to
                             requeue the message. If requeue is false or the
                             requeue attempt fails the messages are discarded or
                             dead-lettered.

        """
        self._impl.basic_reject(delivery_tag=delivery_tag, requeue=requeue)

    @coroutine
    def confirm_delivery(self):
        """Turn on RabbitMQ-proprietary Confirm mode in the channel.

        For more information see:
            http://www.rabbitmq.com/extensions.html#confirms
        """
        if self._delivery_confirmation:
            LOGGER.error('confirm_delivery: confirmation was already enabled '
                         'on channel=%s', self.channel_number)
            return

        with self._pending_future() as confirm_future:
            self._impl.confirm_delivery(
                self._on_message_ack_nack,
                callback=confirm_future.set_result)

            # Wait for the channel to respond
            yield confirm_future
        self._delivery_confirmation = True

        # Unroutable messages returned after this point will be in the context
        # of publisher acknowledgments
        self._impl.add_on_return_callback(self._on_puback_message_returned)

    @coroutine
    def exchange_declare(self, exchange=None,
                         exchange_type='direct', passive=False, durable=False,
                         auto_delete=False, internal=False,
                         arguments=None):
        """This method creates an exchange if it does not already exist, and if
        the exchange exists, verifies that it is of the correct and expected
        class.

        If passive set, the server will reply with Declare-Ok if the exchange
        already exists with the same name, and raise an error if not and if the
        exchange does not already exist, the server MUST raise a channel
        exception with reply code 404 (not found).

        :param exchange: The exchange name consists of a non-empty sequence of
                          these characters: letters, digits, hyphen, underscore,
                          period, or colon.
        :type exchange: str or unicode
        :param str exchange_type: The exchange type to use
        :param bool passive: Perform a declare or just check to see if it exists
        :param bool durable: Survive a reboot of RabbitMQ
        :param bool auto_delete: Remove when no more queues are bound to it
        :param bool internal: Can only be published to by other exchanges
        :param dict arguments: Custom key/value pair arguments for the exchange

        :returns: Method frame from the Exchange.Declare-ok response
        :rtype: `pika.frame.Method` having `method` attribute of type
          `spec.Exchange.DeclareOk`

        """
        with self._pending_future() as declare_ok_result:
            self._impl.exchange_declare(
                callback=declare_ok_result.set_result,
                exchange=exchange,
                exchange_type=exchange_type,
                passive=passive,
                durable=durable,
                auto_delete=auto_delete,
                internal=internal,
                arguments=arguments)

            yield declare_ok_result
            raise Return(declare_ok_result.result())

    @coroutine
    def exchange_delete(self, exchange=None, if_unused=False):
        """Delete the exchange.

        :param exchange: The exchange name
        :type exchange: str or unicode
        :param bool if_unused: only delete if the exchange is unused

        :returns: Method frame from the Exchange.Delete-ok response
        :rtype: `pika.frame.Method` having `method` attribute of type
          `spec.Exchange.DeleteOk`

        """
        with self._pending_future() as delete_ok_result:
            self._impl.exchange_delete(
                exchange=exchange,
                if_unused=if_unused,
                callback=delete_ok_result.set_result)

            yield delete_ok_result
            raise Return(delete_ok_result.result())

    @coroutine
    def exchange_bind(self, destination=None, source=None, routing_key='',
                      arguments=None):
        """Bind an exchange to another exchange.

        :param destination: The destination exchange to bind
        :type destination: str or unicode
        :param source: The source exchange to bind to
        :type source: str or unicode
        :param routing_key: The routing key to bind on
        :type routing_key: str or unicode
        :param dict arguments: Custom key/value pair arguments for the binding

        :returns: Method frame from the Exchange.Bind-ok response
        :rtype: `pika.frame.Method` having `method` attribute of type
          `spec.Exchange.BindOk`

        """
        with self._pending_future() as \
                bind_ok_result:
            self._impl.exchange_bind(
                destination=destination,
                source=source,
                routing_key=routing_key,
                arguments=arguments,
                callback=bind_ok_result.set_result)

            yield bind_ok_result
            raise Return(bind_ok_result.result())

    @coroutine
    def exchange_unbind(self, destination=None, source=None, routing_key='',
                        arguments=None):
        """Unbind an exchange from another exchange.

        :param destination: The destination exchange to unbind
        :type destination: str or unicode
        :param source: The source exchange to unbind from
        :type source: str or unicode
        :param routing_key: The routing key to unbind
        :type routing_key: str or unicode
        :param dict arguments: Custom key/value pair arguments for the binding

        :returns: Method frame from the Exchange.Unbind-ok response
        :rtype: `pika.frame.Method` having `method` attribute of type
          `spec.Exchange.UnbindOk`

        """
        with self._pending_future() as unbind_ok_result:
            self._impl.exchange_unbind(
                destination=destination,
                source=source,
                routing_key=routing_key,
                arguments=arguments,
                callback=unbind_ok_result.set_result)

            yield unbind_ok_result
            raise Return(unbind_ok_result.result())

    @coroutine
    def queue_declare(self, queue, passive=False, durable=False,
                      exclusive=False, auto_delete=False,
                      arguments=None):
        """Declare queue, create if needed. This method creates or checks a
        queue. When creating a new queue the client can specify various
        properties that control the durability of the queue and its contents,
        and the level of sharing for the queue.

        Use an empty string as the queue name for the broker to auto-generate
        one. Retrieve this auto-generated queue name from the returned
        `spec.Queue.DeclareOk` method frame.

        :param queue: The queue name
        :type queue: str or unicode; if empty string, the broker will create a
          unique queue name;
        :param bool passive: Only check to see if the queue exists and raise
          `ChannelClosed` if it doesn't;
        :param bool durable: Survive reboots of the broker
        :param bool exclusive: Only allow access by the current connection
        :param bool auto_delete: Delete after consumer cancels or disconnects
        :param dict arguments: Custom key/value arguments for the queue

        :returns: Method frame from the Queue.Declare-ok response
        :rtype: `pika.frame.Method` having `method` attribute of type
          `spec.Queue.DeclareOk`

        """
        with self._pending_future() as \
                declare_ok_result:
            self._impl.queue_declare(
                queue=queue,
                passive=passive,
                durable=durable,
                exclusive=exclusive,
                auto_delete=auto_delete,
                arguments=arguments,
                callback=declare_ok_result.set_result)

            yield declare_ok_result
            raise Return(declare_ok_result.result())

    @coroutine
    def queue_delete(self, queue, if_unused=False, if_empty=False):
        """Delete a queue from the broker.

        :param queue: The queue to delete
        :type queue: str or unicode
        :param bool if_unused: only delete if it's unused
        :param bool if_empty: only delete if the queue is empty

        :returns: Method frame from the Queue.Delete-ok response
        :rtype: `pika.frame.Method` having `method` attribute of type
          `spec.Queue.DeleteOk`

        """
        with self._pending_future() as \
                delete_ok_result:
            self._impl.queue_delete(queue=queue,
                                    if_unused=if_unused,
                                    if_empty=if_empty,
                                    callback=delete_ok_result.set_result)

            yield delete_ok_result
            raise Return(delete_ok_result.result())

    @coroutine
    def queue_purge(self, queue):
        """Purge all of the messages from the specified queue

        :param queue: The queue to purge
        :type  queue: str or unicode

        :returns: Method frame from the Queue.Purge-ok response
        :rtype: `pika.frame.Method` having `method` attribute of type
          `spec.Queue.PurgeOk`

        """
        with self._pending_future() as \
                purge_ok_result:
            self._impl.queue_purge(queue=queue,
                                   callback=purge_ok_result.set_result)
            yield purge_ok_result
            raise Return(purge_ok_result.result())

    @coroutine
    def queue_bind(self, queue, exchange, routing_key=None,
                   arguments=None):
        """Bind the queue to the specified exchange

        :param queue: The queue to bind to the exchange
        :type queue: str or unicode
        :param exchange: The source exchange to bind to
        :type exchange: str or unicode
        :param routing_key: The routing key to bind on
        :type routing_key: str or unicode
        :param dict arguments: Custom key/value pair arguments for the binding

        :returns: Method frame from the Queue.Bind-ok response
        :rtype: `pika.frame.Method` having `method` attribute of type
          `spec.Queue.BindOk`

        """
        with self._pending_future() as \
                bind_ok_result:
            self._impl.queue_bind(queue=queue,
                                  exchange=exchange,
                                  routing_key=routing_key,
                                  arguments=arguments,
                                  callback=bind_ok_result.set_result)
            yield bind_ok_result
            raise Return(bind_ok_result.result())

    @coroutine
    def queue_unbind(self, queue, exchange=None, routing_key=None,
                     arguments=None):
        """Unbind a queue from an exchange.

        :param queue: The queue to unbind from the exchange
        :type queue: str or unicode
        :param exchange: The source exchange to bind from
        :type exchange: str or unicode
        :param routing_key: The routing key to unbind
        :type routing_key: str or unicode
        :param dict arguments: Custom key/value pair arguments for the binding

        :returns: Method frame from the Queue.Unbind-ok response
        :rtype: `pika.frame.Method` having `method` attribute of type
          `spec.Queue.UnbindOk`

        """
        with self._pending_future() as \
                unbind_ok_result:
            self._impl.queue_unbind(queue=queue,
                                    exchange=exchange,
                                    routing_key=routing_key,
                                    arguments=arguments,
                                    callback=unbind_ok_result.set_result)
            yield unbind_ok_result
            raise Return(unbind_ok_result.result())

    @coroutine
    def tx_select(self):
        """Select standard transaction mode. This method sets the channel to use
        standard transactions. The client must use this method at least once on
        a channel before using the Commit or Rollback methods.

        :returns: Method frame from the Tx.Select-ok response
        :rtype: `pika.frame.Method` having `method` attribute of type
          `spec.Tx.SelectOk`

        """
        with self._pending_future() as \
                select_ok_result:
            self._impl.tx_select(select_ok_result.set_result)

            yield select_ok_result
            raise Return(select_ok_result.result())

    @coroutine
    def tx_commit(self):
        """Commit a transaction.

        :returns: Method frame from the Tx.Commit-ok response
        :rtype: `pika.frame.Method` having `method` attribute of type
          `spec.Tx.CommitOk`

        """
        with self._pending_future() as \
                commit_ok_result:
            self._impl.tx_commit(commit_ok_result.set_result)

            yield commit_ok_result
            raise Return(commit_ok_result.result())

    @coroutine
    def tx_rollback(self):
        """Rollback a transaction.

        :returns: Method frame from the Tx.Commit-ok response
        :rtype: `pika.frame.Method` having `method` attribute of type
          `spec.Tx.CommitOk`

        """
        with self._pending_future() as \
                rollback_ok_result:
            self._impl.tx_rollback(rollback_ok_result.set_result)

            yield rollback_ok_result
            raise Return(rollback_ok_result.result())

    def _on_message_ack_nack(self, frame):
        if frame.method.multiple:
            num_confirmed = frame.method.delivery_tag - self._delivery_info[0].tag + 1
        else:
            assert frame.method.delivery_tag == self._delivery_info[0].tag
            num_confirmed = 1
        for _ in range(num_confirmed):
            self._delivery_info.popleft().future.set_result(frame.method.NAME)

    def _raise_if_not_open(self):
        if not self.is_open:
            raise pika.exceptions.ChannelClosed()

    @contextlib.contextmanager
    def _pending_future(self):
        with self._future_store.pending_future() as future:
            yield future

    @contextlib.contextmanager
    def _pending_reply(self, reply):
        with self._pending_future() as future:
            self._impl.add_callback(future.set_result, replies=(reply,), one_shot=True)
            yield future
