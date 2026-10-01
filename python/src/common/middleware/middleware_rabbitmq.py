import pika
from pika.exceptions import AMQPConnectionError, AMQPError

from .middleware import (
    MessageMiddleware,
    MessageMiddlewareCloseError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareQueue,
)

EXCHANGE_TYPE = "direct"


def _rabbitmq_call(function, *args, error=MessageMiddlewareMessageError, **kwargs):
    """Ejecuta una llamada a RabbitMQ y traduce sus errores a los del middleware."""
    try:
        return function(*args, **kwargs)
    except AMQPConnectionError as e:
        raise MessageMiddlewareDisconnectedError from e
    except AMQPError as e:
        raise error from e


def _create_message_handler(on_message_callback):
    """Adapta el callback de RabbitMQ al callback del middleware."""

    def handle_message(channel, method, _properties, body):
        ack = lambda: channel.basic_ack(delivery_tag=method.delivery_tag)
        nack = lambda: channel.basic_nack(delivery_tag=method.delivery_tag)
        on_message_callback(body, ack, nack)

    return handle_message


class _MessageMiddlewareRabbitMQ(MessageMiddleware):
    """Base común de cola y exchange: conexión, consumo y cierre."""

    def __init__(self, host, channel=None):
        self.host = host
        self.consumer_tag = None

        if channel is not None:
            # Canal compartido: se consume todo desde un único `pump_forever()`.
            self._connection = None
            self._channel = channel
        else:
            self._connection, self._channel = _rabbitmq_call(self._create_channel, host)
        try:
            self._setup_topology()
        except Exception:
            # Si la declaración falla, se libera la conexión ya abierta
            self.close()
            raise

    @property
    def channel(self):
        """Expone el canal para que otro middleware lo comparta (`channel=`)."""
        return self._channel

    @staticmethod
    def _create_channel(host):
        """Crea una conexión y un canal de comunicación con RabbitMQ."""
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        return connection, connection.channel()

    def _setup_topology(self):
        """Declara la topología propia de cada subclase (cola o exchange)."""
        raise NotImplementedError

    def _register_consumer(self, queue_name, on_message_callback):
        """Registra el consumo de una cola, sin bloquear."""
        self.consumer_tag = _rabbitmq_call(
            self._channel.basic_consume,
            queue=queue_name,
            on_message_callback=_create_message_handler(on_message_callback),
            auto_ack=False,
        )

    def _consume_from(self, queue_name, on_message_callback):
        """Consume de una cola. El callback recibe (cuerpo, ack, nack)."""
        self._register_consumer(queue_name, on_message_callback)
        self.pump_forever()

    def pump_forever(self):
        """Bloquea despachando los mensajes de todos los consumidores del canal."""
        _rabbitmq_call(self._channel.start_consuming)

    def call_later(self, delay_seconds, callback):
        """Ejecuta `callback` en el hilo que consume tras `delay_seconds`.
        Solo lo puede usar el middleware dueño de la conexión."""
        if self._connection is None:
            raise MessageMiddlewareMessageError(
                "call_later requiere el middleware que creó la conexión"
            )
        _rabbitmq_call(self._connection.call_later, delay_seconds, callback)

    def stop_consuming(self):
        """Detiene el consumo de mensajes. Si no se está consumiendo, no hace nada."""
        if self._channel.is_open and self.consumer_tag:
            _rabbitmq_call(self._channel.basic_cancel, self.consumer_tag)
            self.consumer_tag = None
            _rabbitmq_call(self._channel.stop_consuming)

    def close(self):
        """Cierra el canal y la conexión (esta última solo si es la dueña)."""
        if self._channel.is_open:
            _rabbitmq_call(self._channel.close, error=MessageMiddlewareCloseError)
        if self._connection is not None and self._connection.is_open:
            _rabbitmq_call(self._connection.close, error=MessageMiddlewareCloseError)


class MessageMiddlewareQueueRabbitMQ(
    _MessageMiddlewareRabbitMQ, MessageMiddlewareQueue
):
    def __init__(self, host, queue_name, channel=None):
        self.queue_name = queue_name
        super().__init__(host, channel=channel)

    def _setup_topology(self):
        _rabbitmq_call(self._channel.queue_declare, queue=self.queue_name, durable=True)

    def send(self, message):
        """Envía un mensaje a la cola asociada a esta instancia."""
        _rabbitmq_call(
            self._channel.basic_publish,
            exchange="",
            routing_key=self.queue_name,
            body=message,
        )

    def start_consuming(self, on_message_callback):
        """Inicia el consumo de mensajes de la cola asociada a esta instancia."""
        self._consume_from(self.queue_name, on_message_callback)

    def register_consumer(self, on_message_callback):
        """Registra el consumo de esta cola sin bloquear."""
        self._register_consumer(self.queue_name, on_message_callback)


class MessageMiddlewareExchangeRabbitMQ(
    _MessageMiddlewareRabbitMQ, MessageMiddlewareExchange
):
    def __init__(self, host, exchange_name, routing_keys, channel=None):
        """Crea la conexión y declara el exchange en RabbitMQ"""
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self._consumer_queue_name = None
        super().__init__(host, channel=channel)

    def _setup_topology(self):
        _rabbitmq_call(
            self._channel.exchange_declare,
            exchange=self.exchange_name,
            exchange_type=EXCHANGE_TYPE,
            durable=True,
        )

    def send(self, message):
        """Envía el mensaje al exchange una vez por cada routing key configurada."""
        for routing_key in self.routing_keys:
            _rabbitmq_call(
                self._channel.basic_publish,
                exchange=self.exchange_name,
                routing_key=routing_key,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent
                ),
            )

    def _ensure_consumer_queue(self):
        """Crea una vez la cola exclusiva asociada a las routing keys."""
        if self._consumer_queue_name is not None:
            return self._consumer_queue_name

        result = _rabbitmq_call(self._channel.queue_declare, queue="", exclusive=True)
        queue_name = result.method.queue

        for routing_key in self.routing_keys:
            _rabbitmq_call(
                self._channel.queue_bind,
                exchange=self.exchange_name,
                queue=queue_name,
                routing_key=routing_key,
            )

        self._consumer_queue_name = queue_name
        return queue_name

    def start_consuming(self, on_message_callback):
        """Consume del exchange y bloquea."""
        queue_name = self._ensure_consumer_queue()
        self._consume_from(queue_name, on_message_callback)

    def register_consumer(self, on_message_callback):
        """Registra el consumo del exchange, sin bloquear."""
        queue_name = self._ensure_consumer_queue()
        self._register_consumer(queue_name, on_message_callback)
