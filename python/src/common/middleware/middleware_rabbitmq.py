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
    """Base común para las variantes de cola y exchange sobre RabbitMQ.

    Concentra el ciclo de vida compartido (conexión, consumo y cierre). Las
    subclases solo aportan la declaración de la topología (cola o exchange)
    y de qué cola consumen.
    """

    def __init__(self, host):
        self.host = host
        self.consumer_tag = None

        self._connection, self._channel = _rabbitmq_call(self._create_channel, host)
        try:
            self._setup_topology()
        except Exception:
            # Si la declaración falla, se libera la conexión ya abierta
            self.close()
            raise

    @staticmethod
    def _create_channel(host):
        """Crea una conexión y un canal de comunicación con RabbitMQ."""
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        return connection, connection.channel()

    def _setup_topology(self):
        """Declara la topología propia de cada subclase (cola o exchange)."""
        raise NotImplementedError

    def _consume_from(self, queue_name, on_message_callback):
        """Inicia el consumo desde una cola concreta.

        `on_message_callback` recibe tres argumentos: el cuerpo del mensaje, una
        función para confirmarlo (ack) y otra para rechazarlo (nack). Así el
        consumidor decide si el mensaje fue procesado correctamente o no.
        """
        self.consumer_tag = _rabbitmq_call(
            self._channel.basic_consume,
            queue=queue_name,
            on_message_callback=_create_message_handler(on_message_callback),
            auto_ack=False,
        )
        _rabbitmq_call(self._channel.start_consuming)

    def stop_consuming(self):
        """Detiene el consumo de mensajes. Si no se está consumiendo, no hace nada."""
        if self._channel.is_open and self.consumer_tag:
            _rabbitmq_call(self._channel.basic_cancel, self.consumer_tag)
            self.consumer_tag = None
            _rabbitmq_call(self._channel.stop_consuming)

    def request_stop(self):
        """Igual que `stop_consuming`, pero seguro de invocar desde un hilo
        distinto al que está corriendo `start_consuming`."""
        if self._connection.is_open:
            self._connection.add_callback_threadsafe(self.stop_consuming)

    def close(self):
        """Cierra el canal y la conexión de RabbitMQ si siguen abiertos."""
        if self._channel.is_open:
            _rabbitmq_call(self._channel.close, error=MessageMiddlewareCloseError)
        if self._connection.is_open:
            _rabbitmq_call(self._connection.close, error=MessageMiddlewareCloseError)


class MessageMiddlewareQueueRabbitMQ(
    _MessageMiddlewareRabbitMQ, MessageMiddlewareQueue
):
    def __init__(self, host, queue_name):
        """Crea la conexión y declara la cola en RabbitMQ."""
        self.queue_name = queue_name
        super().__init__(host)

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


class MessageMiddlewareExchangeRabbitMQ(
    _MessageMiddlewareRabbitMQ, MessageMiddlewareExchange
):
    def __init__(self, host, exchange_name, routing_keys):
        """Crea la conexión y declara el exchange en RabbitMQ"""
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        super().__init__(host)

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

    def start_consuming(self, on_message_callback):
        """Inicia el consumo de mensajes del exchange. Se crea una cola anónima
        y exclusiva para este consumidor, y se vincula a las routing keys configuradas.
        """
        # Cola anónima y exclusiva para el suscriptor.
        result = _rabbitmq_call(self._channel.queue_declare, queue="", exclusive=True)
        queue_name = result.method.queue

        for routing_key in self.routing_keys:
            _rabbitmq_call(
                self._channel.queue_bind,
                exchange=self.exchange_name,
                queue=queue_name,
                routing_key=routing_key,
            )

        self._consume_from(queue_name, on_message_callback)
