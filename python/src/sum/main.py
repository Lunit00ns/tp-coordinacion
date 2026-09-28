import logging
import os
import signal
import threading
import zlib

from common import fruit_item, message_protocol, middleware

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]

# Exchange para coordinación interna entre réplicas de Sum
SUM_CONTROL_EXCHANGE = f"{SUM_PREFIX}_control"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

im = message_protocol.internal
logger = logging.getLogger(__name__)


def _aggregation_id(client_id, fruit):
    """Calcula la instancia de Aggregation asignada a una fruta de forma
    determinista."""
    key = f"{client_id}|{fruit}".encode()
    return zlib.crc32(key) % AGGREGATION_AMOUNT


class SumFilter:
    """Suma pares (fruta, cantidad) por cliente y distribuye los totales
    a Aggregation.

    Usa un protocolo de barrera y reportes de progreso entre réplicas de
    Sum para garantizar la recepción completa de los datos antes de flushear.
    """

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )

        self.control_input = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_PREFIX}_ctrl_{ID}"]
        )
        self.control_outputs = [
            middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_PREFIX}_ctrl_{i}"]
            )
            for i in range(SUM_AMOUNT)
        ]
        self.aggregation_outputs = [
            middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            for i in range(AGGREGATION_AMOUNT)
        ]

        self.lock = threading.Lock()
        self.amount_by_client = {}
        self.local_count = {}
        self.total_count = {}
        self.progress_reports = {}
        self.flushed_clients = set()
        self.control_thread = threading.Thread(target=self._run_control)

    # -- Datos --------------------------------------------------------

    def _process_data(self, client_id, fruit, amount):
        client_state = self.amount_by_client.setdefault(client_id, {})
        client_state[fruit] = client_state.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

        self.local_count[client_id] = self.local_count.get(client_id, 0) + 1
        known_total = client_id in self.total_count

        if known_total:
            return self._own_progress_message(client_id)
        return None

    def _own_progress_message(self, client_id):
        count = self.local_count.get(client_id, 0)
        return im.serialize(im.build_sum_progress(client_id, ID, count))

    # -- Barrera de cierre entre réplicas de Sum -----------------------

    def _broadcast(self, message):
        """Publica a todas las réplicas de Sum. Necesita 'self.lock' para
        evitar concurrencia en Pika."""
        for control_output in self.control_outputs:
            control_output.send(message)

    def _on_input_message(self, message, ack, nack):
        try:
            fields = im.deserialize(message)
            client_id = fields["client_id"]

            with self.lock:
                if fields["type"] == im.MsgType.DATA:
                    progress_message = self._process_data(
                        client_id, fields["fruit"], fields["amount"]
                    )
                    if progress_message is not None:
                        self._broadcast(progress_message)
                else:
                    logger.info(f"Broadcasting SUM_BARRIER for client {client_id}")
                    self._broadcast(
                        im.serialize(
                            im.build_sum_barrier(client_id, fields["total_count"])
                        )
                    )
            ack()
        except Exception:
            logger.exception("Error processing input message")
            nack()

    def _on_control_message(self, message, ack, nack):
        try:
            fields = im.deserialize(message)
            client_id = fields["client_id"]

            with self.lock:
                if fields["type"] == im.MsgType.SUM_BARRIER:
                    if client_id not in self.total_count:
                        self.total_count[client_id] = fields["total_count"]
                    self._broadcast(self._own_progress_message(client_id))
                else:
                    reports = self.progress_reports.setdefault(client_id, {})
                    reports[fields["sum_id"]] = fields["count"]

                should_flush = self._is_client_complete(client_id)

            if should_flush:
                self._flush_client(client_id)
            ack()
        except Exception:
            logger.exception("Error processing control message")
            nack()

    def _is_client_complete(self, client_id):
        """Verifica si la suma de los conteos reportados alcanza el total
        del cliente (con lock)."""
        if client_id in self.flushed_clients:
            return False
        if client_id not in self.total_count:
            return False
        reports = self.progress_reports.get(client_id, {})
        if len(reports) < SUM_AMOUNT:
            return False
        return sum(reports.values()) == self.total_count[client_id]

    # -- Reparto hacia Aggregation --------------------------------------

    def _flush_client(self, client_id):
        with self.lock:
            if client_id in self.flushed_clients:
                return
            self.flushed_clients.add(client_id)
            client_state = self.amount_by_client.pop(client_id, {})
            self.local_count.pop(client_id, None)
            self.total_count.pop(client_id, None)
            self.progress_reports.pop(client_id, None)

        logger.info(f"Flushing client {client_id} ({len(client_state)} fruits)")
        for item in client_state.values():
            agg_id = _aggregation_id(client_id, item.fruit)
            self.aggregation_outputs[agg_id].send(
                im.serialize(im.build_data(client_id, item.fruit, item.amount))
            )

        barrier_message = im.serialize(im.build_agg_barrier(client_id, ID))
        for aggregation_output in self.aggregation_outputs:
            aggregation_output.send(barrier_message)

    def _run_control(self):
        self.control_input.start_consuming(self._on_control_message)
        self.control_input.close()

    def start(self):
        self.control_thread.start()
        self.input_queue.start_consuming(self._on_input_message)

    def stop(self):
        self.input_queue.stop_consuming()
        self.control_input.request_stop()

    def close(self):
        self.control_thread.join()
        self.input_queue.close()
        for control_output in self.control_outputs:
            control_output.close()
        for aggregation_output in self.aggregation_outputs:
            aggregation_output.close()


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()

    def handle_sigterm(signum, frame):
        sum_filter.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)

    sum_filter.start()
    sum_filter.close()
    return 0


if __name__ == "__main__":
    main()
