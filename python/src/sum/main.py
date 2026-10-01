import logging
import os
import signal
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

# Progreso: se reporta cada K mensajes y el resto, tras un rato sin datos.
PROGRESS_REPORT_EVERY = int(os.environ.get("PROGRESS_REPORT_EVERY", "100"))
PROGRESS_FLUSH_DELAY = float(os.environ.get("PROGRESS_FLUSH_DELAY", "0.2"))

im = message_protocol.internal
logger = logging.getLogger(__name__)


def _aggregation_id(client_id, fruit):
    """Aggregation asignado a una fruta (determinista entre procesos)."""
    key = f"{client_id}|{fruit}".encode()
    return zlib.crc32(key) % AGGREGATION_AMOUNT


class SumFilter:
    """Suma (fruta, cantidad) por cliente y manda los totales a Aggregation
    cuando las réplicas de Sum confirman que procesaron todo."""

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.control_input = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            SUM_CONTROL_EXCHANGE,
            [f"{SUM_PREFIX}_ctrl_{ID}"],
            channel=self.input_queue.channel,
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

        self.amount_by_client = {}
        self.local_count = {}
        self.total_count = {}
        self.progress_reports = {}
        self.flushed_clients = set()
        # Clientes con mensajes procesados que todavía no se reportaron
        self.unreported_clients = set()
        self.report_timer_pending = False

    # -- Datos --------------------------------------------------------

    def _process_data(self, client_id, fruit, amount):
        client_state = self.amount_by_client.setdefault(client_id, {})
        client_state[fruit] = client_state.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

        self.local_count[client_id] = self.local_count.get(client_id, 0) + 1
        if client_id in self.total_count:
            self._report_progress_if_due(client_id)

    def _report_progress_if_due(self, client_id):
        """Reporta cada K mensajes; el resto lo reporta el temporizador."""
        if self.local_count[client_id] % PROGRESS_REPORT_EVERY == 0:
            self._report_progress(client_id)
            return
        self.unreported_clients.add(client_id)
        if not self.report_timer_pending:
            self.report_timer_pending = True
            self.input_queue.call_later(
                PROGRESS_FLUSH_DELAY, self._flush_unreported_progress
            )

    def _report_progress(self, client_id):
        self.unreported_clients.discard(client_id)
        self._broadcast(self._own_progress_message(client_id))

    def _flush_unreported_progress(self):
        self.report_timer_pending = False
        try:
            for client_id in list(self.unreported_clients):
                self._report_progress(client_id)
        except Exception:
            logger.exception("Error reporting pending progress")

    def _own_progress_message(self, client_id):
        count = self.local_count.get(client_id, 0)
        return im.serialize(im.build_sum_progress(client_id, ID, count))

    # -- Barrera de cierre entre réplicas de Sum -----------------------

    def _broadcast(self, message):
        for control_output in self.control_outputs:
            control_output.send(message)

    def _on_input_message(self, message, ack, nack):
        try:
            fields = im.deserialize(message)
            client_id = fields["client_id"]

            if fields["type"] == im.MsgType.DATA:
                self._process_data(client_id, fields["fruit"], fields["amount"])
            else:
                logger.info(f"Broadcasting SUM_BARRIER for client {client_id}")
                self._broadcast(
                    im.serialize(im.build_sum_barrier(client_id, fields["total_count"]))
                )
            ack()
        except Exception:
            logger.exception("Error processing input message")
            nack()

    def _on_control_message(self, message, ack, nack):
        try:
            fields = im.deserialize(message)
            client_id = fields["client_id"]

            if fields["type"] == im.MsgType.SUM_BARRIER:
                if client_id not in self.total_count:
                    self.total_count[client_id] = fields["total_count"]
                self._report_progress(client_id)
            else:
                reports = self.progress_reports.setdefault(client_id, {})
                reports[fields["sum_id"]] = fields["count"]

            if self._is_client_complete(client_id):
                self._flush_client(client_id)
            ack()
        except Exception:
            logger.exception("Error processing control message")
            nack()

    def _is_client_complete(self, client_id):
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
        if client_id in self.flushed_clients:
            return
        self.flushed_clients.add(client_id)
        self.unreported_clients.discard(client_id)
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

    def start(self):
        self.input_queue.register_consumer(self._on_input_message)
        self.control_input.register_consumer(self._on_control_message)
        self.input_queue.pump_forever()

    def stop(self):
        self.input_queue.stop_consuming()

    def close(self):
        resources = [self.input_queue, self.control_input]
        resources.extend(self.control_outputs)
        resources.extend(self.aggregation_outputs)
        for resource in resources:
            try:
                resource.close()
            except Exception:
                logger.exception("Error closing resource")


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()

    def handle_sigterm(signum, frame):
        sum_filter.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)

    try:
        sum_filter.start()
    finally:
        sum_filter.close()
    return 0


if __name__ == "__main__":
    main()
