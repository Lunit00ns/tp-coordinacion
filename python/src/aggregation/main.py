import bisect
import logging
import os
import signal

from common import barrier, fruit_item, message_protocol, middleware

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])

im = message_protocol.internal
logger = logging.getLogger(__name__)


class AggregationFilter:
    """Junta los totales de Sum por cliente y manda su top parcial a Join
    cuando avisaron las `SUM_AMOUNT` instancias."""

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_top_by_client = {}
        self.sum_barrier = barrier.ReplicaBarrier(SUM_AMOUNT)

    def _process_data(self, client_id, fruit, amount):
        fruit_top = self.fruit_top_by_client.setdefault(client_id, [])
        for i in range(len(fruit_top)):
            if fruit_top[i].fruit == fruit:
                updated_item = fruit_top[i] + fruit_item.FruitItem(fruit, amount)
                del fruit_top[i]
                bisect.insort(fruit_top, updated_item)
                return
        bisect.insort(fruit_top, fruit_item.FruitItem(fruit, amount))

    def _process_barrier(self, client_id, sum_id):
        _, is_complete = self.sum_barrier.mark(client_id, sum_id)
        if not is_complete:
            return

        logger.info(f"Received all sum barriers for client {client_id}")
        fruit_top = self.fruit_top_by_client.pop(client_id, [])
        fruit_chunk = list(fruit_top[-TOP_SIZE:])
        fruit_chunk.reverse()
        result = [(item.fruit, item.amount) for item in fruit_chunk]
        self.output_queue.send(im.serialize(im.build_partial(client_id, ID, result)))
        self.sum_barrier.clear(client_id)

    def process_message(self, message, ack, nack):
        try:
            fields = im.deserialize(message)
            if fields["type"] == im.MsgType.DATA:
                self._process_data(
                    fields["client_id"], fields["fruit"], fields["amount"]
                )
            else:
                self._process_barrier(fields["client_id"], fields["sum_id"])
            ack()
        except Exception:
            logger.exception("Error processing message")
            nack()

    def start(self):
        self.input_exchange.start_consuming(self.process_message)

    def stop(self):
        self.input_exchange.stop_consuming()

    def close(self):
        for resource in [self.input_exchange, self.output_queue]:
            try:
                resource.close()
            except Exception:
                logger.exception("Error closing resource")


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()

    def handle_sigterm(signum, frame):
        aggregation_filter.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)

    try:
        aggregation_filter.start()
    finally:
        aggregation_filter.close()
    return 0


if __name__ == "__main__":
    main()
