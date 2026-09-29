import logging
import os
import signal

from common import barrier, fruit_item, message_protocol, middleware

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])

im = message_protocol.internal
logger = logging.getLogger(__name__)


class JoinFilter:
    """Consolida los tops parciales de Aggregation en un top final por cliente.

    Junta las frutas reportadas por cada nodo de Aggregation sin sumar (ya que 
    están particionadas previamente) y devuelve el resultado al recibir las 
    barreras de las `AGGREGATION_AMOUNT` instancias.
    """

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_top_by_client = {}
        self.agg_barrier = barrier.ReplicaBarrier(AGGREGATION_AMOUNT)

    def process_message(self, message, ack, nack):
        try:
            fields = im.deserialize(message)
            client_id = fields["client_id"]
            agg_id = fields["agg_id"]
            fruit_top = fields["fruit_top"]

            is_new, is_complete = self.agg_barrier.mark(client_id, agg_id)
            if is_new:
                state = self.fruit_top_by_client.setdefault(client_id, [])
                state.extend(
                    fruit_item.FruitItem(fruit, amount) for fruit, amount in fruit_top
                )

            if not is_complete:
                ack()
                return

            logger.info(f"Received all aggregation partials for client {client_id}")
            state = self.fruit_top_by_client.pop(client_id, [])
            state.sort()
            top_chunk = list(state[-TOP_SIZE:])
            top_chunk.reverse()
            result = [(item.fruit, item.amount) for item in top_chunk]
            self.output_queue.send(im.serialize(im.build_result(client_id, result)))
            self.agg_barrier.clear(client_id)
            ack()
        except Exception:
            logger.exception("Error processing message")
            nack()

    def start(self):
        self.input_queue.start_consuming(self.process_message)

    def stop(self):
        self.input_queue.stop_consuming()

    def close(self):
        for resource in [self.input_queue, self.output_queue]:
            try:
                resource.close()
            except Exception:
                logger.exception("Error closing resource")


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()

    def handle_sigterm(signum, frame):
        join_filter.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)

    try:
        join_filter.start()
    finally:
        join_filter.close()
    return 0


if __name__ == "__main__":
    main()
