import uuid

from common import message_protocol

im = message_protocol.internal


class MessageHandler:
    def __init__(self):
        # Un client_id por conexión: permite aislar los flujos de cada
        # cliente (Sum/Aggregation/Join) y, a la vuelta, distinguir cuál
        # resultado le corresponde a este socket.
        self.client_id = str(uuid.uuid4())

    def serialize_data_message(self, message):
        [fruit, amount] = message
        msg = im.build_data(self.client_id, fruit, amount)
        return im.serialize(msg)

    def serialize_eof_message(self, message):
        return im.serialize(im.build_eof(self.client_id))

    def deserialize_result_message(self, message):
        fields = im.deserialize(message)
        if fields["client_id"] != self.client_id:
            return None
        return fields["fruit_top"]
