import uuid

from common import message_protocol

im = message_protocol.internal


class MessageHandler:
    def __init__(self):
        # Un client_id por conexión para aislar flujos y rutar el resultado
        # correcto.
        self.client_id = str(uuid.uuid4())
        # Contador de mensajes DATA enviados; se incluye en el EOF para
        # validación en Sum.
        self.data_count = 0

    def serialize_data_message(self, message):
        [fruit, amount] = message
        self.data_count += 1
        msg = im.build_data(self.client_id, fruit, amount)
        return im.serialize(msg)

    def serialize_eof_message(self, message):
        return im.serialize(im.build_eof(self.client_id, self.data_count))

    def deserialize_result_message(self, message):
        fields = im.deserialize(message)
        if fields["client_id"] != self.client_id:
            return None
        return fields["fruit_top"]
