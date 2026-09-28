import json


class MsgType:
    DATA = "DATA"
    EOF = "EOF"
    SUM_BARRIER = "SUM_BARRIER"
    AGG_BARRIER = "AGG_BARRIER"
    PARTIAL = "PARTIAL"
    RESULT = "RESULT"


def build_data(client_id, fruit, amount):
    """Par (fruta, cantidad) de un cliente."""
    return {
        "type": MsgType.DATA,
        "client_id": client_id,
        "fruit": fruit,
        "amount": amount,
    }


def build_eof(client_id):
    """Fin de flujo de datos de un cliente."""
    return {"type": MsgType.EOF, "client_id": client_id}


def build_sum_barrier(client_id):
    """Aviso de una instancia de Aggregation a todas las de Sum de que ya flusheó
    (o no tenía nada que flushear) los datos de este cliente."""
    return {"type": MsgType.SUM_BARRIER, "client_id": client_id}


def build_agg_barrier(client_id, sum_id):
    """Aviso de una instancia de Sum a todas las de Aggregation de que ya flusheó
    (o no tenía nada que flushear) los datos de este cliente."""
    return {"type": MsgType.AGG_BARRIER, "client_id": client_id, "sum_id": sum_id}


def build_partial(client_id, agg_id, fruit_top):
    """Top parcial de una instancia de Aggregation."""
    return {
        "type": MsgType.PARTIAL,
        "client_id": client_id,
        "agg_id": agg_id,
        "fruit_top": fruit_top,
    }


def build_result(client_id, fruit_top):
    """Resultado final de un cliente."""
    return {"type": MsgType.RESULT, "client_id": client_id, "fruit_top": fruit_top}


def serialize(message):
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    return json.loads(message.decode("utf-8"))
