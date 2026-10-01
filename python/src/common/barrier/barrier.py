class ReplicaBarrier:
    """Sincroniza la llegada de reportes de réplicas distintas para una
    clave hasta alcanzar N."""

    def __init__(self, expected_count):
        self._expected_count = expected_count
        self._seen_by_key = {}

    def mark(self, key, replica_id):
        """Registra `replica_id` para `key` y retorna `(is_new, is_complete)`."""
        seen = self._seen_by_key.setdefault(key, set())
        is_new = replica_id not in seen
        seen.add(replica_id)
        return is_new, len(seen) >= self._expected_count

    def clear(self, key):
        """Libera el estado de `key` una vez resuelta la barrera."""
        self._seen_by_key.pop(key, None)
