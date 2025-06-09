import threading
from typing import Optional


class TransactionId:
    """
    Unique identifier for a database transaction.

    Each transaction gets a monotonically increasing ID.
    Thread-safe counter ensures uniqueness across concurrent transactions.
    """

    _counter_lock = threading.Lock()
    _next_id = 1

    def __init__(self, id_value: Optional[int] = None):
        if id_value is not None:
            self.id = id_value
        else:
            with TransactionId._counter_lock:
                self.id = TransactionId._next_id
                TransactionId._next_id += 1

    def get_id(self) -> int:
        return self.id

    def __eq__(self, other: object) -> bool:
        return isinstance(other, TransactionId) and self.id == other.id

    def __hash__(self) -> int:
        return hash(self.id)

    def __str__(self) -> str:
        return f"TransactionId({self.id})"

    def __repr__(self) -> str:
        return self.__str__()
