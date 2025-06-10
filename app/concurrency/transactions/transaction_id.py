import threading


class TransactionId:
    """
    TransactionId is a class that contains the identifier of a transaction.

    Each transaction gets a unique ID used throughout the system
    for locking, logging, and recovery.

    The ID is generated using an atomic counter to ensure uniqueness
    across all threads.
    """

    _counter = 0
    _counter_lock = threading.Lock()

    def __init__(self):
        """Create a new unique transaction ID."""
        with TransactionId._counter_lock:
            TransactionId._counter += 1
            self.id = TransactionId._counter

    def get_id(self) -> int:
        """Return the unique ID of this transaction."""
        return self.id

    def __eq__(self, other) -> bool:
        """Check if two transaction IDs are equal."""
        if not isinstance(other, TransactionId):
            return False
        return self.id == other.id

    def __hash__(self) -> int:
        """Return hash code for use in dictionaries and sets."""
        return hash(self.id)

    def __str__(self) -> str:
        return f"TransactionId({self.id})"

    def __repr__(self) -> str:
        return self.__str__()