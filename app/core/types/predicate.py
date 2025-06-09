from enum import Enum


class Predicate(Enum):
    """Predicate operations for field comparisons."""
    EQUALS = "="
    NOT_EQUALS = "!="
    GREATER_THAN = ">"
    LESS_THAN = "<"
    GREATER_THAN_OR_EQ = ">="
    LESS_THAN_OR_EQ = "<="
    LIKE = "LIKE"
