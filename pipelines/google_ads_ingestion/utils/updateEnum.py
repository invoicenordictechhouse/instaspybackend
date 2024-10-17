from enum import Enum

class UpdateMode(str, Enum):
    ALL = "all"
    ACTIVE = "active"
    SPECIFIC = "specific"