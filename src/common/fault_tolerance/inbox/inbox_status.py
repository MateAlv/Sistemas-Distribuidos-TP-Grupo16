from enum import Enum, auto


class InboxStatus(Enum):
    NEW = auto()      # never seen: process it
    APPLIED = auto()  # state changed but outputs not yet published+committed
    DONE = auto()     # already finished: drop as duplicate
