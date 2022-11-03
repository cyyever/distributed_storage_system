from dataclasses import dataclass
from enum import IntEnum, auto


# we use POSIX errno for error notification, see $(man 3 errno) for details
class Errno(IntEnum):
    SUCC = auto()


@dataclass
class OpenRequest:
    path: str
    create_file: bool


@dataclass
class OpenReply:
    errno: Errno
    fd: int


# RAID interface types
@dataclass
class BlockReadRequest:
    block_no: int


@dataclass
class BlockWriteRequest:
    block_no: int
    block: bytes


@dataclass
class BlockReadReply:
    errno: Errno
    block_no: int
    block: bytes


@dataclass
class BlockWriteReply:
    errno: Errno
    block_no: int
