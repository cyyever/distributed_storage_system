import os
import sys
import threading
import typing
from concurrent import futures
from dataclasses import dataclass

import grpc
import hydra

from config import global_config, load_config

sys.path.insert(
    0, os.path.join(os.path.dirname(os.path.realpath(__file__)), "proto_lib")
)

from cyy_naive_lib.log import get_logger

import proto_lib.fs_pb2 as fs_pb2
import proto_lib.fs_pb2_grpc as fs_pb2_grpc


@dataclass
class SuperBlock:
    FS_type: int
    FS_version: int
    inode_table_offset: int
    inode_number: int
    inode_bitmap_offset: int
    inode_bitmap_size: int
    data_table_offset: int
    data_block_number: int
    data_bitmap_offset: int
    data_bitmap_size: int


@dataclass
class Inode:
    size: int
    block_number: int
    ctime: int
    mtime: int
    block_number: int
    block_ptr: int


class FSServicer(fs_pb2_grpc.FileSystemServicer):
    def __init__(self):
        self.__super_block: dict = dict()
        self.__path_map: dict = dict()
        self.__lock = threading.RLock()


if __name__ == "__main__":
    load_config()
    server = grpc.server(futures.ThreadPoolExecutor())
    fs_pb2_grpc.add_FileSystemServicer_to_server(FSServicer(), server)
    server.add_insecure_port("[::]:56991")
    get_logger().info("start FS server")
    server.start()
    server.wait_for_termination()
    get_logger().info("end FS server")
