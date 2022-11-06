import os
import sys
import threading
from concurrent import futures
from dataclasses import dataclass

import grpc

from config import load_config

sys.path.insert(
    0, os.path.join(os.path.dirname(os.path.realpath(__file__)), "proto_lib")
)


import proto_lib.fs_pb2 as fs_pb2
import proto_lib.fs_pb2_grpc as fs_pb2_grpc


@dataclass
class SuperBlock:
    FS_type: int
    FS_version: int
    bitmap_offset: int
    inode_bitmap_size: int
    data_bitmap_size: int
    inode_table_offset: int
    inode_number: int
    data_table_offset: int
    data_block_number: int


class FSServicer(fs_pb2_grpc.FileSystemServicer):
    def __init__(self):
        self.__super_block: SuperBlock | None = None
        self.__path_map: dict = {}
        self.__lock = threading.RLock()


if __name__ == "__main__":
    config = load_config()
    server = grpc.server(futures.ThreadPoolExecutor())
    fs_pb2_grpc.add_FileSystemServicer_to_server(FSServicer(), server)
    fs_port = config["fs_port"]
    server.add_insecure_port(f"[::]:{fs_port}")
    print("start FS server")
    server.start()
    server.wait_for_termination()
    print("end FS server")
