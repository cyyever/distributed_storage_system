import os
import sys
import threading
# import threading
from concurrent import futures

import grpc

from config import load_config
from constant import block_size

# import numpy as np


sys.path.insert(
    0, os.path.join(os.path.dirname(os.path.realpath(__file__)), "proto_lib")
)


import proto_lib.errno_pb2 as errno_pb2
import proto_lib.raid_pb2 as raid_pb2
import proto_lib.raid_pb2_grpc as raid_pb2_grpc


class RAIDNode(raid_pb2_grpc.RAIDNodeServicer):
    def __init__(self, block_number: int):
        self.__block_number = block_number
        self.__blocks = [bytearray(block_size) for _ in range(block_number)]
        self.__lock = threading.Lock()

    def Read(self, request, context):
        context.set_code(grpc.StatusCode.OK)
        if request.block_no < self.__block_number:
            return raid_pb2.BlockReadReply(
                ok=raid_pb2.BlockReadOKReply(block=self.__blocks[request.block_no])
            )
        return raid_pb2.BlockReadReply(errno=errno_pb2.Errno.ERRNO_OUT_OF_RANGE)

    def Write(self, request, context):
        context.set_code(grpc.StatusCode.OK)
        if request.block_no < self.__block_number:
            self.__blocks[request.block_no] = request.block
            return raid_pb2.BlockWriteReply()
        return raid_pb2.BlockWriteReply(errno=errno_pb2.Errno.ERRNO_OUT_OF_RANGE)


if __name__ == "__main__":
    config = load_config()
    first_raid_node_port = config["first_raid_node_port"]
    raid_node_number = config["raid_node_number"]
    block_number = config["block_number"]
    servers = []
    for i in range(raid_node_number):
        server = grpc.server(futures.ThreadPoolExecutor())
        raid_pb2_grpc.add_RAIDNodeServicer_to_server(
            RAIDNode(block_number=block_number), server
        )
        port = first_raid_node_port + i
        server.add_insecure_port(f"[::]:{port}")
        servers.append(server)
    print("start RAID servers")
    for server in servers:
        server.start()
    print("finish start RAID servers")
    for server in servers:
        server.wait_for_termination()
    print("end RAID servers")
