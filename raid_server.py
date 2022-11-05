import os
import sys
# import threading
from concurrent import futures

import grpc
import hydra
import numpy as np

from config import global_config, load_config
from constant import block_size

sys.path.insert(
    0, os.path.join(os.path.dirname(os.path.realpath(__file__)), "proto_lib")
)


import proto_lib.errono_pb2 as errno_pb2
import proto_lib.raid_pb2 as raid_pb2
import proto_lib.raid_pb2_grpc as raid_pb2_grpc


class RAIDNode(raid_pb2_grpc.RAIDNodeServicer):
    def __init__(self, block_number: int):
        self.__block_number = block_number
        self.__blocks = np.empty(shape=[block_number, block_size], dtype=np.byte)

    def Read(self, request, context):
        if request.block_no < self.__block_number:
            return raid_pb2.BlockReadReply(
                ok=raid_pb2.BlockReadOKReply(block=self.__blocks[request.block_no])
            )
        return raid_pb2.BlockReadReply(errno=errono_pb2.Errno.ERRNO_OUT_OF_RANGE)

    # def Write(self, request, context):
    #     """Missing associated documentation comment in .proto file."""
    #     context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    #     context.set_details("Method not implemented!")
    #     raise NotImplementedError("Method not implemented!")


if __name__ == "__main__":
    load_config()
    first_raid_node_port = global_config["first_raid_node_port"]
    raid_node_number = global_config["raid_node_number"]
    block_number = global_config["block_number"]
    servers = []
    for i in range(raid_node_number):
        server = grpc.server(futures.ThreadPoolExecutor())
        raid_pb2_grpc.add_RAIDNodeServicer_to_server(RAIDNode(), server)
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
