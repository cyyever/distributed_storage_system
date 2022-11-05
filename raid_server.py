import os
import sys
# import threading
from concurrent import futures

import grpc
import hydra

from config import global_config, load_config

sys.path.insert(
    0, os.path.join(os.path.dirname(os.path.realpath(__file__)), "proto_lib")
)


import proto_lib.raid_pb2 as raid_pb2
import proto_lib.raid_pb2_grpc as raid_pb2_grpc


class RAIDNode(raid_pb2_grpc.RAIDNodeServicer):
    pass


if __name__ == "__main__":
    load_config()
    first_raid_node_port = global_config["first_raid_node_port"]
    raid_node_number = global_config["raid_node_number"]
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
