from __future__ import print_function

import os
import random
import sys

import grpc

sys.path.insert(0, os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))
sys.path.insert(
    0, os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "proto_lib")
)
import proto_lib.errno_pb2 as errno_pb2
import proto_lib.raid_pb2 as raid_pb2
import proto_lib.raid_pb2_grpc as raid_pb2_grpc
from config import load_config


def test_client():
    config = load_config()
    first_raid_node_port = config["first_raid_node_port"]
    with grpc.insecure_channel(f"localhost:{first_raid_node_port}") as channel:
        stub = raid_pb2_grpc.RAIDNodeStub(channel)
        stub.Read(raid_pb2.BlockReadRequest(block_no=0))
