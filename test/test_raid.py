from __future__ import print_function

import os
import sys

import grpc

sys.path.insert(0, os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))
sys.path.insert(
    0, os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "proto_lib")
)
from config import load_config
from proto_lib import raid_pb2, raid_pb2_grpc


def test_client():
    # config = load_config()
    first_raid_node_port = 60001
    # config["RAID"]["first_node_port"]
    block_size = 512
    # config["RAID"]["block_size"]
    print(block_size)
    with grpc.insecure_channel(f"localhost:{first_raid_node_port}") as channel:
        stub = raid_pb2_grpc.RAIDNodeStub(channel)
        block = bytearray(block_size)
        block[-2] = 3
        reply = stub.Write(raid_pb2.BlockWriteRequest(block_no=0, block=bytes(block)))
        assert not reply.HasField("error")
        reply = stub.Read(raid_pb2.BlockReadRequest(block_no=0))
        assert not reply.HasField("error")
        new_block = reply.ok.block
        assert new_block[0] == 0
        assert new_block[-2] == 3
