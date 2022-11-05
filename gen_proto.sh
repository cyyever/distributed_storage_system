pip3 install grpcio-tools --user
rm -rf proto_lib
mkdir -p proto_lib
python3 -m grpc_tools.protoc -Iproto --python_out=proto_lib --grpc_python_out=proto_lib proto/fs.proto proto/raid.proto
python3 -m grpc_tools.protoc -Iproto --python_out=proto_lib proto/errno.proto
