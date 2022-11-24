# distributed_storage_system

Implementation of a distributed storage system with RAID-6 mechanism

## Structure

```
project
└───README.md                 this file
└───src                       contains source code
│   │   block.hpp             contains block definitions
│   │   block_cache.hpp       implementation of block cache
│   │   config.hpp            configuration parsing
│   │   disk.hpp              contains RAID disk code
│   │   fs_block.hpp          contains file system block definitions
│   │   fs_server.cpp         gRPC file system server
│   │   galois_field.cpp      GF(2^8) for RAID 6
│   │   raid_controller.hpp   implementation of RAID 6
│   │   raid_file_system.hpp  implementation of file system
│   │   raid_node_server.hpp  gRPC RAID node server
│
└───test                    contains test programs
└───fuzzing                 contains fuzzing test programs
└───proto                   contains gRPC interace definition files
└───conf                    contains the configuration file
```

## Installation

This is a C++23 project. Currently only gcc>=12 and the latest version of MSVC can compile the project.

### Install open source dependencies

1. Install spdlog according to [the instructions](https://github.com/gabime/spdlog).
2. Install doctest according to [the instructions](https://github.com/doctest/doctest).
3. Install protobuf according to [the instructions](https://github.com/protocolbuffers/protobuf).
4. Install grpc according to [the instructions](https://github.com/grpc/grpc).

### Install my libraries

5.

```
git clone --recursive git@github.com:cyyever/naive_cpp_lib.git
cd naive_cpp_lib
mkdir build && cd build
cmake -DBUILD_SHARED_LIBS=on ..
sudo make install
```

6.

```
git clone --recursive git@github.com:cyyever/algorithm.git
cd algorithm
mkdir build && cd build
cmake -DBUILD_SHARED_LIBS=on ..
sudo make install
```

### Building

```
cd distributed_storage_system
mkdir build && cd build
cmake -DBUILD_SHARED_LIBS=on  -DCMAKE_CXX_COMPILER=/usr/bin/g++-12 ..
make
```

## Test

Modify the configuration file if needed.

Use the following command to start the RAID node services

```
cd distributed_storage_system/build
./raid_node_server ../conf/raid_fs.yaml
```

Next use the following command to start the file system service

```
cd distributed_storage_system/build
./fs_server ../conf/raid_fs.yaml

```

Perform test

```
cd distributed_storage_system/build
./test/file_system_test
```
