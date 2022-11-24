# distributed_storage_system

Implementation of a distributed storage system with RAID-6 mechanism

## Structure
```
project
└───README.md               this file           
└───src                     contains source code
└───test                    contains test programs
└───fuzzing                 contains fuzzing test programs
└───proto                   contains gRPC interace definition files
└───conf                    contains the configuration file
```

## Installation

This is a C++23 project. Currently only gcc>=12 and the latest version of MSVC can compile the project.

### Install open source lib requirements

1. Install spdlog according to [the instructions](https://github.com/gabime/spdlog).
2. Install doctest according to [the instructions](https://github.com/doctest/doctest).
3. Install protobuf according to [the instructions](https://github.com/protocolbuffers/protobuf).
4. Install grpc according to [the instructions](https://github.com/grpc/grpc).

#### Install my libs

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

### Compiling

```
cd distributed_storage_system
mkdir build && cd build
cmake -DBUILD_SHARED_LIBS=on  -DCMAKE_CXX_COMPILER=/usr/bin/g++-12 ..
make
```

###
