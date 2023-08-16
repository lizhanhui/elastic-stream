---
sidebar_position: 1
---

# Quick Start

## Install from pre-built package

Download deb file from [latest release](https://github.com/AutoMQ/elastic-stream/releases/latest).

```shell
# Please change the file name to match your OS architecture. 
dpkg -i pd_0.2.8_amd64.deb
dpkg -i range-server_0.2.8_amd64.deb
```

## Build from source

If there is no pre-built package for your OS or you just want to try, follow the steps below to build from source.

### Install dependencies

```shell
git clone https://github.com/AutoMQ/elastic-stream.git
cd elastic-stream
sudo ./scripts/install_deps.sh
```

### Build Placement Driver

Notice: Placement Driver use docker to build

```shell
# Install build tools
sudo apt-get update
sudo apt-get install -y make docker.io

cd pd
make
./bin/${OS}_${ARCH}/pd --version
```

### Build Range Server

```shell
# Install nightly rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup default nightly

cargo build --bin range-server --release
target/release/range-server --version
```

### Build Rust and Java SDK

```shell
# Install Java
sudo apt-get update
sudo apt-get install -y openjdk-17-jdk maven
./sdks/build.sh
```

## Run

### Launch Placement Driver

```shell
pd
```

### Launch Range Server

```shell
range-server start --config etc/range-server.yaml --log etc/range-server-log.yaml
```

### Launch Example workload

```shell
# Run Rust example
cargo run --example main

# Run Java example
java -jar sdks/frontend-java/examples/target/examples-1.0-SNAPSHOT-jar-with-dependencies.jar
```
