#!/bin/bash

if [ ! -f rocksdb.deb ]; then
    arch=$(uname -m)
    if [ "$arch" == "aarch64" ]; then
        echo "Host arch is aarch64"
        wget -O rocksdb.deb https://github.com/AutoMQ/rocksdb/releases/download/rocksdb-v8.1.1b0/rocksdb-aarch64.deb
    elif [ "$arch" == "x86_64" ]; then
        echo "Host arch is amd64"
        wget -O rocksdb.deb https://github.com/AutoMQ/rocksdb/releases/download/rocksdb-v8.1.1b0/rocksdb-amd64.deb
    else
        echo "Unsupported arch"
        exit 1
    fi
fi
./try_install_dpkg.sh rocksdb.deb
rm -f rocksdb.deb
