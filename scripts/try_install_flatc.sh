#!/bin/bash

if [ ! -f /usr/local/bin/flatc ]; then
    apt-get update
    apt-get install -y unzip clang
    arch=$(uname -m)
    if [ "$arch" == "aarch64" ]; then
        echo "Host arch is aarch64"
        echo "aarch64 flatc binary is not available"
        exit 1
    elif [ "$arch" == "x86_64" ]; then
        echo "Host arch is amd64"
        wget -O flatc.zip https://github.com/google/flatbuffers/releases/download/v23.5.26/Linux.flatc.binary.g++-10.zip
    else
        echo "Unsupported arch"
        exit 1
    fi
    unzip flatc.zip
    mv flatc /usr/local/bin/
    rm flatc.zip
else
    echo "flatc exists"
fi
