#!/bin/bash

try_install_flatc() {
    if [ ! -f /usr/local/bin/flatc ]; then
        sudo apt-get update
        sudo apt-get install -y unzip clang
        wget -O flatc.zip https://github.com/google/flatbuffers/releases/download/v23.1.4/Linux.flatc.binary.g++-10.zip
        unzip flatc.zip
        sudo mv flatc /usr/local/bin/
        rm flatc.zip
    else
        echo "flatc exists"
    fi
}

try_install_flatc
