#!/bin/bash
set -e

if [ "$EUID" -ne 0 ]
  then echo "To install dependencies, you need to run as root. Please try running with sudo: sudo $0"
fi

BASEDIR=$(dirname "$0")
cd "$BASEDIR" || exit

./try_install_flatc.sh
./try_install_rocksdb.sh
