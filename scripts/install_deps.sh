#!/bin/bash

BASEDIR=$(dirname "$0")
cd "$BASEDIR" || exit

./try_install_flatc.sh
./try_install_rocksdb.sh
