#!/bin/bash

cp ../target/release/data-node data-node/usr/local/bin/
cp ../etc/*.yaml data-node/etc/data-node/
rm data-node/usr/local/bin/.gitignore
rm data-node/etc/data-node/.gitignore
dpkg-deb --build data-node
