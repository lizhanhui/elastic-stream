#!/bin/bash
cp ../*_amd64.deb .
cp ../../pd/distribution/*_amd64.deb .
docker build -t elasticstream/elastic-stream:nightly -f Dockerfile .
docker push elasticstream/elastic-stream:nightly
