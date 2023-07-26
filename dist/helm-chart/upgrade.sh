#!/bin/bash
echo $KUBE_CONFIG_DATA | base64 -d > ./config

helm upgrade --force pd pd \
    -n elastic-stream-long-running \
    --reuse-values \
    --kubeconfig ./config \
    --wait --timeout 10m

helm upgrade --force range-server range-server \
    -n elastic-stream-long-running \
    --reuse-values \
    --kubeconfig ./config \
    --wait --timeout 10m
