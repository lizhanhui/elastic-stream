#!/bin/bash
echo $KUBE_CONFIG_DATA | base64 -d > ./config

kubectl --kubeconfig ./config apply -f range-server.yaml -n elastic-stream-long-running
kubectl --kubeconfig ./config apply -f pd.yaml -n elastic-stream-long-running
