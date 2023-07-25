echo $KUBE_CONFIG_DATA | base64 -d > /tmp/config

helm upgrade --force pd pd \
    -n elastic-stream-long-running \
    --reuse-values \
    --kubeconfig /tmp/config \
    --wait --timeout 10m

helm upgrade --force range-server range-server \
    -n elastic-stream-long-running \
    --reuse-values \
    --kubeconfig /tmp/config \
    --wait --timeout 10m
