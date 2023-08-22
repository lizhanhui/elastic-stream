find . -not \( \
    \( \
      -wholename './output' \
      -o -wholename './.git' \
      -o -wholename './.go' \
      -o -wholename './bin' \
      -o -wholename './.licenses' \
      -o -wholename './api/kvpb' \
      -o -wholename './api/rpcfb' \
      -o -wholename './third_party' \
    \) -prune \
  \) -name '*.go'
