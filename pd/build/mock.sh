#!/bin/sh

# Copyright 2016 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit
set -o nounset
set -o pipefail

export CGO_ENABLED=0
export GO111MODULE=on

echo "Generating mocks:"
for IFILE in $("$(dirname "$0")"/tool/find_files.sh | xargs grep -l -e '//go:generate mockgen'); do
  go generate -v "$IFILE"
  mock_file=$(echo "$IFILE" | sed 's/\.go/_mock.go/')
  if [ ! -f "$mock_file" ]; then
    echo "mock file $mock_file not found, please check the go:generate command in $IFILE"
    exit 1
  fi
  sed -i '1i//go:build testing' "$mock_file"
done
echo
