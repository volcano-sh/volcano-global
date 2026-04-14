#!/bin/bash

# Copyright 2025 The Volcano Authors.
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

REPO_ROOT=$(dirname "${BASH_SOURCE[0]}")/..
cd "${REPO_ROOT}"

SKIP_SETUP=${SKIP_SETUP:-"false"}
ARTIFACTS_PATH=${ARTIFACTS_PATH:-"/tmp/e2e-logs"}
export ARTIFACTS_PATH

if [ "${SKIP_SETUP}" != "true" ]; then
	echo "SKIP_SETUP=false, running environment setup"
	./hack/setup-e2e-env.sh
else
	echo "SKIP_SETUP=true, skipping environment setup"
fi

./hack/run-e2e.sh
