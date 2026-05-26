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

KARMADA_VERSION=${KARMADA_VERSION:-"v1.12.0"}
KARMADA_DIR=${KARMADA_DIR:-"/tmp/karmada"}
KARMADA_TARBALL_URL="https://github.com/karmada-io/karmada/archive/refs/tags/${KARMADA_VERSION}.tar.gz"
MEMBER_CLUSTERS=${MEMBER_CLUSTERS:-"member1 member2 member3"}

echo "=== Setting up Karmada multi-cluster environment ==="
echo "Karmada version: ${KARMADA_VERSION}"

if ! docker info >/dev/null 2>&1; then
    echo "ERROR: Docker engine is not reachable. Please start Docker Desktop and try again."
    exit 1
fi

# Step 1: Download and extract the Karmada release tarball
if [ -d "${KARMADA_DIR}" ]; then
    echo "Karmada directory already exists at ${KARMADA_DIR}, removing..."
    rm -rf "${KARMADA_DIR}"
fi

echo "Downloading Karmada ${KARMADA_VERSION}"
mkdir -p "${KARMADA_DIR}"
curl -sL "${KARMADA_TARBALL_URL}" | tar xz --strip-components=1 -C "${KARMADA_DIR}" || {
    echo "ERROR: Failed to download Karmada tarball from '${KARMADA_TARBALL_URL}'"
    echo "ERROR: Please verify KARMADA_VERSION points to an existing release tag."
    exit 1
}

# Step 2: cd into it
cd "${KARMADA_DIR}"

# Step 3: Deploy the Karmada environment
# Disable Go VCS stamping: the tarball has no .git directory, so `go build`
# would otherwise fail with "error obtaining VCS status: exit status 128".
export GOFLAGS="${GOFLAGS:-} -buildvcs=false"
echo "Running Karmada local-up script..."
./hack/local-up-karmada.sh

# Verify the environment came up
echo "Verifying Karmada API server..."
export KUBECONFIG="${HOME}/.kube/karmada.config"
kubectl --context karmada-apiserver get ns default >/dev/null
echo "Karmada API server is healthy."

echo "Verifying member clusters..."
export KUBECONFIG="${HOME}/.kube/members.config"
for cluster in ${MEMBER_CLUSTERS}; do
    kubectl --context "${cluster}" get ns default >/dev/null
    echo "  ${cluster} is ready."
done

echo "=== Karmada multi-cluster environment is ready ==="
echo "  Karmada config: ${HOME}/.kube/karmada.config"
echo "  Members config: ${HOME}/.kube/members.config"
echo "  Contexts: karmada-host, karmada-apiserver, member1, member2, member3"
