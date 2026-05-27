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
KARMADA_KUBECONFIG=${KARMADA_KUBECONFIG:-"${HOME}/.kube/karmada.config"}
MEMBERS_KUBECONFIG=${MEMBERS_KUBECONFIG:-"${HOME}/.kube/members.config"}
KARMADA_HOST_CLUSTER=${KARMADA_HOST_CLUSTER:-"karmada-host"}
MEMBER_CLUSTERS=${MEMBER_CLUSTERS:-"member1 member2 member3"}
ARTIFACTS_PATH=${ARTIFACTS_PATH:-"${REPO_ROOT}/volcano-global-e2e-logs"}
GINKGO_FLAGS=${GINKGO_FLAGS:-""}

if [ ! -f "${KARMADA_KUBECONFIG}" ]; then
    echo "ERROR: Karmada kubeconfig not found: ${KARMADA_KUBECONFIG}"
    exit 1
fi
if [ ! -f "${MEMBERS_KUBECONFIG}" ]; then
    echo "ERROR: Members kubeconfig not found: ${MEMBERS_KUBECONFIG}"
    exit 1
fi

mkdir -p "${ARTIFACTS_PATH}"

echo "=== Running Volcano-Global E2E Tests ==="
echo "Karmada kubeconfig: ${KARMADA_KUBECONFIG}"
echo "Members kubeconfig: ${MEMBERS_KUBECONFIG}"
echo "Artifacts path: ${ARTIFACTS_PATH}"

# Install ginkgo
echo "Installing ginkgo..."
GO111MODULE=on go install github.com/onsi/ginkgo/v2/ginkgo@v2.28.3

GO_BIN="$(go env GOBIN)"
if [ -z "${GO_BIN}" ]; then
    GO_BIN="$(go env GOPATH)/bin"
fi
export PATH="${GO_BIN}:${PATH}"

if ! command -v ginkgo >/dev/null 2>&1; then
    echo "ERROR: ginkgo was installed but is not on PATH (expected in ${GO_BIN})"
    exit 1
fi

export KUBECONFIG="${KARMADA_KUBECONFIG}"
kubectl --context karmada-apiserver get ns default >/dev/null

set +e
GOOS=$(go env GOHOSTOS) GOARCH=$(go env GOHOSTARCH) CGO_ENABLED=1 \
ginkgo -v --race --trace --fail-fast -p --randomize-all \
    ${GINKGO_FLAGS} \
    "${REPO_ROOT}/test/e2e/..." \
    -- \
    --karmada-context=karmada-apiserver \
    --karmada-kubeconfig="${KARMADA_KUBECONFIG}" \
    --member-kubeconfig="${MEMBERS_KUBECONFIG}"
TESTING_RESULT=$?
set -e

# Collect logs
echo "Collecting logs to ${ARTIFACTS_PATH}..."

echo "Collecting Kind cluster logs..."
for cluster in "${KARMADA_HOST_CLUSTER}" ${MEMBER_CLUSTERS}; do
    CLUSTER_LOG_DIR="${ARTIFACTS_PATH}/${cluster}"
    mkdir -p "${CLUSTER_LOG_DIR}"
    kind export logs --name="${cluster}" "${CLUSTER_LOG_DIR}" 2>/dev/null || true
done

# Collect volcano-global pod logs
echo "Collecting volcano-global pod logs..."
kubectl --context "${KARMADA_HOST_CLUSTER}" -n volcano-global logs -l app=volcano-global-controller-manager \
    --tail=-1 > "${ARTIFACTS_PATH}/controller-manager.log" 2>/dev/null || true
kubectl --context "${KARMADA_HOST_CLUSTER}" -n volcano-global logs -l app=volcano-global-webhook-manager \
    --tail=-1 > "${ARTIFACTS_PATH}/webhook-manager.log" 2>/dev/null || true

echo "Collected logs at ${ARTIFACTS_PATH}:"
ls -al "${ARTIFACTS_PATH}"

exit ${TESTING_RESULT}
