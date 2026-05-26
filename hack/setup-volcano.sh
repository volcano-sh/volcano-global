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

VOLCANO_VERSION=${VOLCANO_VERSION:-"release-1.14"}
VOLCANO_INSTALL_URL="https://raw.githubusercontent.com/volcano-sh/volcano/${VOLCANO_VERSION}/installer/volcano-development.yaml"
MEMBER_CLUSTERS=${MEMBER_CLUSTERS:-"member1 member2 member3"}
MEMBERS_KUBECONFIG=${MEMBERS_KUBECONFIG:-"${HOME}/.kube/members.config"}

echo "=== Installing Volcano on member clusters ==="
echo "Volcano version: ${VOLCANO_VERSION}"
echo "Member clusters: ${MEMBER_CLUSTERS}"

export KUBECONFIG="${MEMBERS_KUBECONFIG}"

for cluster in ${MEMBER_CLUSTERS}; do
    echo "Installing Volcano on ${cluster}..."
    kubectl --context "${cluster}" apply -f "${VOLCANO_INSTALL_URL}"
done

echo "Waiting for Volcano pods to be ready on each member cluster..."
for cluster in ${MEMBER_CLUSTERS}; do
    echo "Checking Volcano readiness on ${cluster}..."

    kubectl --context "${cluster}" -n volcano-system wait --for=condition=Available deployment --all --timeout=300s || {
        echo "ERROR: Volcano deployments on ${cluster} did not become ready in time"
        kubectl --context "${cluster}" -n volcano-system get pods
        exit 1
    }
    echo "Volcano is ready on ${cluster}."
done

echo "=== Volcano installed on all member clusters ==="
