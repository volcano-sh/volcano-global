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

TAG=${TAG:-$(git rev-parse --verify HEAD 2>/dev/null || echo "latest")}
IMAGE_PREFIX=${IMAGE_PREFIX:-"volcanosh"}
KARMADA_KUBECONFIG=${KARMADA_KUBECONFIG:-"${HOME}/.kube/karmada.config"}
KARMADA_HOST_CLUSTER=${KARMADA_HOST_CLUSTER:-"karmada-host"}
VOLCANO_VERSION=${VOLCANO_VERSION:-"release-1.14"}
SKIP_BUILD=${SKIP_BUILD:-"false"}

CONTROLLER_MANAGER_IMAGE="${IMAGE_PREFIX}/volcano-global-controller-manager:${TAG}"
WEBHOOK_MANAGER_IMAGE="${IMAGE_PREFIX}/volcano-global-webhook-manager:${TAG}"

ensure_context() {
    local context="$1"
    if ! kubectl config get-contexts "${context}" >/dev/null 2>&1; then
        echo "ERROR: Missing kubeconfig context '${context}'"
        exit 1
    fi
}

ensure_exists() {
    local context="$1"
    shift
    if ! kubectl --context "${context}" "$@" >/dev/null 2>&1; then
        echo "ERROR: Validation failed: kubectl --context ${context} $*"
        exit 1
    fi
}

kind_load_image() {
    local image="$1"
    local cluster_name="$2"

    if ! kind get clusters 2>/dev/null | grep -qx "${cluster_name}"; then
        echo "ERROR: Kind cluster '${cluster_name}' not found. Did you run hack/setup-karmada.sh?"
        exit 1
    fi

    local attempt=1
    local max_attempts=3
    while [ "${attempt}" -le "${max_attempts}" ]; do
        if kind load docker-image "${image}" --name "${cluster_name}"; then
            return 0
        fi
        echo "WARN: Failed to load image '${image}' into Kind '${cluster_name}' (attempt ${attempt}/${max_attempts})"
        attempt=$((attempt + 1))
        sleep 2
    done

    echo "ERROR: Unable to load image '${image}' into Kind cluster '${cluster_name}'."
    echo "ERROR: Check that the image exists locally and the Kind cluster is running."
    exit 1
}

echo "=== Deploying Volcano-Global ==="
echo "Image tag: ${TAG}"
echo "Controller image: ${CONTROLLER_MANAGER_IMAGE}"
echo "Webhook image: ${WEBHOOK_MANAGER_IMAGE}"

# Step 1: Build images
if [ "${SKIP_BUILD}" != "true" ]; then
    echo "Building volcano-global images..."
    make images TAG="${TAG}"
else
    echo "Skipping image build (SKIP_BUILD=true)"
fi

# Step 2: Load images into the karmada-host Kind cluster
echo "Loading images into Kind cluster ${KARMADA_HOST_CLUSTER}..."
kind_load_image "${CONTROLLER_MANAGER_IMAGE}" "${KARMADA_HOST_CLUSTER}"
kind_load_image "${WEBHOOK_MANAGER_IMAGE}" "${KARMADA_HOST_CLUSTER}"

export KUBECONFIG="${KARMADA_KUBECONFIG}"
ensure_context "${KARMADA_HOST_CLUSTER}"
ensure_context "karmada-apiserver"

# Step 3: Deploy Kubernetes Reflector to share kubeconfig secret
echo "Deploying Kubernetes Reflector..."
kubectl --context "${KARMADA_HOST_CLUSTER}" -n kube-system apply -f \
    https://github.com/emberstack/kubernetes-reflector/releases/download/v7.1.262/reflector.yaml

echo "Annotating karmada-webhook-config secret for reflection..."
kubectl --context "${KARMADA_HOST_CLUSTER}" annotate secret karmada-webhook-config \
    reflector.v1.k8s.emberstack.com/reflection-allowed="true" \
    reflector.v1.k8s.emberstack.com/reflection-auto-namespaces="volcano-global" \
    reflector.v1.k8s.emberstack.com/reflection-auto-enabled="true" \
    --namespace=karmada-system --overwrite
ensure_exists "${KARMADA_HOST_CLUSTER}" -n karmada-system get secret karmada-webhook-config

# Step 4: Apply CRDs to Karmada API server
echo "Applying CRDs to Karmada API server..."
kubectl --context karmada-apiserver apply -f docs/deploy/training.volcano.sh_hyperjobs.yaml
kubectl --context karmada-apiserver apply -f \
    "https://raw.githubusercontent.com/volcano-sh/volcano/${VOLCANO_VERSION}/installer/helm/chart/volcano/crd/bases/batch.volcano.sh_jobs.yaml"
kubectl --context karmada-apiserver apply -f \
    "https://raw.githubusercontent.com/volcano-sh/volcano/${VOLCANO_VERSION}/installer/helm/chart/volcano/crd/bases/scheduling.volcano.sh_queues.yaml"
ensure_exists "karmada-apiserver" get crd hyperjobs.training.volcano.sh
ensure_exists "karmada-apiserver" get crd jobs.batch.volcano.sh
ensure_exists "karmada-apiserver" get crd queues.scheduling.volcano.sh

# Apply DataDependency CRDs when present (required by datadependency e2e suite).
if [ -d "docs/deploy/crds" ]; then
    echo "Applying DataDependency CRDs..."
    kubectl --context karmada-apiserver apply -f docs/deploy/crds/
fi


# Step 5: Deploy volcano-global controller and webhook manager
echo "Creating volcano-global namespace..."
kubectl --context karmada-apiserver apply -f docs/deploy/volcano-global-namespace.yaml
kubectl --context "${KARMADA_HOST_CLUSTER}" apply -f docs/deploy/volcano-global-namespace.yaml

echo "Waiting for kubeconfig secret to be reflected to volcano-global namespace..."
RETRY_COUNT=0
MAX_RETRIES=60
until kubectl --context "${KARMADA_HOST_CLUSTER}" -n volcano-global get secret karmada-webhook-config &>/dev/null; do
    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [ ${RETRY_COUNT} -ge ${MAX_RETRIES} ]; then
        echo "ERROR: Kubeconfig secret was not reflected to volcano-global namespace"
        exit 1
    fi
    echo "Waiting for kubeconfig secret reflection... (${RETRY_COUNT}/${MAX_RETRIES})"
    sleep 5
done
ensure_exists "${KARMADA_HOST_CLUSTER}" -n volcano-global get secret karmada-webhook-config

echo "Deploying volcano-global controller-manager..."
sed "s|image: .*volcano-global-controller-manager:.*|image: ${CONTROLLER_MANAGER_IMAGE}|" \
    docs/deploy/volcano-global-controller-manager.yaml | \
    sed "s|imagePullPolicy: .*|imagePullPolicy: IfNotPresent|" | \
    kubectl --context "${KARMADA_HOST_CLUSTER}" apply -f -

echo "Deploying volcano-global webhook-manager..."
sed "s|image: .*volcano-global-webhook-manager:.*|image: ${WEBHOOK_MANAGER_IMAGE}|" \
    docs/deploy/volcano-global-webhook-manager.yaml | \
    sed "s|imagePullPolicy: .*|imagePullPolicy: IfNotPresent|" | \
    kubectl --context "${KARMADA_HOST_CLUSTER}" apply -f -

echo "Applying webhook configuration..."
kubectl --context karmada-apiserver apply -f docs/deploy/volcano-global-webhooks.yaml
ensure_exists "karmada-apiserver" get mutatingwebhookconfiguration volcano-admission-service-resourcebindings-mutate
ensure_exists "karmada-apiserver" get mutatingwebhookconfiguration volcano-admission-service-jobs-mutate
ensure_exists "karmada-apiserver" get validatingwebhookconfiguration volcano-admission-service-jobs-validate

# Step 6: Apply resource interpreters
echo "Applying resource interpreters..."
kubectl --context karmada-apiserver apply -f docs/deploy/vcjob-resource-interpreter-customization.yaml
kubectl --context karmada-apiserver apply -f docs/deploy/queue-resource-interpreter-customization.yaml
ensure_exists "karmada-apiserver" get resourceinterpretercustomization vcjob-configuration
ensure_exists "karmada-apiserver" get resourceinterpretercustomization queue-configuration

# Step 7: Apply queue propagation policy
echo "Applying all-queue propagation policy..."
kubectl --context karmada-apiserver apply -f docs/deploy/volcano-global-all-queue-propagation.yaml
kubectl --context karmada-apiserver label clusterpropagationpolicy volcano-global-all-queue-propagation \
    resourcetemplate.karmada.io/deletion-protected=Always --overwrite
ensure_exists "karmada-apiserver" get clusterpropagationpolicy volcano-global-all-queue-propagation
actual_label=$(kubectl --context karmada-apiserver get clusterpropagationpolicy volcano-global-all-queue-propagation \
    -o jsonpath='{.metadata.labels.resourcetemplate\.karmada\.io/deletion-protected}')
if [ "${actual_label}" != "Always" ]; then
    echo "ERROR: Expected deletion-protected label to be 'Always', got '${actual_label}'"
    exit 1
fi

# Wait for volcano-global deployments to be ready
echo "Waiting for volcano-global deployments to be ready..."
kubectl --context "${KARMADA_HOST_CLUSTER}" -n volcano-global wait --for=condition=Available deployment --all --timeout=300s

echo "=== Volcano-Global deployed successfully ==="
kubectl --context "${KARMADA_HOST_CLUSTER}" -n volcano-global get pods
