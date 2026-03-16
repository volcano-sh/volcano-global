#!/usr/bin/env bash

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

# This script sets up a local multi-cluster environment for e2e testing
# It deploys Karmada, Volcano on member clusters, and volcano-global components
#
# TODO: Setup script enhancements:
# - Add macOS Docker networking workaround for local development
# - Add option to skip Karmada setup if already running
# - Add configurable number of member clusters
# - Add support for using pre-built images from registry
# - Add teardown/cleanup script

REPO_ROOT=$(dirname "${BASH_SOURCE[0]}")/..
cd "${REPO_ROOT}"

# Configuration
CLUSTER_VERSION=${CLUSTER_VERSION:-"kindest/node:v1.34.0"}
KARMADA_VERSION=${KARMADA_VERSION:-"v1.13.0-beta.0"}
VOLCANO_VERSION=${VOLCANO_VERSION:-"v1.10.0"}
ARTIFACTS_PATH=${ARTIFACTS_PATH:-"/tmp/volcano-global"}
NUM_MEMBER_CLUSTERS=${NUM_MEMBER_CLUSTERS:-2}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Create artifacts directory
mkdir -p "${ARTIFACTS_PATH}"

# Function to wait for a deployment to be ready
wait_deployment_ready() {
    local namespace=$1
    local deployment=$2
    local context=${3:-""}
    local timeout=${4:-300}
    
    log_info "Waiting for deployment ${deployment} in namespace ${namespace} to be ready..."
    
    local kubectl_cmd="kubectl"
    if [[ -n "${context}" ]]; then
        kubectl_cmd="kubectl --context ${context}"
    fi
    
    local start_time=$(date +%s)
    while true; do
        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))
        
        if [[ ${elapsed} -gt ${timeout} ]]; then
            log_error "Timeout waiting for deployment ${deployment}"
            ${kubectl_cmd} -n "${namespace}" describe deployment "${deployment}" || true
            ${kubectl_cmd} -n "${namespace}" get pods || true
            return 1
        fi
        
        local ready=$(${kubectl_cmd} -n "${namespace}" get deployment "${deployment}" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
        local desired=$(${kubectl_cmd} -n "${namespace}" get deployment "${deployment}" -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "1")
        
        if [[ "${ready}" == "${desired}" ]] && [[ "${ready}" != "0" ]]; then
            log_info "Deployment ${deployment} is ready (${ready}/${desired})"
            return 0
        fi
        
        echo -n "."
        sleep 5
    done
}

# Function to wait for a job to complete
wait_job_complete() {
    local namespace=$1
    local job=$2
    local context=${3:-""}
    local timeout=${4:-120}
    
    log_info "Waiting for job ${job} in namespace ${namespace} to complete..."
    
    local kubectl_cmd="kubectl"
    if [[ -n "${context}" ]]; then
        kubectl_cmd="kubectl --context ${context}"
    fi
    
    local start_time=$(date +%s)
    while true; do
        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))
        
        if [[ ${elapsed} -gt ${timeout} ]]; then
            log_error "Timeout waiting for job ${job}"
            ${kubectl_cmd} -n "${namespace}" describe job "${job}" || true
            ${kubectl_cmd} -n "${namespace}" logs -l job-name="${job}" || true
            return 1
        fi
        
        local status=$(${kubectl_cmd} -n "${namespace}" get job "${job}" -o jsonpath='{.status.succeeded}' 2>/dev/null || echo "0")
        
        if [[ "${status}" == "1" ]]; then
            log_info "Job ${job} completed successfully"
            return 0
        fi
        
        # Check for failed job
        local failed=$(${kubectl_cmd} -n "${namespace}" get job "${job}" -o jsonpath='{.status.failed}' 2>/dev/null || echo "0")
        if [[ "${failed}" != "0" ]] && [[ "${failed}" != "" ]]; then
            log_error "Job ${job} failed"
            ${kubectl_cmd} -n "${namespace}" logs -l job-name="${job}" || true
            return 1
        fi
        
        echo -n "."
        sleep 5
    done
}

# Step 1: Set up Karmada environment
setup_karmada() {
    log_info "Setting up Karmada environment..."
    
    # Clone Karmada repository
    local karmada_dir="/tmp/karmada"
    if [[ -d "${karmada_dir}" ]]; then
        log_info "Removing existing Karmada directory..."
        rm -rf "${karmada_dir}"
    fi
    
    log_info "Cloning Karmada repository (${KARMADA_VERSION})..."
    git clone --depth 1 --branch "${KARMADA_VERSION}" https://github.com/karmada-io/karmada.git "${karmada_dir}"
    
    cd "${karmada_dir}"
    
    # Set cluster version for kind
    export CLUSTER_VERSION="${CLUSTER_VERSION}"
    
    # Run Karmada local-up script
    log_info "Running Karmada local-up script..."
    ./hack/local-up-karmada.sh
    
    cd "${REPO_ROOT}"
    
    log_info "Karmada setup completed successfully"
}

# Step 2: Deploy Volcano on member clusters
deploy_volcano() {
    log_info "Deploying Volcano on member clusters..."
    
    export KUBECONFIG="${HOME}/.kube/members.config"
    
    for i in $(seq 1 ${NUM_MEMBER_CLUSTERS}); do
        local member="member${i}"
        log_info "Deploying Volcano on ${member}..."
        
        kubectl --context "${member}" apply -f "https://raw.githubusercontent.com/volcano-sh/volcano/release-1.10/installer/volcano-development.yaml"
        
        # Wait for Volcano components to be ready
        log_info "Waiting for Volcano scheduler on ${member}..."
        wait_deployment_ready "volcano-system" "volcano-scheduler" "${member}" 300 || {
            log_error "Failed to deploy Volcano scheduler on ${member}"
            return 1
        }
        
        log_info "Waiting for Volcano controller on ${member}..."
        wait_deployment_ready "volcano-system" "volcano-controllers" "${member}" 300 || {
            log_error "Failed to deploy Volcano controller on ${member}"
            return 1
        }
    done
    
    log_info "Volcano deployed on all member clusters"
}

# Step 3: Deploy Kubernetes Reflector for secret sharing
deploy_reflector() {
    log_info "Deploying Kubernetes Reflector..."
    
    export KUBECONFIG="${HOME}/.kube/karmada.config"
    
    # Deploy reflector
    kubectl --context karmada-host -n kube-system apply -f https://github.com/emberstack/kubernetes-reflector/releases/download/v7.1.262/reflector.yaml
    
    # Wait for reflector to be ready
    wait_deployment_ready "kube-system" "reflector" "karmada-host" 120 || {
        log_error "Failed to deploy Kubernetes Reflector"
        return 1
    }
    
    # Annotate the secret for reflection
    kubectl --context karmada-host annotate secret karmada-webhook-config \
        reflector.v1.k8s.emberstack.com/reflection-allowed="true" \
        reflector.v1.k8s.emberstack.com/reflection-auto-namespaces="volcano-global" \
        reflector.v1.k8s.emberstack.com/reflection-auto-enabled="true" \
        --namespace=karmada-system --overwrite
    
    log_info "Kubernetes Reflector deployed successfully"
}

# Step 4: Apply required CRDs to Karmada control plane
apply_crds() {
    log_info "Applying required CRDs to Karmada control plane..."
    
    export KUBECONFIG="${HOME}/.kube/karmada.config"
    
    # Apply Volcano CRDs to Karmada API server
    kubectl --context karmada-apiserver apply -f docs/deploy/training.volcano.sh_hyperjobs.yaml
    kubectl --context karmada-apiserver apply -f https://raw.githubusercontent.com/volcano-sh/volcano/release-1.10/installer/helm/chart/volcano/crd/bases/batch.volcano.sh_jobs.yaml
    kubectl --context karmada-apiserver apply -f https://raw.githubusercontent.com/volcano-sh/volcano/release-1.10/installer/helm/chart/volcano/crd/bases/scheduling.volcano.sh_queues.yaml
    
    log_info "CRDs applied successfully"
}

# Step 5: Build and load volcano-global images
build_and_load_images() {
    log_info "Building volcano-global images..."
    
    # Build the binaries
    make all
    
    # Build Docker images
    export TAG="e2e-test"
    export IMAGE_PREFIX="volcanosh"
    
    for component in controller-manager webhook-manager; do
        log_info "Building image for ${component}..."
        docker build -t "${IMAGE_PREFIX}/volcano-global-${component}:${TAG}" \
            -f "./installer/dockerfile/${component}/Dockerfile" .
    done
    
    # Load images into kind clusters
    export KUBECONFIG="${HOME}/.kube/karmada.config"
    
    # Get the karmada-host cluster name (typically karmada-host)
    local host_cluster="karmada-host"
    
    for component in controller-manager webhook-manager; do
        log_info "Loading ${component} image into ${host_cluster}..."
        kind load docker-image "${IMAGE_PREFIX}/volcano-global-${component}:${TAG}" --name "${host_cluster}"
    done
    
    log_info "Images built and loaded successfully"
}

# Step 6: Deploy volcano-global components
deploy_volcano_global() {
    log_info "Deploying volcano-global components..."
    
    export KUBECONFIG="${HOME}/.kube/karmada.config"
    local TAG="e2e-test"
    
    # Create namespace in Karmada API server (for leader election)
    kubectl --context karmada-apiserver apply -f docs/deploy/volcano-global-namespace.yaml
    
    # Create namespace in Karmada host cluster
    kubectl --context karmada-host apply -f docs/deploy/volcano-global-namespace.yaml
    
    # Wait for the secret to be reflected
    log_info "Waiting for karmada-webhook-config secret to be reflected to volcano-global namespace..."
    local max_wait=60
    local waited=0
    while [[ ${waited} -lt ${max_wait} ]]; do
        if kubectl --context karmada-host -n volcano-global get secret karmada-webhook-config &>/dev/null; then
            log_info "Secret reflected successfully"
            break
        fi
        sleep 2
        waited=$((waited + 2))
    done
    
    if [[ ${waited} -ge ${max_wait} ]]; then
        log_warn "Secret not reflected, creating manually..."
        # Copy the secret manually if reflector didn't work
        kubectl --context karmada-host -n karmada-system get secret karmada-webhook-config -o yaml | \
            sed 's/namespace: karmada-system/namespace: volcano-global/' | \
            kubectl --context karmada-host apply -f -
    fi
    
    # Deploy webhook manager (includes the init job for certificates)
    log_info "Deploying volcano-global-webhook-manager..."
    sed "s|image: volcanosh/volcano-global-webhook-manager:latest|image: volcanosh/volcano-global-webhook-manager:${TAG}|g" \
        docs/deploy/volcano-global-webhook-manager.yaml | \
        sed 's|imagePullPolicy: IfNotPresent|imagePullPolicy: Never|g' | \
        kubectl --context karmada-host apply -f -
    
    # Wait for the admission init job to complete
    wait_job_complete "volcano-global" "volcano-global-admission-init" "karmada-host" 120 || {
        log_warn "Admission init job may have completed in a previous run, continuing..."
    }
    
    # Wait for webhook manager to be ready
    wait_deployment_ready "volcano-global" "volcano-global-webhook-manager" "karmada-host" 300 || {
        log_error "Failed to deploy webhook manager"
        return 1
    }
    
    # Deploy controller manager
    log_info "Deploying volcano-global-controller-manager..."
    sed "s|image: volcanosh/volcano-global-controller-manager:latest|image: volcanosh/volcano-global-controller-manager:${TAG}|g" \
        docs/deploy/volcano-global-controller-manager.yaml | \
        sed 's|imagePullPolicy: IfNotPresent|imagePullPolicy: Never|g' | \
        kubectl --context karmada-host apply -f -
    
    # Wait for controller manager to be ready
    wait_deployment_ready "volcano-global" "volcano-global-controller-manager" "karmada-host" 300 || {
        log_error "Failed to deploy controller manager"
        return 1
    }
    
    # Apply webhook configuration
    kubectl --context karmada-apiserver apply -f docs/deploy/volcano-global-webhooks.yaml
    
    # Apply resource interpreters
    kubectl --context karmada-apiserver apply -f docs/deploy/vcjob-resource-interpreter-customization.yaml
    kubectl --context karmada-apiserver apply -f docs/deploy/queue-resource-interpreter-customization.yaml
    
    # Apply queue propagation policy
    kubectl --context karmada-apiserver apply -f docs/deploy/volcano-global-all-queue-propagation.yaml
    
    log_info "volcano-global components deployed successfully"
}

# Step 7: Verify the setup
verify_setup() {
    log_info "Verifying the setup..."
    
    export KUBECONFIG="${HOME}/.kube/karmada.config"
    
    # Check volcano-global components
    log_info "Checking volcano-global components..."
    kubectl --context karmada-host -n volcano-global get pods
    
    # Check Karmada clusters
    log_info "Checking Karmada member clusters..."
    kubectl --context karmada-apiserver get clusters
    
    # Check Volcano on member clusters
    export KUBECONFIG="${HOME}/.kube/members.config"
    for i in $(seq 1 ${NUM_MEMBER_CLUSTERS}); do
        local member="member${i}"
        log_info "Checking Volcano on ${member}..."
        kubectl --context "${member}" -n volcano-system get pods
    done
    
    log_info "Setup verification completed"
}

# Function to collect logs for debugging
collect_logs() {
    log_info "Collecting logs for debugging..."
    
    mkdir -p "${ARTIFACTS_PATH}/logs"
    
    export KUBECONFIG="${HOME}/.kube/karmada.config"
    
    # Collect volcano-global logs
    kubectl --context karmada-host -n volcano-global logs -l app=volcano-global-controller-manager --tail=-1 > "${ARTIFACTS_PATH}/logs/controller-manager.log" 2>&1 || true
    kubectl --context karmada-host -n volcano-global logs -l app=volcano-global-webhook-manager --tail=-1 > "${ARTIFACTS_PATH}/logs/webhook-manager.log" 2>&1 || true
    
    # Collect Karmada component logs
    kubectl --context karmada-host -n karmada-system logs -l app=karmada-controller-manager --tail=-1 > "${ARTIFACTS_PATH}/logs/karmada-controller-manager.log" 2>&1 || true
    kubectl --context karmada-host -n karmada-system logs -l app=karmada-scheduler --tail=-1 > "${ARTIFACTS_PATH}/logs/karmada-scheduler.log" 2>&1 || true
    
    # Collect member cluster logs
    export KUBECONFIG="${HOME}/.kube/members.config"
    for i in $(seq 1 ${NUM_MEMBER_CLUSTERS}); do
        local member="member${i}"
        kubectl --context "${member}" -n volcano-system logs -l app=volcano-scheduler --tail=-1 > "${ARTIFACTS_PATH}/logs/volcano-scheduler-${member}.log" 2>&1 || true
        kubectl --context "${member}" -n volcano-system logs -l app=volcano-controller-manager --tail=-1 > "${ARTIFACTS_PATH}/logs/volcano-controller-${member}.log" 2>&1 || true
    done
    
    log_info "Logs collected to ${ARTIFACTS_PATH}/logs/"
}

# Main execution
main() {
    log_info "Starting volcano-global e2e test environment setup..."
    log_info "Configuration:"
    log_info "  CLUSTER_VERSION: ${CLUSTER_VERSION}"
    log_info "  KARMADA_VERSION: ${KARMADA_VERSION}"
    log_info "  VOLCANO_VERSION: ${VOLCANO_VERSION}"
    log_info "  NUM_MEMBER_CLUSTERS: ${NUM_MEMBER_CLUSTERS}"
    log_info "  ARTIFACTS_PATH: ${ARTIFACTS_PATH}"
    
    # Execute setup steps
    setup_karmada
    deploy_volcano
    deploy_reflector
    apply_crds
    build_and_load_images
    deploy_volcano_global
    verify_setup
    
    log_info "=========================================="
    log_info "volcano-global e2e test environment is ready!"
    log_info "=========================================="
    log_info ""
    log_info "Karmada config: ${HOME}/.kube/karmada.config"
    log_info "Member config: ${HOME}/.kube/members.config"
    log_info ""
    log_info "To access Karmada API server:"
    log_info "  export KUBECONFIG=${HOME}/.kube/karmada.config"
    log_info "  kubectl --context karmada-apiserver get clusters"
    log_info ""
    log_info "To access member clusters:"
    log_info "  export KUBECONFIG=${HOME}/.kube/members.config"
    log_info "  kubectl --context member1 get pods -A"
}

# Trap to collect logs on failure
trap 'collect_logs' ERR

main "$@"
