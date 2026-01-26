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

# This script runs e2e tests for volcano-global

REPO_ROOT=$(dirname "${BASH_SOURCE[0]}")/..
cd "${REPO_ROOT}"

# Configuration
ARTIFACTS_PATH=${ARTIFACTS_PATH:-"/tmp/volcano-global/e2e-logs"}
E2E_FOCUS=${E2E_FOCUS:-""}
E2E_SKIP=${E2E_SKIP:-""}
E2E_PARALLEL=${E2E_PARALLEL:-1}
E2E_FLAKE_ATTEMPTS=${E2E_FLAKE_ATTEMPTS:-2}
E2E_TIMEOUT=${E2E_TIMEOUT:-"30m"}

# Kubeconfig paths
KARMADA_CONFIG="${HOME}/.kube/karmada.config"
MEMBERS_CONFIG="${HOME}/.kube/members.config"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

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

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check if ginkgo is installed
    if ! command -v ginkgo &> /dev/null; then
        log_info "Installing ginkgo..."
        go install github.com/onsi/ginkgo/v2/ginkgo@latest
    fi
    
    # Check if kubeconfig files exist
    if [[ ! -f "${KARMADA_CONFIG}" ]]; then
        log_error "Karmada config not found at ${KARMADA_CONFIG}"
        log_error "Please run hack/local-up-volcano-global.sh first"
        exit 1
    fi
    
    if [[ ! -f "${MEMBERS_CONFIG}" ]]; then
        log_error "Members config not found at ${MEMBERS_CONFIG}"
        log_error "Please run hack/local-up-volcano-global.sh first"
        exit 1
    fi
    
    log_info "Prerequisites check passed"
}

# Collect logs from all clusters
collect_logs() {
    log_info "Collecting logs..."
    
    local log_dir="${ARTIFACTS_PATH}/cluster-logs"
    mkdir -p "${log_dir}"
    
    # Collect Karmada host cluster logs
    export KUBECONFIG="${KARMADA_CONFIG}"
    
    log_info "Collecting Karmada host cluster logs..."
    kubectl --context karmada-host cluster-info dump --output-directory="${log_dir}/karmada-host" || true
    
    # Collect volcano-global component logs
    kubectl --context karmada-host -n volcano-global logs -l app=volcano-global-controller-manager --tail=10000 > "${log_dir}/volcano-global-controller-manager.log" 2>&1 || true
    kubectl --context karmada-host -n volcano-global logs -l app=volcano-global-webhook-manager --tail=10000 > "${log_dir}/volcano-global-webhook-manager.log" 2>&1 || true
    
    # Collect Karmada component logs
    kubectl --context karmada-host -n karmada-system logs -l app=karmada-controller-manager --tail=5000 > "${log_dir}/karmada-controller-manager.log" 2>&1 || true
    kubectl --context karmada-host -n karmada-system logs -l app=karmada-scheduler --tail=5000 > "${log_dir}/karmada-scheduler.log" 2>&1 || true
    kubectl --context karmada-host -n karmada-system logs -l app=karmada-webhook --tail=5000 > "${log_dir}/karmada-webhook.log" 2>&1 || true
    
    # Collect member cluster logs
    export KUBECONFIG="${MEMBERS_CONFIG}"
    for member in member1 member2; do
        log_info "Collecting ${member} cluster logs..."
        mkdir -p "${log_dir}/${member}"
        kubectl --context "${member}" cluster-info dump --output-directory="${log_dir}/${member}" || true
        
        # Collect Volcano logs
        kubectl --context "${member}" -n volcano-system logs -l app=volcano-scheduler --tail=5000 > "${log_dir}/${member}/volcano-scheduler.log" 2>&1 || true
        kubectl --context "${member}" -n volcano-system logs -l app=volcano-controller-manager --tail=5000 > "${log_dir}/${member}/volcano-controller.log" 2>&1 || true
    done
    
    log_info "Logs collected to ${log_dir}"
}

# Run e2e tests
run_e2e_tests() {
    log_info "Running e2e tests..."
    log_info "Configuration:"
    log_info "  ARTIFACTS_PATH: ${ARTIFACTS_PATH}"
    log_info "  E2E_FOCUS: ${E2E_FOCUS:-'(all tests)'}"
    log_info "  E2E_SKIP: ${E2E_SKIP:-'(none)'}"
    log_info "  E2E_PARALLEL: ${E2E_PARALLEL}"
    log_info "  E2E_TIMEOUT: ${E2E_TIMEOUT}"
    
    # Set environment variables for tests
    export KARMADA_KUBECONFIG="${KARMADA_CONFIG}"
    export MEMBERS_KUBECONFIG="${MEMBERS_CONFIG}"
    export E2E_ARTIFACTS="${ARTIFACTS_PATH}"
    
    # Build ginkgo flags
    local ginkgo_flags=(
        "-v"
        "--timeout=${E2E_TIMEOUT}"
        "--procs=${E2E_PARALLEL}"
        "--flake-attempts=${E2E_FLAKE_ATTEMPTS}"
        "--output-dir=${ARTIFACTS_PATH}"
        "--json-report=e2e-report.json"
        "--junit-report=e2e-junit.xml"
    )
    
    if [[ -n "${E2E_FOCUS}" ]]; then
        ginkgo_flags+=("--focus=${E2E_FOCUS}")
    fi
    
    if [[ -n "${E2E_SKIP}" ]]; then
        ginkgo_flags+=("--skip=${E2E_SKIP}")
    fi
    
    # Run the tests
    cd "${REPO_ROOT}"
    
    log_info "Executing: ginkgo ${ginkgo_flags[*]} ./test/e2e/..."
    
    local exit_code=0
    ginkgo "${ginkgo_flags[@]}" ./test/e2e/... || exit_code=$?
    
    return ${exit_code}
}

# Print test summary
print_summary() {
    local exit_code=$1
    
    echo ""
    echo "=========================================="
    if [[ ${exit_code} -eq 0 ]]; then
        log_info "E2E Tests PASSED"
    else
        log_error "E2E Tests FAILED (exit code: ${exit_code})"
    fi
    echo "=========================================="
    echo ""
    log_info "Test artifacts saved to: ${ARTIFACTS_PATH}"
    
    # Print report summary if available
    if [[ -f "${ARTIFACTS_PATH}/e2e-report.json" ]]; then
        log_info "Test report: ${ARTIFACTS_PATH}/e2e-report.json"
    fi
    if [[ -f "${ARTIFACTS_PATH}/e2e-junit.xml" ]]; then
        log_info "JUnit report: ${ARTIFACTS_PATH}/e2e-junit.xml"
    fi
}

# Trap to collect logs on exit
cleanup() {
    local exit_code=$?
    collect_logs
    print_summary ${exit_code}
}

trap cleanup EXIT

# Main execution
main() {
    check_prerequisites
    run_e2e_tests
}

main "$@"
