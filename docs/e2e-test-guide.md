# Volcano-Global E2E Test Guide

## Overview

This guide explains how to run and extend the `volcano-global` e2e suite.

The e2e pipeline validates:
- Resource quota and priority dispatch behavior
- Cross-cluster VCJob scheduling
- Data dependency aware scheduling
- HyperJob scheduling

## Prerequisites

- Docker
- Go (version from `go.mod`)
- `kubectl`
- `kind`
- Internet access to pull Karmada/Volcano manifests and images

## Local Run

From repository root:

```bash
./hack/setup-e2e-env.sh
make e2e-test
```

Logs are collected in `volcano-global-e2e-logs/` by default.
For one-command execution, run:

```bash
./hack/e2e.sh
```

For reruns without reprovisioning:

```bash
SKIP_SETUP=true ./hack/e2e.sh
```

## Test Layout

```text
test/e2e/
  framework/
    clients.go
    framework.go
    vcjob.go
    queue.go
    propagationpolicy.go
    resourcebinding.go
    hyperjob.go
  quota/
  vcjob/
  hyperjob/
  datadependency/
```

## CI Workflow

The workflow is at `.github/workflows/e2e.yaml` and runs on:
- `pull_request` to `main`
- `push` to `main`
- manual `workflow_dispatch`

Main CI stages:
1. Setup toolchain (`go`, `kind`, `kubectl`)
2. Bootstrap Karmada multi-cluster environment
3. Install Volcano on member clusters
4. Deploy volcano-global components
5. Run ginkgo e2e tests (`SKIP_SETUP=true` in CI because setup was done in prior steps)
6. Upload logs as artifacts (`if: always()`)

## Deploy Guide Coverage Checks

The script flow mirrors `docs/deploy/README.md` and validates:
- reflector and `karmada-webhook-config` secret reflection into `volcano-global`
- CRDs applied to `karmada-apiserver` context
- webhook configurations present on `karmada-apiserver`
- resource interpreter customizations (`vcjob-configuration`, `queue-configuration`)
- all-queue ClusterPropagationPolicy label `resourcetemplate.karmada.io/deletion-protected=Always`

## Adding New E2E Cases

1. Add shared logic under `test/e2e/framework/` if reusable.
2. Add test spec in the feature suite package (`quota`, `vcjob`, `hyperjob`, `datadependency`).
3. Keep tests:
   - deterministic
   - isolated (use unique namespaces/resources)
   - cleanup-safe
4. Re-run:

```bash
make e2e-test
```

## Known Limitation

Current upstream interpreter behavior for divided VCJob replicas can produce invalid `minAvailable`/`replicas` combinations in some scenarios. Prefer Aggregated placement in VCJob e2e tests until upstream fix is merged.

