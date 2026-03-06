# HyperJob User Guide

## Overview

HyperJob is a higher-level abstraction built on top of Volcano Job. It enables
large-scale AI training workloads to be distributed across multiple heterogeneous
clusters, while preserving the full capabilities of existing Volcano Jobs within
each cluster.

With HyperJob, you can:
- Split training jobs that exceed single-cluster GPU capacity across multiple clusters
- Target specific clusters with different accelerator types (e.g., NVIDIA GPUs, Ascend NPUs)
- Track the status of all sub-jobs through a single unified resource

For the controller implementation details, see the
[HyperJob Controller Design](../proposals/hyperjob-controller-design.md).

## Prerequisites

Before using HyperJob, ensure that:
1. **Volcano Global** is deployed and running (see the [Deploy Guide](../deploy/README.md))
2. **Karmada** control plane is running with member clusters joined
3. **Volcano** (v1.10.0+) is installed on all member clusters
4. The **HyperJob CRD** is applied on the Karmada API server:
   ```bash
   export KUBECONFIG=$HOME/.kube/karmada.config
   kubectl --context karmada-apiserver apply -f docs/deploy/training.volcano.sh_hyperjobs.yaml
   ```

## API Reference

### HyperJob

```yaml
apiVersion: training.volcano.sh/v1alpha1
kind: HyperJob
metadata:
  name: <name>
  namespace: <namespace>
spec:
  minAvailable: <int>          # Minimum number of VCJobs that must be running (optional)
  replicatedJobs:              # List of job templates
  - name: <string>             # Unique name for this job group within the HyperJob
    replicas: <int>            # Number of VCJobs to create from this template (default: 1)
    clusterNames:              # Preferred cluster names for scheduling (optional)
    - <cluster-name>
    templateSpec:              # Volcano Job spec template
      <VCJob spec fields>
```

### Field Descriptions

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `spec.minAvailable` | int32 | No | Minimum number of VCJobs that must be running. Reserved for future fault-tolerance use; currently not enforced by the controller. |
| `spec.replicatedJobs` | []ReplicatedJob | Yes | List of job templates. Each entry defines a group of identical VCJobs. |
| `replicatedJobs[].name` | string | Yes | Unique identifier for this replicated job within the HyperJob. |
| `replicatedJobs[].replicas` | int32 | No | Number of VCJobs to create from this template. Defaults to 1. |
| `replicatedJobs[].clusterNames` | []string | No | Preferred clusters for scheduling. The controller attempts to place replicas on these clusters first. |
| `replicatedJobs[].templateSpec` | JobSpec | Yes | The Volcano Job specification used as a template for creating child VCJobs. |

### Status

The HyperJob status provides a unified view of all child VCJobs:

```yaml
status:
  splitCount: <int>             # Total number of VCJobs created
  observedGeneration: <int>     # Spec generation last reconciled
  conditions:                   # Set only when ALL child VCJobs are in terminal states
  - type: Completed             # All child VCJobs completed successfully
    status: "True"
  - type: Failed                # All child VCJobs finished but at least one failed
    status: "True"
  replicatedJobsStatus:         # Per-replicated-job status
  - name: <replicatedjob-name>
    jobStates:
      <vcjob-name>: <phase>     # Phase of each individual VCJob
    pending: <int>
    running: <int>
    succeeded: <int>
    failed: <int>
    terminating: <int>
    unknown: <int>
```

**Key status behaviors:**
- While child VCJobs are running, no `conditions` are set. This lets you distinguish in-progress from terminal jobs.
- `Completed` condition is set when **all** child VCJobs complete successfully.
- `Failed` condition is set when **all** child VCJobs finish but at least one failed, was aborted, or was terminated.

## Child Resource Naming

The controller creates VCJobs and PropagationPolicies following this naming pattern:

```
{hyperjob-name}-{replicatedjob-name}-{index}
```

**Example:** A HyperJob named `llm-training` with a ReplicatedJob named `trainer` and `replicas: 2` creates:
- VCJobs: `llm-training-trainer-0`, `llm-training-trainer-1`
- PropagationPolicies: `llm-training-trainer-0`, `llm-training-trainer-1`

## Use Cases

### Case 1: Large-scale Training Job Splitting

Split a large LLM training job that requires 256 GPUs across two clusters, each
with 128 GPUs available.

```yaml
apiVersion: training.volcano.sh/v1alpha1
kind: HyperJob
metadata:
  name: llm-training
  namespace: default
spec:
  minAvailable: 2
  replicatedJobs:
  - name: trainer
    replicas: 2
    templateSpec:
      minAvailable: 1
      schedulerName: volcano
      tasks:
      - name: worker
        replicas: 128
        template:
          spec:
            containers:
            - name: trainer
              image: training-image:v1
              resources:
                requests:
                  nvidia.com/gpu: "1"
                limits:
                  nvidia.com/gpu: "1"
            restartPolicy: OnFailure
```

This creates two VCJobs (`llm-training-trainer-0` and `llm-training-trainer-1`),
each with 128 worker pods requesting one GPU. Karmada will distribute these jobs
to available clusters.

See the full example: [`docs/deploy/example/hyperjob/llm-training.yaml`](../deploy/example/hyperjob/llm-training.yaml)

### Case 2: Heterogeneous Cluster Training

Target different accelerator types on different clusters. This example uses Ascend
NPU clusters (910B and 910C) with node affinity to ensure pods land on the correct
hardware.

```yaml
apiVersion: training.volcano.sh/v1alpha1
kind: HyperJob
metadata:
  name: llm-heterogeneous
  namespace: default
spec:
  minAvailable: 2
  replicatedJobs:
  - name: trainer-910b
    replicas: 1
    clusterNames:
    - cluster-ascend-910b-1
    - cluster-ascend-910b-2
    templateSpec:
      minAvailable: 1
      schedulerName: volcano
      tasks:
      - name: worker
        replicas: 64
        template:
          spec:
            affinity:
              nodeAffinity:
                requiredDuringSchedulingIgnoredDuringExecution:
                  nodeSelectorTerms:
                  - matchExpressions:
                    - key: hardware-type
                      operator: In
                      values:
                      - Ascend910B
            containers:
            - name: trainer
              image: training-image:v1
              resources:
                requests:
                  huawei.com/ascend910b: "1"
                limits:
                  huawei.com/ascend910b: "1"
            restartPolicy: OnFailure
  - name: trainer-910c
    replicas: 1
    clusterNames:
    - cluster-ascend-910c-1
    templateSpec:
      minAvailable: 1
      schedulerName: volcano
      tasks:
      - name: worker
        replicas: 64
        template:
          spec:
            affinity:
              nodeAffinity:
                requiredDuringSchedulingIgnoredDuringExecution:
                  nodeSelectorTerms:
                  - matchExpressions:
                    - key: hardware-type
                      operator: In
                      values:
                      - Ascend910C
            containers:
            - name: trainer
              image: training-image:v1
              resources:
                requests:
                  huawei.com/ascend910c: "1"
                limits:
                  huawei.com/ascend910c: "1"
            restartPolicy: OnFailure
```

The controller creates one VCJob per ReplicatedJob, each pinned to the specified
cluster(s) via the generated PropagationPolicy's `clusterAffinity`.

See the full example: [`docs/deploy/example/hyperjob/heterogeneous-training.yaml`](../deploy/example/hyperjob/heterogeneous-training.yaml)

### Case 3: Training with Cluster Affinity and Fault Tolerance

Run a fault-tolerant training job across three clusters with `minAvailable: 2`,
so the HyperJob remains healthy even if one cluster becomes unavailable.

```yaml
apiVersion: training.volcano.sh/v1alpha1
kind: HyperJob
metadata:
  name: resilient-training
  namespace: default
spec:
  minAvailable: 2
  replicatedJobs:
  - name: trainer
    replicas: 3
    clusterNames:
    - cluster-gpu-west-1
    - cluster-gpu-east-1
    - cluster-gpu-central-1
    templateSpec:
      minAvailable: 1
      schedulerName: volcano
      policies:
      - event: PodEvicted
        action: RestartJob
      tasks:
      - name: worker
        replicas: 32
        template:
          spec:
            containers:
            - name: trainer
              image: training-image:v1
              resources:
                requests:
                  nvidia.com/gpu: "1"
                limits:
                  nvidia.com/gpu: "1"
            restartPolicy: OnFailure
```

See the full example: [`docs/deploy/example/hyperjob/cluster-affinity-training.yaml`](../deploy/example/hyperjob/cluster-affinity-training.yaml)

## Applying a HyperJob

```bash
# Switch to Karmada control plane context
export KUBECONFIG=$HOME/.kube/karmada.config

# Apply the HyperJob
kubectl --context karmada-apiserver apply -f <your-hyperjob.yaml>
```

## Monitoring Status

```bash
# Check HyperJob status
kubectl --context karmada-apiserver get hyperjob <name> -o yaml

# Check child VCJobs created by the HyperJob
kubectl --context karmada-apiserver get vcjob -l volcano.sh/hyperjob-name=<name>

# Check pods in a specific member cluster
kubectl --context <member-cluster> get pods
```

### Example status output

```yaml
status:
  splitCount: 2
  observedGeneration: 1
  replicatedJobsStatus:
  - name: trainer
    jobStates:
      llm-training-trainer-0: Running
      llm-training-trainer-1: Running
    running: 256
    pending: 0
    succeeded: 0
    failed: 0
```

## Deleting a HyperJob

Deleting a HyperJob automatically removes all child VCJobs and PropagationPolicies
through Kubernetes garbage collection (owner references):

```bash
kubectl --context karmada-apiserver delete hyperjob <name>
```

## Troubleshooting

### HyperJob not creating child VCJobs

1. Verify the HyperJob CRD is installed on the Karmada API server:
   ```bash
   kubectl --context karmada-apiserver get crd hyperjobs.training.volcano.sh
   ```
2. Check the volcano-global controller manager logs:
   ```bash
   kubectl --context karmada-host -n volcano-global logs -l app=volcano-global-controller-manager
   ```

### VCJobs created but not scheduled to clusters

1. Verify that PropagationPolicies are created alongside the VCJobs:
   ```bash
   kubectl --context karmada-apiserver get propagationpolicy -l volcano.sh/hyperjob-name=<name>
   ```
2. Check the Karmada scheduler logs for placement decisions:
   ```bash
   kubectl --context karmada-host -n karmada-system logs -l app=karmada-scheduler
   ```
3. Ensure member clusters are joined and have sufficient resources.

### HyperJob stuck with no conditions

This is expected behavior when child VCJobs are still running. Conditions are only
set once **all** child VCJobs reach a terminal state (Completed, Failed, Aborted,
or Terminated).

## References

- [HyperJob API Design](https://github.com/volcano-sh/volcano/blob/master/docs/design/hyperjob-multi-cluster-job-splitting.md)
- [HyperJob Controller Design](../proposals/hyperjob-controller-design.md)
- [Deploy Guide](../deploy/README.md)
- [Karmada PropagationPolicy](https://karmada.io/docs/userguide/scheduling/resource-propagating)
- [Volcano Job](https://volcano.sh/en/docs/vcjob/)
