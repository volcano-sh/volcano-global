# Volcano Global Supports Queue Capacity Management

@Vacant2333 2024/9/22

## Introduction

Target issue: [OSPP 2024: Volcano Support Multi-Cloud AI Job Scheduling(queue capacity management)](https://github.com/volcano-sh/volcano/issues/3731)

With the rapid development of large AI models, a single K8s cluster is increasingly unable to meet the needs of
large model AI job training due to resource and performance bottlenecks. More and more users are using
multiple clusters to manage and run AI jobs. Volcano is developing Supports task scheduling of multi-cluster AI jobs,
which involves multi-cluster job management, multi-tenant task fair scheduling, queue management and
other series of requirements. The multi-cluster orchestration system [Karmada](https://karmada.io/) has gradually become an industry standard.
Volcano needs to build AI job scheduling capabilities in multi-cluster scenarios based on Karmada's existing capabilities,
and make up for the lack of queue management and other capabilities in Karmada scheduling to solve AI jobs in multi-cluster scenarios.
Task scheduling, queue management, and **multi-tenant quota management** issues.

This proposal targets the queue capacity management capability.

### Goals

- Support queue capacity management
- Enhance Dispatcher flexibility

## Proposal

<!--
在 Karmada 中，负载在下发时会在 Karmada Scheduler 中检查 Worker Cluster 资源是否足够，可以很好避免资源较为碎片的情况。
但是 volcano-global 的 Dispatcher 生效在负载被 Scheduler 处理之前，我们在 Dispatcher 实现队列以及容量管理的能力，但是我们无法在
Dispatcher 中做到像 Scheduler 一样精确的判断资源是否真正足够任务下发，在资源碎片的情况下是有可能会出现任务可以入队但是无法正常运行的情况。

Dispatcher 需要通过检查对应的 Status Condition 来避免任务持续占用队列资源，直到该任务的 Condition 更新为正常执行才会导致
Queue.allocated 变更，基于此，我们仅能保证资源不足的情况下任务一定不会下发。
-->

In Karmada, when workloads are distributed,
the Karmada Scheduler checks whether the Worker Cluster has sufficient resources,
effectively avoiding scenarios with fragmented resources.  
However, the `volcano-global` Dispatcher is activated before the workloads are processed by the Scheduler.
While the Dispatcher implements queue and capacity management capabilities,
it cannot make as precise judgments as the Scheduler
on whether the resources are truly sufficient for task distribution.
In cases of fragmented resources, it is possible for tasks to be enqueued but unable to run normally.

The Dispatcher needs
to check the corresponding Status Condition to prevent tasks from continuously occupying queue resources.
Only when a task's Condition is updated to normal execution will the `Queue.allocated` change.
Based on this, we can only guarantee that tasks will definitely not be dispatched in cases of insufficient resources.

### Background

![volcano-global-design](images/volcano_global_design.png)

<!--
目前，volcano-global 的设计目标是以一种类似于 Kueue 的方式实现调度功能。
当我们创建一个 ResourceBinding（在 Karmada 中的一个 CRD，用于描述“资源分配到工作集群”的操作）时，
我们会将其暂停，类似于暂停一个 Job 资源。然后，它会交给我们的调度器（控制器）来实现排队、排序和容量管理等功能。

目前，调度器只能处理队列和任务排序。

我们需要在此基础上扩展调度器的可扩展性，以实现容量管理和多租户所需的功能，如 allocatable（判断一个任务是否可以提交）、
overused（在每次任务调度前判断是否超出应有的使用量）以及调度器应该扩展的其他支持点。
-->

Currently, the design goal of `volcano-global` is to implement dispatch capabilities in a manner similar to `Kueue`.
When we create a `ResourceBinding` (a CRD in Karmada,
used to describe the operation of 'resource distribution to worker clusters'),
we suspend it similar to a Job resource.
Then, it is handed over to our Dispatcher (Controller) to implement capabilities such as queuing,
sorting, and capacity management.

Currently, the Dispatcher can only handle queue and task sorting.

We need to expand the extensibility of the Dispatcher on this basis, to implement capacity management and multi-tenant
required capabilities such as **allocatable** (determining whether a task can be submitted), **overused**
(judging whether it exceeds deserve before each task dispatch), and other support points that the Dispatcher should extend.

### Capacity Plugin

My implementation plan is to provide capacity management capabilities in the **Dispatcher** through the form of plugins.
For example, job sorting capabilities in the **Dispatcher** are implemented through the **Priority** plugin.
I will name this plugin **Capacity**,
and it will provide the necessary functionality for capacity management by implementing the following **Func**.

#### QueueInfo Struct

```go
type queueInfo struct {
	id api.QueueID
	name string
	share float64
	
	// realCapacity represents the resource limit of the queue.
	realCapacity *api.Resource
	// allocatedCapacity represents the resource request of all allocated jobs in the queue.
	allocatedCapacity *api.Resource
}
```

#### Enqueueable Func

Enqueueable determines whether the load can be enqueued by checking the following items:

1.	Whether `queue.capacity` is set
2.	Whether `workload.MinResource` is set
3.	If `queue.remainCapacity` > `workload.MinResource`

#### Allocatable Func

<!--
Allocatable 判断负载对应的队列剩余容量是否足够, 与 enqueue 不同, enqueue 只会在 Dispatch 流程开始时检查队列的总剩余容量是否足够负载入队,
而 Allocatable 则要在有其他任务可能已入队并且占用了容量的情况下判断.
我们需要维护一个 Allocated Capacity 来帮助判断.
-->

**Allocatable** determines whether the remaining capacity of the queue corresponding to the load is sufficient.
Unlike `enqueue`, which only checks if the total remaining capacity of the queue is sufficient for the load to enqueue
at the start of the Dispatch process, **Allocatable** needs to make a judgment when other tasks may have already
enqueued and occupied capacity.

We need to maintain an **Allocated Capacity** to assist in this determination.

#### QueueOrder Func

<!--
默认将采用 Queue 的优先级来对 Queue 排序. 在相同或无优先级的情况则通过 Queue.share 来排序.
Queue 的优先级于 Volcano V1.10.0 正式加入.
-->

By default, queues will be sorted based on the **priority of the Queue**.
If they have the same priority or no priority, sorting will be done based on `Queue.share`.

Queue priority was officially introduced in `Volcano` V1.10.0.

#### Overused Func

<!--
Overused Func 将不会在这次被实现，这一版的 Proposal 目标是在 Capacity Plugin 上实现最基础的容量管理能力，如果 Overused 在
Capacity Plugin 中实现将会导致集群中的空闲资源无法被使用，他将会在之后被实现。

Overused Func 通过 Queue.share(已使用资源/队列总资源) 来优化调度任务的顺序，避免集群资源被单一队列占用导致其他队列没有足够的可用资源。
在这一版中我们会通过 Queue.Priority + Task.Priority 的方式轮询调度，可以改善前文提到的问题。
-->

The `Overused Func` will not be implemented in this release.
The goal of this version of the proposal is
to implement the most basic capacity management capabilities on the `Capacity Plugin`.
Implementing `Overused` in the `Capacity Plugin` would prevent the cluster's idle resources from being used,
and it will be implemented later.

The `Overused Func` optimizes the order of scheduling tasks through
`Queue.share` (used resources / total queue resources),
preventing cluster resources from being monopolized by a single queue
and leaving other queues without sufficient available resources.
In this version,
we will use a round-robin scheduling method
based on `Queue.Priority + Task.Priority` to address the issues mentioned previously.

### Dispatcher

<!--
目前 Dispatcher 的拓展性非常有限，为了支持 Capacity Plugin 提到的 Funcs，我们需要修改 Dispatcher 来支持，以及避免无法真正执行的
任务占用 Queue.allocated 导致可运行的任务无法入队。
-->

The current Dispatcher has very limited extensibility.
To support the `Funcs` mentioned in the `Capacity Plugin`, we need to modify the Dispatcher to provide support,
and to avoid the issue of tasks that cannot be executed occupying `Queue.allocated`,
which prevents tasks that are able to run from being enqueued.

#### Support the funcs of the Capacity plugin

<!--
Dispatcher 目前没有如 Allocatable/Enqueueable Func 的注册方法，为了支持 Capacity Plugin 实现容量管理的能力，我们需要修改
Dispatcher 的调度逻辑，通过这些拓展点来帮助 Dispatcher 更好的判断是否能够入队，以及容量是否足够。
-->

Currently, the Dispatcher lacks a registration method for functions such as `Allocatable/Enqueueable Func`.
To support the capacity management capabilities of the `Capacity Plugin`,
we need to modify the Dispatcher's scheduling logic.
These extension points will help the Dispatcher
better determine whether tasks can be enqueued and whether capacity is sufficient.

#### Condition Check

<!--
Dispatcher Cache 目前没有保存 Task.Status，在碎片资源过多导致任务无法正常下发的情况下我们无法正常处理，他将会持续占用队列容量，
我们需要在 Cache 中新增字段来保存 Status.Conditions 来避免该问题，帮助 Capacity Plugin 精确计算正常运行的任务所占用的容量。
-->

The **Dispatcher Cache** currently does not save `Task.Status`.
In situations where excessive fragmented resources prevent tasks from being properly distributed,
we are unable to handle this properly,
as it will continue to occupy queue capacity.
We need to add a field to the Cache to save `Status.Conditions` to avoid this issue,
helping the **Capacity Plugin** accurately calculate the capacity occupied by tasks that are running normally.

#### EventHandler

<!--
Dispatcher 目前不支持类似 Volcano Scheduler 中的 EventHandler能力，这导致任务在 Allocate/Deallocate 时，Capacity 插件无法
得知事件的发生，所以我们在这里需要一种类似的能力(Callback)来保证 Capacity 能够在一个任务下发后及时更新如 Queue.allocated。
-->

The **Dispatcher** currently does not support capabilities similar to the `EventHandler` found in the **Volcano Scheduler**.
This leads to the **Capacity Plugin** being unaware of events during task allocation and deallocation.
Therefore,
we need a similar capability (Callback)
to ensure that the Capacity can promptly update metrics like `Queue.allocated` after a task is dispatched.
