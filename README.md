# Volcano Global

Volcano-global is a multiple kubernetes clusters scheduler, designed to efficiently manage and schedule AI/ML,
Bigdata workloads across multiple kubernetes clusters,
which cleverly combines the capabilities of [Karmada](https://karmada.io/) and [Volcano](https://volcano.sh/en/)

This project originated from:
- [LFX Mentorship CNCF - Volcano: Volcano supports multi-cluster AI workloads scheduling](https://mentorship.lfx.linuxfoundation.org/project/132a4971-6969-4ca6-a695-783ece3ac768)
- [OSPP 2024 Volcano supports queue capacity management capabilities in multi-cluster AI workload scheduling](https://summer-ospp.ac.cn/org/prodetail/243ba0505?list=org&navpage=org)

> This is an alpha version and code is subject to change.
> 
# Quick start

You can read [deploy guide](docs/deploy/README.md).

# How does it run?

![volcano global design](docs/imgs/volcano_global_design.svg)

# Release Guide

Volcano-global is a seperated subproject, having its own release. The project plays well with [volcano repo](https://github.com/volcano-sh/volcano), however, the release are not coupled. We will release new versions as needed.
