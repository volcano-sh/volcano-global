# Topology feasibility fixtures

`fixtures/cases.json` gives two member clusters the same four node names but
different tier-1 layouts. Each worker has one available slot. The fragmented
member has two leaves with two slots each; the feasible member has one leaf
with four slots. The four-Pod Job needs one slot per Pod.

Run the hand-calculated fixture checks with:

```sh
go test ./test/topologyfeasibility
```

T02 is the counterexample: aggregate capacity is four, but no allowed tier-1
domain holds the complete gang. T03 and T04 are paired positive controls. T05
checks that soft mode does not turn the preference into a hard rejection. T07
removes one eligible node to isolate ordinary capacity failure. The identical
node names across members require callers to include cluster identity in their
keys and reports.

These tests only validate the fixtures and their simple oracle. They do not
call an estimator or observe a member Volcano scheduler. The estimator work for
[#32](https://github.com/volcano-sh/volcano-global/issues/32) has no callable
entry point on the pinned main revision. Once its implementation is available,
the follow-up adapter must record the exact implementation SHA, raw per-member
response, fallback/error, selected cluster, and actual four-Pod placement. A
real member test must use actual node names, available CPU requests, native
HyperNodes, and a Volcano Job with `minAvailable: 4`; it must confirm the
scheduler and topology configuration before calling a result a pass.
