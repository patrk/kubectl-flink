# 4. Minimal typed resources and informers

Flink CRs carry large fields such as `lastReconciledSpec`, pod templates and managed fields. Decoding them into
unstructured maps and stripping fields afterwards costs most of the memory and time. Instead the tool defines Go structs
with only the fields it reads, registers them in a private scheme and lists and watches through a typed client. The
JSON decoder skips everything else without allocating it. The same structs back informers, so the cache holds only
what health and the list need.

Informers start with WatchList, which streams the initial state from the API server's watch cache instead of
building one large LIST response; client-go v0.37 enables it by default and falls back to LIST on servers without
it. One-shot lists are paginated (250 per page) and namespaces are fetched in parallel. LIST pages are decoded
with `encoding/json` straight into the list type: the codec scans the whole body once more to find the kind,
which doubles the decode time. Watch events go through the codec. Pods are selected with
`type=flink-native-kubernetes` and decoded into their own minimal type: phase, readiness, restarts, the last
waiting and termination reason, and requests.

Because nothing generated from the CRDs guards the hand-written structs, a test checks every field they read
against the CRD schema of each supported operator version (`make crds`), so an upstream rename fails the build
instead of decoding as empty. Events, HA ConfigMaps and logs are fetched per job
on demand. JobManager REST is never called for a list. Informer deltas are coalesced into a 250 ms redraw tick. Each
tick re-derives every job from the caches: for 1,500 jobs and 3,000 pods that takes under 1 ms
(`BenchmarkRederive`), less than tracking which jobs changed would cost in code.
