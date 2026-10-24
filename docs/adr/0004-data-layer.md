# 4. Minimal typed resources and informers

Flink CRs carry large fields such as `lastReconciledSpec`, pod templates and managed fields. Decoding them into
unstructured maps and stripping fields afterwards costs most of the memory and time. Instead the tool defines Go structs
with only the fields it reads, registers them in a private scheme and lists and watches through a typed client. The
JSON decoder skips everything else without allocating it. The same structs back informers, so the cache holds only
what health and the list need.

Informers start with WatchList, which streams the initial state from the API server's watch cache instead of building one large LIST response; client-go v0.37 enables it by default and falls back to LIST on servers without it. One-shot lists are paginated and namespaces are fetched in parallel. Pods are
watched with the Flink component labels and reduced to a few fields. Events, HA ConfigMaps and logs are fetched per job
on demand. JobManager REST is never called for a list. Informer deltas are coalesced into a 250 ms redraw tick, and
only changed jobs are re-derived.
