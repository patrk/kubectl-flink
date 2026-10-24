# 4. Minimal typed resources and informers

Flink CRs carry large fields such as `lastReconciledSpec`, pod templates and managed fields. Decoding them into
unstructured maps and stripping fields afterwards costs most of the memory and time. Instead the tool defines Go structs
with only the fields it reads, registers them in a private scheme and lists and watches through a typed client. The
JSON decoder skips everything else without allocating it. The same structs back informers, so the cache holds only
what health and the list need.

Lists are paginated and namespaces are fetched in parallel; WatchList is used when the server supports it. Pods are
watched with the Flink component labels and reduced to a few fields. Events, HA ConfigMaps and logs are fetched per job
on demand. JobManager REST is never called for a list. Informer deltas are coalesced into a 250 ms redraw tick, and
only changed jobs are re-derived.
