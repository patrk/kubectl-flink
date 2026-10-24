# 5. Scopes and multiple contexts

Where a user can look differs per site: one jumphost may reach pre-production and production of a cluster, another
only development. Users also often cannot list Flink resources at cluster scope. The profile therefore defines scopes,
each a context with a list of namespaces, and the default is the current context and namespace.

`ls` and `top` load all scopes in parallel with one informer set per context and add a CONTEXT column when there is
more than one. A scope that fails to connect or is forbidden becomes an error row; the others keep working. `-A` tries
cluster scope and falls back to the scope's namespaces, then to the context namespace, saying so in one line.
