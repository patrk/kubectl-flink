# 11. The report bundle and its redaction

`report` hands a problem to someone else: `summary.md`, printed and kept in the bundle, says what is wrong as far as
the known patterns tell, what else was seen, what happened recently and what could not be collected; the bundle holds
the evidence behind it. It is built from the API server's and the JobManager's own answers, not from the tool's
minimal types, so nothing support needs is dropped: the resource without kubectl's last-applied copy, the pods, the
events of the resource and its pods, the Flink configuration the operator renders, JobManager logs and those of the
TaskManagers in trouble first, the REST answers, and `why` and `history` as text. Tails and the number of TaskManagers
keep it small enough to share; a `.tar.gz` is written reproducibly, a directory on request.

Every file is redacted before it is written, and the summary counts what was masked per file. Values are masked, keys
and structure stay, so a reader sees that a password was set. A value is secret when its key names a password,
token, key or credential, in JSON at any depth, in `{name|key, value}` pairs such as env vars and Flink's REST
configuration, in argument lists (`--password x` and `--password=x`), in `key: value` and `key=value` lines of
configuration files and logs, and inside JSON held as a string, such as the operator's copy of the last reconciled
spec. Free text is also searched for JAAS passwords, credentials in URLs, Bearer and Basic headers, JSON web tokens
and AWS key ids. References to secrets, such as a Secret's name or a key selector, stay readable: which Secret is
missing is often the evidence. A profile adds key and text patterns for secrets only its platform has.

A test plants a marker in each of these places in a captured scenario, builds the bundle through the command and
fails on any marker it finds, naming the place that leaked.
