# 8. Write operations: policy, owners, no automation

Jobs are often owned by something else: a GitOps controller, or a service that creates CRs and keeps its own record
of job state. Patching such a job directly can be reverted or leave the owner out of sync. The tool reads managedFields
and profile rules to name the owner, warns before writing, and shows the owner's own command when the profile knows
it. Each scope and operation has a policy of allow, confirm or block; confirming on production asks the user to type the
namespace and job name, because pre-production and production can share a context.

Every write prints its patch, supports client and server dry runs, uses the field manager `kubectl-flink` and can wait
on the operator's lifecycle state. Bulk operations take selectors on CR labels, name patterns or pod-template labels,
run in batches, stop after a set number of failures and never offer `resume --all`. `restart` writes an epoch
millisecond nonce, which other controllers may read as a timestamp. The tool never restarts a job on its own.
