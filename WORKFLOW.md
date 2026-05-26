---
schema: conductor/repository-workflow-policy/1
execution:
  canonicalize_commands: []
  verify_commands: []
  max_attempts: 3
  retry_backoff_seconds: 60
  command_timeout_seconds: 1800
context:
  read_first:
    - README.md
    - docs/policy.md
landing:
  default_merge_method: squash
  allowed_merge_methods:
    - merge
    - squash
---

Use this repository policy as the working contract for Conductor-owned lanes in Datalens.

Datalens should use English for agent communication, issue comments and records, pull
requests, and commit messages. This repository-level policy intentionally overrides the
global pilot configuration, which may use another language for other repositories.

Conductor is the scheduler, runner, and state owner. The managed agent is responsible for
one leased issue at a time and must work inside the lane that Conductor assigns. Do not
start unrelated work, widen the issue scope, or manually invent lifecycle transitions that
are not represented by Conductor state, tracker records, or PR state.

The repository is currently lightweight and does not define a root task runner yet. The
normal gate is therefore empty. When project-native commands are introduced, add them to
`execution.canonicalize_commands` and `execution.verify_commands` here before relying on
them in managed lanes.

The normal implementation path is:

1. Read the leased issue, the current lane state, this workflow policy, and the files
   needed to understand the requested change.
2. Make the smallest coherent code, test, and documentation changes required by the
   issue.
3. Use repository-native commands for formatting, validation, and tests once they exist.
4. Let Conductor create the `conductor/commit/1` commit record after a successful attempt;
   do not hand-write lifecycle state into commit messages.
5. Push the lane branch and create or update the PR only through the configured landing
   capability.
6. Leave issue progress, terminal records, review handoff, repair completion, and closeout
   through the declared Conductor tracker tools.

Treat `context.read_first` as required startup context. Conductor may inline small
repository-relative files in the first turn; larger files are listed with path, size, and
hash and must be read from the assigned worktree before editing. This is an execution
contract, not a separate read-receipt workflow.

Tracker writeback is part of the execution contract. Use only the declared Conductor
tracker tools for issue transitions, comments, labels, progress records, attempt results,
terminal records, review checkpoints, review handoff, repair completion, and closeout.
Tool calls must stay scoped to the currently leased issue and current attempt. Before any
ordinary terminal success, write `issue_attempt_result` with schema
`conductor/attempt-result/1`, then write and finalize the terminal record. Do not write ad
hoc lifecycle state into ordinary comments when a structured tracker record exists.

Managed-agent ordinary issue comments, human-readable tracker record prose, PR content,
and commit-message prose must be written in English for this repository.

When declared by the managed-agent runtime, use `conductor_issue_provider.issue_get` to
fetch referenced issue context and `conductor_issue_provider.issue_comments_list` to
refresh comments. These issue-provider tools are for context reads; lifecycle writeback
still goes through the Conductor tracker tools above.

Treat `In Review` as a PR-backed handoff state. A normal success path must have a pushed
lane branch, a non-draft PR, a commit record, and a tracker handoff record before the issue
is considered ready for review. Draft PRs are acceptable only when the issue or runtime
prompt explicitly requests one.

Keep secrets out of every durable surface: issue comments, PR bodies, commit messages,
logs, state files, snapshots, metrics labels, and test fixtures. Use environment variables
or configured secret references for provider tokens.

Keep branch, lane, and worktree behavior aligned with the repository policy and state
store. Do not reuse another issue's lane, do not delete retained work manually to hide a
failed attempt, and do not continue when ownership signals disagree. If Conductor cannot
prove the retained lane, tracker issue, branch, PR, and attempt lineage still refer to the
same work, mark the lane for manual attention through the tracker tools instead of
guessing.
