# Workflow Instructions

Critical: do not spawn, delegate to, or coordinate with multiple AI agents,
subagents, or separate agent sessions. Work in one session only unless the user
explicitly revokes this rule in a later instruction.

- Keep CI simple and directly tied to the current repo contract.
- Protect the frozen snapshot from direct edits.
- Keep the existing push, pull request, and manual triggers, but make every
  push and pull-request workflow follow the same branch-gating pattern used in
  `ZMB-UZH/omero-docker-extended`:
  1. `on.push.branches` and `on.pull_request.branches` must both be pinned to
     the repository's current default branch name.
  2. every job must also keep
     `github.ref_name == github.event.repository.default_branch`.
- GitHub allows contexts and expressions in `jobs.<job_id>.if`, but branch
  filters accept branch-name patterns, so the runtime default-branch check must
  stay in the job guard while the trigger branch remains a literal branch name.
- All workflows are included in this rule set with no exceptions.
- If the repository default branch changes, update all workflow branch filters
  in the same change and rerun the workflow contract test before pushing.
- Use official stable GitHub Actions and current stable tool versions unless the
  repo deliberately pins otherwise.
- Prefer narrow, trustworthy workflows over broad but noisy ones.
- Once multiple ported surfaces exist, the tests workflow must exercise the full
  parity suite, not just an older subset.
- Keep frozen snapshot directories matching `source_code_v*/` excluded from
  security scanning. If a new frozen snapshot is added, update the exclusion in
  the scanner config as part of the same change.
- Never infer remote branch state from local Git alone. Verify the GitHub repo
  page or API before stating that a branch exists, is deleted, or is the
  default branch.
- For GitHub Actions inspection, prefer explicit repo scoping such as
  `gh ... -R strmt7/ome-zarr-C` so local CLI context cannot drift to upstream.
- When an AI agent pushes to the repository, it must wait for all workflows on
  that exact commit to complete and address failures before calling the work
  done.
- Treat the exact check-run set for the pushed commit as the source of truth.
  The combined commit status endpoint can lag after checks are already green.
- Before changing workflow action versions or workflow-installed tool versions,
  verify the current stable release from the official source instead of
  guessing from old examples or cached docs.
