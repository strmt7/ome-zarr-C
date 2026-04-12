# Workflow Instructions

- Keep CI simple and directly tied to the current repo contract.
- Protect the frozen snapshot from direct edits.
- Run parity tests and lint checks on every push and pull request.
- Use official stable GitHub Actions and current stable tool versions unless the
  repo deliberately pins otherwise.
- Prefer narrow, trustworthy workflows over broad but noisy ones.
- Never infer remote branch state from local Git alone. Verify the GitHub repo
  page or API before stating that a branch exists, is deleted, or is the
  default branch.
- When an AI agent pushes to the repository, it must wait for all workflows on
  that exact commit to complete and address failures before calling the work
  done.
