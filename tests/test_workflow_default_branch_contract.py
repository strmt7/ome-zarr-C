import os
import subprocess
from pathlib import Path

import yaml

DEFAULT_BRANCH_GUARD = "github.ref_name == github.event.repository.default_branch"
WORKFLOW_FILES = [
    ".github/workflows/native-cpp-guard.yml",
    ".github/workflows/protect-frozen-source-snapshot.yml",
    ".github/workflows/ruff.yml",
    ".github/workflows/security-code-scanning.yml",
    ".github/workflows/tests.yml",
]


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _read_repo_file(relative_path: str) -> str:
    return (_repo_root() / relative_path).read_text(encoding="utf-8")


def _read_workflow_yaml(relative_path: str) -> dict[str, object]:
    return yaml.load(_read_repo_file(relative_path), Loader=yaml.BaseLoader)


def _detect_default_branch() -> str:
    env_value = os.environ.get("WORKFLOW_DEFAULT_BRANCH", "").strip()
    if env_value:
        return env_value

    repo_root = str(_repo_root())

    symbolic_head = subprocess.run(
        ["git", "-C", repo_root, "symbolic-ref", "--short", "refs/remotes/origin/HEAD"],
        check=False,
        capture_output=True,
        text=True,
    )
    if symbolic_head.returncode == 0:
        _, _, branch = symbolic_head.stdout.strip().partition("/")
        if branch:
            return branch

    configured_merges = subprocess.run(
        ["git", "-C", repo_root, "config", "--get-regexp", r"^branch\..*\.merge$"],
        check=False,
        capture_output=True,
        text=True,
    )
    if configured_merges.returncode == 0:
        merge_branches = {
            value.rsplit("/", maxsplit=1)[-1]
            for line in configured_merges.stdout.splitlines()
            if (parts := line.split(maxsplit=1)) and len(parts) == 2
            for value in [parts[1].strip()]
            if value.startswith("refs/heads/")
        }
        if len(merge_branches) == 1:
            return next(iter(merge_branches))

    raise AssertionError(
        "Could not detect the default branch from local Git metadata. "
        "Set WORKFLOW_DEFAULT_BRANCH explicitly if the local checkout does not "
        "cache origin/HEAD or a unique branch.<name>.merge mapping."
    )


def _event_branches(relative_path: str, event_name: str) -> list[str]:
    workflow = _read_workflow_yaml(relative_path)
    on_config = workflow["on"]
    event_config = on_config[event_name]
    return event_config["branches"]


def test_all_repo_workflows_are_enumerated() -> None:
    actual_workflows = sorted(
        str(path.relative_to(_repo_root()))
        for path in (_repo_root() / ".github" / "workflows").glob("*.yml")
    )
    assert actual_workflows == sorted(WORKFLOW_FILES)


def test_all_workflow_jobs_gate_runner_execution_to_default_branch() -> None:
    for workflow_path in WORKFLOW_FILES:
        workflow_text = _read_repo_file(workflow_path)
        assert DEFAULT_BRANCH_GUARD in workflow_text, (
            f"{workflow_path} must gate job execution to the default branch."
        )


def test_all_workflows_pin_push_and_pull_request_to_the_detected_default_branch() -> (
    None
):
    default_branch = _detect_default_branch()
    for workflow_path in WORKFLOW_FILES:
        assert _event_branches(workflow_path, "push") == [default_branch], (
            f"{workflow_path} push trigger must be pinned to {default_branch!r}."
        )
        assert _event_branches(workflow_path, "pull_request") == [default_branch], (
            f"{workflow_path} pull_request trigger must be pinned to "
            f"{default_branch!r}."
        )


def test_workflow_instructions_document_the_default_branch_job_gate() -> None:
    instructions = _read_repo_file(".github/instructions/workflows.instructions.md")
    assert "default branch" in instructions.lower()
    assert DEFAULT_BRANCH_GUARD in instructions


def test_tests_workflow_runs_full_pytest_discovery() -> None:
    workflow_text = _read_repo_file(".github/workflows/tests.yml")
    assert "python -m pytest -q" in workflow_text
    assert "tests/test_axes_equivalence.py" not in workflow_text
