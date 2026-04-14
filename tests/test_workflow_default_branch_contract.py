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

    result = subprocess.run(
        ["git", "-C", str(_repo_root()), "remote", "show", "origin"],
        check=True,
        capture_output=True,
        text=True,
    )
    marker = "HEAD branch:"
    for line in result.stdout.splitlines():
        if marker in line:
            branch = line.split(marker, maxsplit=1)[1].strip()
            if branch:
                return branch
    raise AssertionError("Could not detect the default branch from origin metadata.")


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
