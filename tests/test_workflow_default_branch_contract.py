from pathlib import Path

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


def test_workflow_instructions_document_the_default_branch_job_gate() -> None:
    instructions = _read_repo_file(".github/instructions/workflows.instructions.md")
    assert "default branch" in instructions.lower()
    assert DEFAULT_BRANCH_GUARD in instructions
