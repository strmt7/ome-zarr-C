from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Outcome:
    status: str
    value: Any = None
    error_type: type[BaseException] | None = None
    error_message: str | None = None
    payload: Any = None
    stdout: str = ""
    records: Any = None
    tree: Any = None
    browser_calls: Any = None
    test_calls: Any = None


def ok(**kwargs) -> Outcome:
    return Outcome(status="ok", **kwargs)


def err(exc: BaseException, **kwargs) -> Outcome:
    return Outcome(
        status="err",
        error_type=type(exc),
        error_message=str(exc),
        **kwargs,
    )
