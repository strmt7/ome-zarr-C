#!/usr/bin/env python3
"""Report dependency-light GPU capability facts as JSON.

The probe is intentionally read-only and vendor-neutral. It checks device-node
facts, virtualization-adjacent hints, tool availability, and bounded output from
known read-only GPU inspection commands when those commands already exist.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import platform
import re
import shutil
import stat
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class CommandSpec:
    category: str
    name: str
    safe_args: tuple[str, ...] = ()


@dataclass(frozen=True)
class ProbeConfig:
    dev_root: Path = Path("/dev")
    proc_root: Path = Path("/proc")
    sys_root: Path = Path("/sys")
    host_root: Path = Path("/")
    environ: Mapping[str, str] | None = None
    path_env: str | None = None
    run_command_outputs: bool = True
    command_timeout_seconds: float = 4.0
    max_output_bytes: int = 20_000


CommandRunner = Callable[..., subprocess.CompletedProcess[str]]
WhichFunc = Callable[..., str | None]


DEFAULT_COMMAND_SPECS = (
    CommandSpec("ROCm", "rocminfo"),
    CommandSpec("ROCm", "rocm_agent_enumerator"),
    CommandSpec("ROCm", "rocm-smi", ("--showproductname", "--showdriverversion")),
    CommandSpec("ROCm", "amd-smi", ("static", "--asic", "--driver")),
    CommandSpec("HIP", "hipconfig", ("--platform",)),
    CommandSpec("HIP", "hipcc", ("--version",)),
    CommandSpec("OpenCL", "clinfo"),
    CommandSpec("Vulkan", "vulkaninfo", ("--summary",)),
)

DRM_DEVICE_SYSFS_FIELDS = (
    "vendor",
    "device",
    "subsystem_vendor",
    "subsystem_device",
    "revision",
    "class",
)
DRM_ENTRY_SYSFS_FIELDS = ("dev", "status")
OPENCL_SUMMARY_FIELDS = (
    "Number of platforms",
    "Platform Name",
    "Device Name",
    "Device Type",
)
VULKAN_DEVICE_FIELDS = {
    "apiVersion": "api_version",
    "driverVersion": "driver_version",
    "vendorID": "vendor_id",
    "deviceID": "device_id",
    "deviceType": "device_type",
    "deviceName": "device_name",
}
VULKAN_SOFTWARE_RENDERER_TERMS = (
    "lavapipe",
    "llvmpipe",
    "softpipe",
    "software rasterizer",
    "swiftshader",
)
ALIGNED_FIELD_RE = re.compile(
    r"^\s*(?P<label>[A-Za-z0-9#][A-Za-z0-9# /()._-]*?)\s{2,}(?P<value>\S.*)$"
)
VULKAN_GPU_RE = re.compile(r"^\s*GPU(?P<index>\d+):\s*$")
VULKAN_ASSIGNMENT_RE = re.compile(
    r"^\s*(?P<key>[A-Za-z][A-Za-z0-9_]*)\s*=\s*(?P<value>.+?)\s*$"
)
VIRTUALIZATION_TERMS = (
    "containerd",
    "docker",
    "hyper-v",
    "hyperv",
    "kubepods",
    "kvm",
    "lxc",
    "microsoft",
    "podman",
    "qemu",
    "virtualbox",
    "vmware",
    "wsl",
    "xen",
)
VIRTUALIZATION_DMI_FIELDS = (
    "sys_vendor",
    "product_name",
    "product_version",
    "bios_vendor",
    "board_vendor",
)
ENVIRONMENT_HINTS: Mapping[str, str] = {
    "container": "value",
    "KUBERNETES_SERVICE_HOST": "presence",
    "SYSTEMD_CONTAINER": "value",
    "WSL_DISTRO_NAME": "value",
    "WSL_INTEROP": "presence",
}
CONTAINER_MARKER_PATHS = (Path(".dockerenv"), Path("run/.containerenv"))


def _truncate_text(text: str | bytes | None, max_bytes: int) -> tuple[str, bool, int]:
    if text is None:
        return "", False, 0
    if isinstance(text, bytes):
        raw = text
    else:
        raw = text.encode("utf-8", errors="replace")
    if len(raw) <= max_bytes:
        return raw.decode("utf-8", errors="replace"), False, len(raw)
    return raw[:max_bytes].decode("utf-8", errors="replace"), True, len(raw)


def _read_text(path: Path, *, max_bytes: int = 4096) -> dict[str, Any]:
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return {"path": str(path), "available": False}
    except PermissionError as exc:
        return {
            "path": str(path),
            "available": True,
            "readable": False,
            "error": type(exc).__name__,
        }
    except OSError as exc:
        return {
            "path": str(path),
            "available": path.exists(),
            "readable": False,
            "error": type(exc).__name__,
            "message": str(exc),
        }

    value, truncated, byte_count = _truncate_text(raw, max_bytes)
    return {
        "path": str(path),
        "available": True,
        "readable": True,
        "value": value,
        "truncated": truncated,
        "bytes": byte_count,
    }


def _read_first_line(path: Path, *, max_bytes: int = 512) -> dict[str, Any]:
    fact = _read_text(path, max_bytes=max_bytes)
    if fact.get("readable"):
        value = str(fact["value"]).splitlines()[0] if fact["value"] else ""
        fact["value"] = value
    return fact


def _first_int(value: str) -> int | None:
    match = re.search(r"\d+", value)
    if match is None:
        return None
    return int(match.group(0))


def _unique_preserving_order(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    unique: list[str] = []
    for value in values:
        stripped = value.strip()
        if stripped and stripped not in seen:
            seen.add(stripped)
            unique.append(stripped)
    return unique


def _extract_aligned_fields(
    text: str,
    labels: Sequence[str],
) -> dict[str, list[str]]:
    values: dict[str, list[str]] = {label: [] for label in labels}
    wanted = set(labels)
    for line in text.splitlines():
        match = ALIGNED_FIELD_RE.match(line.rstrip())
        if match is None:
            continue
        label = " ".join(match.group("label").split())
        if label in wanted:
            values[label].append(match.group("value").strip())
    return {
        label: _unique_preserving_order(field_values)
        for label, field_values in values.items()
        if field_values
    }


def _observed_terms(text: str) -> list[str]:
    lowered = text.lower()
    return [term for term in VIRTUALIZATION_TERMS if term in lowered]


def _path_kind(mode: int) -> str:
    if stat.S_ISCHR(mode):
        return "character_device"
    if stat.S_ISBLK(mode):
        return "block_device"
    if stat.S_ISDIR(mode):
        return "directory"
    if stat.S_ISREG(mode):
        return "regular_file"
    if stat.S_ISLNK(mode):
        return "symlink"
    if stat.S_ISFIFO(mode):
        return "fifo"
    if stat.S_ISSOCK(mode):
        return "socket"
    return "other"


def path_fact(path: Path) -> dict[str, Any]:
    fact: dict[str, Any] = {"path": str(path), "exists": False}
    try:
        info = path.lstat()
    except FileNotFoundError:
        return fact
    except OSError as exc:
        fact.update(
            {
                "exists": path.exists(),
                "error": type(exc).__name__,
                "message": str(exc),
            }
        )
        return fact

    mode = info.st_mode
    fact.update(
        {
            "exists": True,
            "kind": _path_kind(mode),
            "mode": oct(stat.S_IMODE(mode)),
            "uid": info.st_uid,
            "gid": info.st_gid,
            "readable_by_current_user": os.access(path, os.R_OK),
            "writable_by_current_user": os.access(path, os.W_OK),
        }
    )
    if stat.S_ISCHR(mode) or stat.S_ISBLK(mode):
        fact["device_major"] = os.major(info.st_rdev)
        fact["device_minor"] = os.minor(info.st_rdev)
    if stat.S_ISLNK(mode):
        try:
            fact["symlink_target"] = os.readlink(path)
        except OSError as exc:
            fact["symlink_error"] = type(exc).__name__
    return fact


def _drm_sysfs_fact(entry_name: str, sys_root: Path) -> dict[str, Any]:
    drm_root = sys_root / "class" / "drm" / entry_name
    fact = path_fact(drm_root)
    for field in DRM_ENTRY_SYSFS_FIELDS:
        fact[field] = _read_first_line(drm_root / field)
    device_root = drm_root / "device"
    device: dict[str, Any] = {"path": str(device_root)}
    for field in DRM_DEVICE_SYSFS_FIELDS:
        device[field] = _read_first_line(device_root / field)
    device["uevent"] = _read_text(device_root / "uevent", max_bytes=4096)
    device["driver"] = path_fact(device_root / "driver")
    fact["device"] = device
    return fact


def probe_devices(
    dev_root: Path = Path("/dev"),
    sys_root: Path = Path("/sys"),
) -> dict[str, Any]:
    dri_path = dev_root / "dri"
    dri_fact = path_fact(dri_path)
    entries: list[dict[str, Any]] = []
    if dri_fact.get("kind") == "directory":
        try:
            for path in sorted(dri_path.iterdir()):
                entry = path_fact(path)
                entry["drm_sysfs"] = _drm_sysfs_fact(path.name, sys_root)
                entries.append(entry)
        except OSError as exc:
            dri_fact["entry_error"] = type(exc).__name__
            dri_fact["entry_error_message"] = str(exc)
    dri_fact["entries"] = entries
    return {
        "kfd": path_fact(dev_root / "kfd"),
        "dxg": path_fact(dev_root / "dxg"),
        "dri": dri_fact,
    }


def _cpuinfo_fact(proc_root: Path) -> dict[str, Any]:
    fact = _read_text(proc_root / "cpuinfo", max_bytes=256_000)
    if not fact.get("readable"):
        return fact
    text = str(fact.pop("value"))
    fact["hypervisor_cpu_flag_present"] = any(
        "hypervisor" in line.split(":", maxsplit=1)[-1].split()
        for line in text.splitlines()
        if line.lower().startswith("flags")
    )
    fact["observed_terms"] = _observed_terms(text)
    return fact


def _cgroup_fact(proc_root: Path) -> dict[str, Any]:
    fact = _read_text(proc_root / "1" / "cgroup", max_bytes=128_000)
    if not fact.get("readable"):
        return fact
    text = str(fact.pop("value"))
    fact["line_count"] = len(text.splitlines())
    fact["observed_terms"] = _observed_terms(text)
    return fact


def _environment_facts(environ: Mapping[str, str]) -> list[dict[str, Any]]:
    facts: list[dict[str, Any]] = []
    for name, reporting in ENVIRONMENT_HINTS.items():
        present = name in environ
        fact: dict[str, Any] = {"name": name, "present": present}
        if present and reporting == "value":
            value, truncated, byte_count = _truncate_text(environ[name], 512)
            fact.update({"value": value, "truncated": truncated, "bytes": byte_count})
        facts.append(fact)
    return facts


def probe_virtualization(config: ProbeConfig) -> dict[str, Any]:
    osrelease = _read_first_line(config.proc_root / "sys" / "kernel" / "osrelease")
    if osrelease.get("readable"):
        osrelease["observed_terms"] = _observed_terms(str(osrelease["value"]))

    proc_version = _read_first_line(config.proc_root / "version")
    if proc_version.get("readable"):
        proc_version["observed_terms"] = _observed_terms(str(proc_version["value"]))

    dmi_facts = []
    dmi_root = config.sys_root / "class" / "dmi" / "id"
    for field in VIRTUALIZATION_DMI_FIELDS:
        fact = _read_first_line(dmi_root / field)
        fact["name"] = field
        if fact.get("readable"):
            fact["observed_terms"] = _observed_terms(str(fact["value"]))
        dmi_facts.append(fact)

    marker_facts = [
        path_fact(config.host_root / marker_path)
        for marker_path in CONTAINER_MARKER_PATHS
    ]
    environ = config.environ if config.environ is not None else os.environ
    return {
        "kernel": {
            "osrelease": osrelease,
            "version": proc_version,
        },
        "cpu": _cpuinfo_fact(config.proc_root),
        "dmi": dmi_facts,
        "container": {
            "marker_paths": marker_facts,
            "cgroup": _cgroup_fact(config.proc_root),
        },
        "environment": _environment_facts(environ),
    }


def _summarize_opencl_output(stdout: str, source_truncated: bool) -> dict[str, Any]:
    fields = _extract_aligned_fields(stdout, OPENCL_SUMMARY_FIELDS)
    summary: dict[str, Any] = {"source": "stdout", "source_truncated": source_truncated}

    platform_counts = fields.get("Number of platforms", [])
    if platform_counts:
        platform_count = _first_int(platform_counts[0])
        if platform_count is not None:
            summary["platform_count"] = platform_count
        else:
            summary["platform_count_raw"] = platform_counts[0]
    if platform_names := fields.get("Platform Name"):
        summary["platform_names"] = platform_names
    if device_names := fields.get("Device Name"):
        summary["device_names"] = device_names
    if device_types := fields.get("Device Type"):
        summary["device_types"] = device_types

    return summary if len(summary) > 2 else {}


def _vulkan_software_indicators(device: Mapping[str, Any]) -> list[str]:
    haystack = " ".join(
        str(device.get(field, "")) for field in ("device_name", "device_type")
    ).lower()
    indicators = [term for term in VULKAN_SOFTWARE_RENDERER_TERMS if term in haystack]
    if "physical_device_type_cpu" in haystack:
        indicators.append("cpu_device_type")
    return _unique_preserving_order(indicators)


def _summarize_vulkan_output(stdout: str, source_truncated: bool) -> dict[str, Any]:
    summary: dict[str, Any] = {"source": "stdout", "source_truncated": source_truncated}
    devices: list[dict[str, Any]] = []
    current_device: dict[str, Any] | None = None

    for line in stdout.splitlines():
        stripped = line.strip()
        if stripped.startswith("Vulkan Instance Version:"):
            summary["instance_version"] = stripped.partition(":")[2].strip()
            continue

        gpu_match = VULKAN_GPU_RE.match(line)
        if gpu_match is not None:
            current_device = {"index": int(gpu_match.group("index"))}
            devices.append(current_device)
            continue

        if current_device is None:
            continue
        assignment_match = VULKAN_ASSIGNMENT_RE.match(line)
        if assignment_match is None:
            continue
        key = assignment_match.group("key")
        if key in VULKAN_DEVICE_FIELDS:
            current_device[VULKAN_DEVICE_FIELDS[key]] = assignment_match.group(
                "value"
            ).strip()

    for device in devices:
        if indicators := _vulkan_software_indicators(device):
            device["software_renderer_indicators"] = indicators
    if devices:
        summary["physical_devices"] = devices

    return summary if len(summary) > 2 else {}


def _summarize_command_output(
    spec: CommandSpec,
    output: Mapping[str, Any],
) -> dict[str, Any]:
    if not output.get("executed"):
        return {}
    stdout = output.get("stdout")
    if not isinstance(stdout, str) or not stdout:
        return {}

    source_truncated = bool(output.get("stdout_truncated"))
    if spec.name == "clinfo":
        return _summarize_opencl_output(stdout, source_truncated)
    if spec.name == "vulkaninfo":
        return _summarize_vulkan_output(stdout, source_truncated)
    return {}


def _run_safe_command(
    command: Sequence[str],
    config: ProbeConfig,
    runner: CommandRunner,
) -> dict[str, Any]:
    if (
        not math.isfinite(config.command_timeout_seconds)
        or config.command_timeout_seconds <= 0
    ):
        return {
            "executed": False,
            "command": list(command),
            "reason": "invalid_timeout",
            "timeout_seconds": config.command_timeout_seconds,
        }

    try:
        completed = runner(
            list(command),
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=config.command_timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        stdout, stdout_truncated, stdout_bytes = _truncate_text(
            exc.stdout, config.max_output_bytes
        )
        stderr, stderr_truncated, stderr_bytes = _truncate_text(
            exc.stderr, config.max_output_bytes
        )
        return {
            "executed": True,
            "command": list(command),
            "timed_out": True,
            "timeout_seconds": config.command_timeout_seconds,
            "stdout": stdout,
            "stderr": stderr,
            "stdout_truncated": stdout_truncated,
            "stderr_truncated": stderr_truncated,
            "stdout_bytes": stdout_bytes,
            "stderr_bytes": stderr_bytes,
        }
    except OSError as exc:
        return {
            "executed": True,
            "command": list(command),
            "timed_out": False,
            "error": type(exc).__name__,
            "message": str(exc),
        }

    stdout, stdout_truncated, stdout_bytes = _truncate_text(
        completed.stdout, config.max_output_bytes
    )
    stderr, stderr_truncated, stderr_bytes = _truncate_text(
        completed.stderr, config.max_output_bytes
    )
    return {
        "executed": True,
        "command": list(command),
        "timed_out": False,
        "returncode": completed.returncode,
        "stdout": stdout,
        "stderr": stderr,
        "stdout_truncated": stdout_truncated,
        "stderr_truncated": stderr_truncated,
        "stdout_bytes": stdout_bytes,
        "stderr_bytes": stderr_bytes,
    }


def probe_commands(
    config: ProbeConfig,
    command_specs: Sequence[CommandSpec] = DEFAULT_COMMAND_SPECS,
    *,
    which: WhichFunc = shutil.which,
    runner: CommandRunner = subprocess.run,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for spec in command_specs:
        command_path = which(spec.name, path=config.path_env)
        result: dict[str, Any] = {
            "category": spec.category,
            "name": spec.name,
            "available": command_path is not None,
            "path": command_path,
            "probe_args": list(spec.safe_args),
        }
        if command_path is None:
            result["output"] = {"executed": False, "reason": "not_available"}
        elif not config.run_command_outputs:
            result["output"] = {"executed": False, "reason": "disabled"}
        else:
            result["output"] = _run_safe_command(
                [command_path, *spec.safe_args], config, runner
            )
            if summary := _summarize_command_output(spec, result["output"]):
                result["output"]["summary"] = summary
        results.append(result)
    return results


def platform_facts() -> dict[str, str]:
    return {
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "python_version": platform.python_version(),
    }


def collect_probe(
    config: ProbeConfig | None = None,
    *,
    command_specs: Sequence[CommandSpec] = DEFAULT_COMMAND_SPECS,
    which: WhichFunc = shutil.which,
    runner: CommandRunner = subprocess.run,
) -> dict[str, Any]:
    config = config or ProbeConfig()
    return {
        "schema_version": 1,
        "platform": platform_facts(),
        "virtualization_hints": probe_virtualization(config),
        "devices": probe_devices(config.dev_root, config.sys_root),
        "commands": probe_commands(
            config, command_specs=command_specs, which=which, runner=runner
        ),
    }


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect read-only GPU capability facts as JSON."
    )
    parser.add_argument("--dev-root", type=Path, default=Path("/dev"))
    parser.add_argument("--proc-root", type=Path, default=Path("/proc"))
    parser.add_argument("--sys-root", type=Path, default=Path("/sys"))
    parser.add_argument("--host-root", type=Path, default=Path("/"))
    parser.add_argument(
        "--skip-command-output",
        action="store_true",
        help="report command availability without executing probe commands",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=4.0,
        help="seconds to wait for each command-output probe",
    )
    parser.add_argument(
        "--max-output-bytes",
        type=int,
        default=20_000,
        help="maximum bytes to retain per command stdout/stderr stream",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="pretty-print JSON output",
    )
    args = parser.parse_args(argv)
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("--timeout must be a finite number greater than zero")
    if args.max_output_bytes <= 0:
        parser.error("--max-output-bytes must be greater than zero")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    config = ProbeConfig(
        dev_root=args.dev_root,
        proc_root=args.proc_root,
        sys_root=args.sys_root,
        host_root=args.host_root,
        run_command_outputs=not args.skip_command_output,
        command_timeout_seconds=args.timeout,
        max_output_bytes=args.max_output_bytes,
    )
    payload = collect_probe(config)
    json.dump(payload, sys.stdout, indent=2 if args.pretty else None, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
