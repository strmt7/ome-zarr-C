from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "gpu_capability_probe.py"
SPEC = importlib.util.spec_from_file_location("gpu_capability_probe", MODULE_PATH)
assert SPEC is not None
gpu_probe = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = gpu_probe
assert SPEC.loader is not None
SPEC.loader.exec_module(gpu_probe)


def test_probe_devices_reports_kfd_dxg_and_dri_entries(tmp_path: Path) -> None:
    dev_root = tmp_path / "dev"
    sys_root = tmp_path / "sys"
    dev_root.mkdir()
    (dev_root / "kfd").write_text("", encoding="utf-8")
    dri_root = dev_root / "dri"
    dri_root.mkdir()
    (dri_root / "card0").write_text("", encoding="utf-8")
    (dri_root / "renderD128").write_text("", encoding="utf-8")
    card0_device = sys_root / "class" / "drm" / "card0" / "device"
    card0_device.mkdir(parents=True)
    (card0_device / "vendor").write_text("0x1002\n", encoding="utf-8")
    (card0_device / "device").write_text("0x744c\n", encoding="utf-8")
    (card0_device / "uevent").write_text(
        "DRIVER=amdgpu\nPCI_ID=1002:744C\n", encoding="utf-8"
    )

    devices = gpu_probe.probe_devices(dev_root, sys_root)

    assert devices["kfd"]["exists"] is True
    assert devices["kfd"]["kind"] == "regular_file"
    assert devices["dxg"]["exists"] is False
    assert devices["dri"]["exists"] is True
    assert devices["dri"]["kind"] == "directory"
    assert [Path(entry["path"]).name for entry in devices["dri"]["entries"]] == [
        "card0",
        "renderD128",
    ]
    card0 = devices["dri"]["entries"][0]
    assert card0["drm_sysfs"]["device"]["vendor"]["value"] == "0x1002"
    assert card0["drm_sysfs"]["device"]["device"]["value"] == "0x744c"
    assert "DRIVER=amdgpu" in card0["drm_sysfs"]["device"]["uevent"]["value"]


def test_probe_virtualization_reports_observed_hints(tmp_path: Path) -> None:
    proc_root = tmp_path / "proc"
    sys_root = tmp_path / "sys"
    host_root = tmp_path / "host"
    (proc_root / "sys" / "kernel").mkdir(parents=True)
    (proc_root / "1").mkdir(parents=True)
    (sys_root / "class" / "dmi" / "id").mkdir(parents=True)
    host_root.mkdir()

    (proc_root / "sys" / "kernel" / "osrelease").write_text(
        "5.15.90.1-microsoft-standard-WSL2\n", encoding="utf-8"
    )
    (proc_root / "version").write_text(
        "Linux version 5.15.90.1-microsoft-standard-WSL2\n", encoding="utf-8"
    )
    (proc_root / "cpuinfo").write_text(
        "processor\t: 0\nflags\t\t: fpu sse2 hypervisor\n", encoding="utf-8"
    )
    (proc_root / "1" / "cgroup").write_text(
        "0::/docker/1234567890abcdef\n", encoding="utf-8"
    )
    (sys_root / "class" / "dmi" / "id" / "product_name").write_text(
        "KVM\n", encoding="utf-8"
    )
    (host_root / ".dockerenv").write_text("", encoding="utf-8")

    config = gpu_probe.ProbeConfig(
        proc_root=proc_root,
        sys_root=sys_root,
        host_root=host_root,
        environ={
            "WSL_DISTRO_NAME": "Ubuntu",
            "WSL_INTEROP": "/run/WSL/123_interop",
        },
    )
    hints = gpu_probe.probe_virtualization(config)

    assert "microsoft" in hints["kernel"]["osrelease"]["observed_terms"]
    assert "wsl" in hints["kernel"]["osrelease"]["observed_terms"]
    assert hints["cpu"]["hypervisor_cpu_flag_present"] is True
    assert "docker" in hints["container"]["cgroup"]["observed_terms"]
    assert hints["container"]["marker_paths"][0]["exists"] is True

    dmi_by_name = {fact["name"]: fact for fact in hints["dmi"]}
    assert dmi_by_name["product_name"]["value"] == "KVM"
    assert "kvm" in dmi_by_name["product_name"]["observed_terms"]

    environment = {fact["name"]: fact for fact in hints["environment"]}
    assert environment["WSL_DISTRO_NAME"]["value"] == "Ubuntu"
    assert environment["WSL_INTEROP"]["present"] is True
    assert "value" not in environment["WSL_INTEROP"]


def test_probe_commands_reports_availability_and_capped_output() -> None:
    specs = (
        gpu_probe.CommandSpec("Vulkan", "vulkaninfo", ("--summary",)),
        gpu_probe.CommandSpec("OpenCL", "clinfo", ("-l",)),
    )
    config = gpu_probe.ProbeConfig(
        run_command_outputs=True,
        command_timeout_seconds=1.5,
        max_output_bytes=8,
    )
    seen: dict[str, object] = {}

    def fake_which(name: str, *, path: str | None = None) -> str | None:
        seen["path_env"] = path
        if name == "vulkaninfo":
            return "/usr/bin/vulkaninfo"
        return None

    def fake_runner(
        command: list[str],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        seen["command"] = command
        seen["timeout"] = kwargs["timeout"]
        return subprocess.CompletedProcess(
            command,
            0,
            stdout="abcdefghi",
            stderr="warning",
        )

    results = gpu_probe.probe_commands(
        config,
        command_specs=specs,
        which=fake_which,
        runner=fake_runner,
    )

    assert results[0]["available"] is True
    assert results[0]["path"] == "/usr/bin/vulkaninfo"
    assert results[0]["output"]["command"] == ["/usr/bin/vulkaninfo", "--summary"]
    assert results[0]["output"]["stdout"] == "abcdefgh"
    assert results[0]["output"]["stdout_truncated"] is True
    assert results[0]["output"]["stdout_bytes"] == 9
    assert seen["timeout"] == 1.5

    assert results[1]["available"] is False
    assert results[1]["output"] == {"executed": False, "reason": "not_available"}


def test_probe_commands_can_skip_command_execution() -> None:
    specs = (gpu_probe.CommandSpec("HIP", "hipconfig", ("--platform",)),)
    config = gpu_probe.ProbeConfig(run_command_outputs=False)

    def fake_which(name: str, *, path: str | None = None) -> str | None:
        assert name == "hipconfig"
        assert path is None
        return "/usr/bin/hipconfig"

    def never_run(
        command: list[str],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        raise AssertionError("runner should not be called")

    results = gpu_probe.probe_commands(
        config,
        command_specs=specs,
        which=fake_which,
        runner=never_run,
    )

    assert results == [
        {
            "category": "HIP",
            "name": "hipconfig",
            "available": True,
            "path": "/usr/bin/hipconfig",
            "probe_args": ["--platform"],
            "output": {"executed": False, "reason": "disabled"},
        }
    ]
