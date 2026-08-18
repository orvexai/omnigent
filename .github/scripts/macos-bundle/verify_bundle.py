#!/usr/bin/env python3
"""Structural checks for a macOS bundle assembled on Linux.

This module deliberately never executes anything from the bundle.  Its Mach-O
and ELF checks use magic bytes so the gates work on a Linux runner.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import shutil
import struct
import tarfile
import tempfile
import zipfile
from pathlib import Path

ARCH_CPUTYPE = {"arm64": 0x0100000C, "x86_64": 0x01000007}
WHEEL_PACKAGES = {"omnigent", "omnigent-client", "omnigent-ui-sdk"}
NORMALIZE = re.compile(r"[-_.]+")


class VerificationError(RuntimeError):
    """A named structural gate failed."""


def fail(reason: str) -> None:
    raise VerificationError(reason)


def normalize_name(name: str) -> str:
    return NORMALIZE.sub("-", name).lower()


def metadata_from_bytes(data: bytes) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in data.decode("utf-8", errors="strict").splitlines():
        if ": " in line:
            key, value = line.split(": ", 1)
            if key in {"Name", "Version"}:
                values[key] = value.strip()
    return values


def wheel_metadata(path: Path) -> dict[str, str]:
    with zipfile.ZipFile(path) as wheel:
        candidates = [name for name in wheel.namelist() if name.endswith(".dist-info/METADATA")]
        if len(candidates) != 1:
            fail(f"wheel metadata gate: {path} has {len(candidates)} METADATA files")
        metadata = metadata_from_bytes(wheel.read(candidates[0]))
    if set(metadata) != {"Name", "Version"}:
        fail(f"wheel metadata gate: {path} lacks Name/Version")
    return metadata


def find_wheels(wheels_dir: Path) -> dict[str, Path]:
    result: dict[str, Path] = {}
    for path in sorted(wheels_dir.glob("*.whl")):
        metadata = wheel_metadata(path)
        name = normalize_name(metadata["Name"])
        if name in WHEEL_PACKAGES:
            if name in result:
                fail(f"wheel input gate: duplicate {name} wheels")
            result[name] = path
    missing = WHEEL_PACKAGES - result.keys()
    if missing:
        fail(f"wheel input gate: missing {', '.join(sorted(missing))}")
    return result


def read_pin_file(path: Path, arch: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator:
            fail(f"interpreter identity gate: malformed pin line {line!r}")
        values[key] = value
    for key in ("release", "python", f"{arch}.asset", f"{arch}.sha256"):
        if not values.get(key):
            fail(f"interpreter identity gate: missing pin {key}")
    return values


def macho_slices(data: bytes) -> list[int]:
    if len(data) < 8:
        return []
    little_magic = struct.unpack_from("<I", data)[0]
    big_magic = struct.unpack_from(">I", data)[0]
    if little_magic == 0xFEEDFACF:
        return [struct.unpack_from("<i", data, 4)[0] & 0xFFFFFFFF]
    if big_magic == 0xFEEDFACF:
        return [struct.unpack_from(">i", data, 4)[0] & 0xFFFFFFFF]
    if big_magic in {0xCAFEBABE, 0xCAFEBABF}:
        is_64 = big_magic == 0xCAFEBABF
        entry_size = 32 if is_64 else 20
        if len(data) < 8:
            return []
        count = struct.unpack_from(">I", data, 4)[0]
        end = 8 + count * entry_size
        if end > len(data):
            fail("Mach-O gate: truncated fat header")
        return [
            struct.unpack_from(">i", data, 8 + index * entry_size)[0] & 0xFFFFFFFF
            for index in range(count)
        ]
    if little_magic in {0xCAFEBABE, 0xCAFEBABF}:
        is_64 = little_magic == 0xCAFEBABF
        entry_size = 32 if is_64 else 20
        count = struct.unpack_from("<I", data, 4)[0]
        end = 8 + count * entry_size
        if end > len(data):
            fail("Mach-O gate: truncated little-endian fat header")
        return [
            struct.unpack_from("<i", data, 8 + index * entry_size)[0] & 0xFFFFFFFF
            for index in range(count)
        ]
    return []


def check_binary_tree(bundle: Path, arch: str) -> int:
    expected = ARCH_CPUTYPE[arch]
    macho_count = 0
    for path in bundle.rglob("*"):
        if not path.is_file() or path.is_symlink():
            continue
        data = path.read_bytes()
        if data[:4] == b"\x7fELF":
            fail(f"ELF gate: {path.relative_to(bundle)} is an ELF object")
        if data.startswith(b"#!"):
            if not data.startswith(b"#!/bin/sh\n"):
                relative = path.relative_to(bundle).as_posix()
                fail(f"shebang gate: stray shebang in {relative}")
        slices = macho_slices(data)
        if slices:
            macho_count += 1
            if expected not in slices:
                fail(
                    f"Mach-O architecture gate: {path.relative_to(bundle)} has "
                    f"{[hex(item) for item in slices]}, expected {hex(expected)}"
                )
    if macho_count == 0:
        fail("Mach-O architecture gate: found no Mach-O objects")
    return macho_count


def installed_metadata(site: Path) -> dict[str, tuple[str, str, Path]]:
    result: dict[str, tuple[str, str, Path]] = {}
    for dist_info in sorted(site.glob("*.dist-info")):
        metadata_path = dist_info / "METADATA"
        if not metadata_path.is_file():
            fail(f"dist-info gate: {dist_info.name} has no METADATA")
        metadata = metadata_from_bytes(metadata_path.read_bytes())
        if set(metadata) != {"Name", "Version"}:
            fail(f"dist-info gate: {dist_info.name} lacks Name/Version")
        for filename in ("METADATA", "WHEEL", "RECORD"):
            if not (dist_info / filename).is_file():
                fail(f"dist-info gate: {dist_info.name} has no {filename}")
        normalized = normalize_name(metadata["Name"])
        if normalized in result:
            fail(f"dist-info gate: duplicate installed distribution {normalized}")
        result[normalized] = (metadata["Name"], metadata["Version"], dist_info)
    return result


def check_layout(bundle: Path) -> Path:
    required = [
        bundle / "bin/omnigent",
        bundle / "bin/omni",
        bundle / "python/bin/python3",
        bundle / "python/lib/python3.12/site-packages",
        bundle / "lib/omnigent-launch.py",
        bundle / "README",
    ]
    for path in required:
        if not path.exists() and not path.is_symlink():
            fail(f"layout gate: missing {path.relative_to(bundle)}")
    launcher = bundle / "bin/omnigent"
    if not launcher.is_file() or not launcher.stat().st_mode & 0o111:
        fail("launcher gate: bin/omnigent is not executable")
    launcher_text = launcher.read_text(encoding="utf-8")
    for required_text in ("readlink", "exec ", '"$@"', "/bin/sh"):
        if required_text not in launcher_text:
            fail(f"launcher gate: bin/omnigent lacks {required_text!r}")
    if "PYTHONPATH" in launcher_text or "PYTHONHOME" in launcher_text:
        fail("launcher gate: sets PYTHONPATH or PYTHONHOME")
    omni = bundle / "bin/omni"
    if not omni.is_symlink() or os.readlink(omni) != "omnigent":
        fail("launcher gate: bin/omni is not a relative symlink to omnigent")
    python3 = bundle / "python/bin/python3"
    if not python3.is_symlink():
        fail("interpreter identity gate: python/bin/python3 is not a symlink")
    site = bundle / "python/lib/python3.12/site-packages"
    generated_bin = site / "bin"
    if generated_bin.exists() or generated_bin.is_symlink():
        fail("generated-bin gate: site-packages/bin was not removed")
    if any(path.is_dir() for path in site.rglob("__pycache__")):
        fail("pycache gate: __pycache__ exists under site-packages")
    return site


def check_wheels(
    bundle: Path,
    wheels_dir: Path,
    expected_version: str | None,
    skip_web_ui: bool,
) -> dict[str, str]:
    wheels = find_wheels(wheels_dir)
    wheel_metadata_map = {name: wheel_metadata(path) for name, path in wheels.items()}
    versions = {metadata["Version"] for metadata in wheel_metadata_map.values()}
    if len(versions) != 1:
        fail(f"lockstep version gate: wheel versions differ: {sorted(versions)}")
    version = next(iter(versions))
    if expected_version and version != expected_version:
        fail(f"version gate: wheels are {version}, expected {expected_version}")
    site = bundle / "python/lib/python3.12/site-packages"
    installed = installed_metadata(site)
    for name, metadata in wheel_metadata_map.items():
        installed_item = installed.get(name)
        if not installed_item:
            fail(f"dist-info gate: wheel {name} is not installed")
        if installed_item[1] != metadata["Version"]:
            fail(f"dist-info gate: installed {name} version differs from wheel")
        with zipfile.ZipFile(wheels[name]) as wheel:
            for member in wheel.infolist():
                if member.is_dir() or not member.filename.startswith("omnigent/"):
                    continue
                target = site / member.filename
                if not target.is_file():
                    if not skip_web_ui and member.filename.endswith(
                        "server/static/web-ui/index.html"
                    ):
                        continue
                    fail(f"wheel identity gate: missing {member.filename}")
                if (
                    hashlib.sha256(target.read_bytes()).digest()
                    != hashlib.sha256(wheel.read(member)).digest()
                ):
                    fail(f"wheel identity gate: changed {member.filename}")
    if not skip_web_ui:
        app_wheel = wheels["omnigent"]
        with zipfile.ZipFile(app_wheel) as wheel:
            index_members = [
                member
                for member in wheel.namelist()
                if member.endswith("omnigent/server/static/web-ui/index.html")
            ]
            if len(index_members) != 1:
                fail("web UI gate: wheel has no unique web-ui/index.html")
            expected_hash = hashlib.sha256(wheel.read(index_members[0])).hexdigest()
        installed_index = site / "omnigent/server/static/web-ui/index.html"
        if not installed_index.is_file():
            fail("web UI gate: installed index.html is missing")
        actual_hash = hashlib.sha256(installed_index.read_bytes()).hexdigest()
        if actual_hash != expected_hash:
            fail("web UI gate: installed index.html differs from the app wheel")
    return {name: metadata["Version"] for name, metadata in wheel_metadata_map.items()}


def check_archive(archive_path: Path, expected_root: str) -> None:
    with tarfile.open(archive_path, "r:gz") as archive:
        members = archive.getmembers()
    if not members:
        fail("archive shape gate: archive is empty")
    roots = {member.name.split("/", 1)[0] for member in members}
    if roots != {expected_root}:
        fail(f"archive shape gate: top-level roots are {sorted(roots)}")
    for member in members:
        if member.name.startswith("/") or "/../" in f"/{member.name}/":
            fail(f"archive shape gate: unsafe member {member.name}")
    by_name = {member.name: member for member in members}
    omni = by_name.get(f"{expected_root}/bin/omni")
    if not omni or not omni.issym() or omni.linkname != "omnigent":
        fail("archive shape gate: bin/omni symlink was not preserved")
    launcher = by_name.get(f"{expected_root}/bin/omnigent")
    if not launcher or not launcher.mode & 0o111:
        fail("archive shape gate: bin/omnigent mode was not preserved")


def manifest_packages(path: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        name, separator, version = line.partition("==")
        if not separator or not version:
            fail(f"manifest gate: malformed line in {path.name}: {line}")
        normalized = normalize_name(name)
        if normalized in result:
            fail(f"manifest gate: duplicate {normalized} in {path.name}")
        result[normalized] = version
    return result


def compare_manifests(paths: list[Path]) -> None:
    if len(paths) != 2:
        fail("cross-arch gate: expected exactly two manifests")
    manifests = {path.name: manifest_packages(path) for path in paths}
    first, second = manifests.values()
    difference = set(first) ^ set(second)
    if difference != {"greenlet"}:
        fail(f"cross-arch gate: package-set difference is {sorted(difference)}")
    for package in set(first) & set(second):
        if first[package] != second[package]:
            fail(f"cross-arch gate: {package} versions differ")
    print(
        "cross-arch package identity OK: "
        f"{len(set(first) & set(second))} common packages, greenlet marker difference"
    )


def verify_bundle(
    bundle: Path,
    arch: str,
    wheels_dir: Path | None = None,
    expected_version: str | None = None,
    skip_web_ui: bool = False,
    pins: Path | None = None,
) -> None:
    if arch not in ARCH_CPUTYPE:
        fail(f"architecture gate: unsupported user-facing label {arch}")
    site = check_layout(bundle)
    macho_count = check_binary_tree(bundle, arch)
    if wheels_dir:
        check_wheels(bundle, wheels_dir, expected_version, skip_web_ui)
    if pins:
        values = read_pin_file(pins, arch)
        if not (bundle / "python/lib/python3.12").is_dir():
            fail("interpreter identity gate: CPython 3.12 prefix is missing")
        if values["python"].split(".", 2)[:2] != ["3", "12"]:
            fail("interpreter identity gate: pin is not CPython 3.12")
    print(f"bundle OK: arch={arch}, Mach-O files={macho_count}, site={site}")


def expect_failure(label: str, callback) -> None:
    try:
        callback()
    except VerificationError as error:
        if label not in str(error):
            fail(f"induced-failure gate: expected {label!r}, got {error}")
        print(f"induced failure OK: {label}")
    else:
        fail(f"induced-failure gate: {label} mutation unexpectedly passed")


def induced_failure_tests(
    bundle: Path,
    arch: str,
    wheels_dir: Path,
    expected_version: str | None,
    skip_web_ui: bool,
) -> None:
    # Alongside the bundle, not in /tmp: the negative tests hardlink the tree,
    # and the runner mounts the workspace on a different filesystem.
    with tempfile.TemporaryDirectory(prefix="macos-bundle-negative-", dir=bundle.parent) as temp:
        temp_root = Path(temp)
        elf = temp_root / "elf"
        shutil.copytree(bundle, elf, symlinks=True, copy_function=os.link)
        (elf / "python/lib/python3.12/site-packages/bad.so").write_bytes(b"\x7fELFbad")
        expect_failure(
            "ELF gate",
            lambda: verify_bundle(elf, arch, wheels_dir, expected_version, skip_web_ui),
        )

        missing_ui = temp_root / "missing-ui"
        shutil.copytree(bundle, missing_ui, symlinks=True, copy_function=os.link)
        index = missing_ui / (
            "python/lib/python3.12/site-packages/omnigent/server/static/web-ui/index.html"
        )
        if not skip_web_ui and index.exists():
            index.unlink()
            expect_failure(
                "web UI gate",
                lambda: verify_bundle(missing_ui, arch, wheels_dir, expected_version, skip_web_ui),
            )

        generated_bin = temp_root / "generated-bin"
        shutil.copytree(bundle, generated_bin, symlinks=True, copy_function=os.link)
        (generated_bin / "python/lib/python3.12/site-packages/bin").mkdir()
        expect_failure(
            "generated-bin gate",
            lambda: verify_bundle(generated_bin, arch, wheels_dir, expected_version, skip_web_ui),
        )


def self_test() -> None:
    with tempfile.TemporaryDirectory(prefix="macos-bundle-self-test-") as temp:
        root = Path(temp)
        thin = root / "arm.so"
        thin.write_bytes(struct.pack("<Iii", 0xFEEDFACF, ARCH_CPUTYPE["arm64"], 0))
        assert macho_slices(thin.read_bytes()) == [ARCH_CPUTYPE["arm64"]]
        fat = root / "fat.so"
        fat.write_bytes(
            struct.pack(">II", 0xCAFEBABE, 2)
            + struct.pack(">iiIII", ARCH_CPUTYPE["arm64"], 0, 0, 0, 0)
            + struct.pack(">iiIII", ARCH_CPUTYPE["x86_64"], 0, 0, 0, 0)
        )
        assert set(macho_slices(fat.read_bytes())) == set(ARCH_CPUTYPE.values())
        foreign_root = root / "foreign"
        foreign_root.mkdir()
        (foreign_root / "intel.so").write_bytes(
            struct.pack("<Iii", 0xFEEDFACF, ARCH_CPUTYPE["x86_64"], 0)
        )
        try:
            check_binary_tree(foreign_root, "arm64")
        except VerificationError as error:
            assert "Mach-O architecture gate" in str(error)
        else:
            raise AssertionError("foreign-architecture fixture did not fail")
        elf = root / "bad.so"
        elf.write_bytes(b"\x7fELF")
        try:
            check_binary_tree(root, "arm64")
        except VerificationError as error:
            assert "ELF gate" in str(error)
        else:
            raise AssertionError("ELF fixture did not fail")
    print("verify_bundle self-test OK")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bundle", type=Path)
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--archive-root")
    parser.add_argument("--arch", choices=sorted(ARCH_CPUTYPE))
    parser.add_argument("--wheels-dir", type=Path)
    parser.add_argument("--expected-version")
    parser.add_argument("--skip-web-ui", action="store_true")
    parser.add_argument("--pins", type=Path)
    parser.add_argument("--manifest", type=Path, action="append", default=[])
    parser.add_argument("--induced-failure-tests", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    try:
        if args.self_test:
            self_test()
            return 0
        if args.manifest:
            compare_manifests(args.manifest)
            return 0
        if not args.bundle or not args.arch:
            parser.error("--bundle and --arch are required")
        verify_bundle(
            args.bundle,
            args.arch,
            args.wheels_dir,
            args.expected_version,
            args.skip_web_ui,
            args.pins,
        )
        if args.archive:
            check_archive(args.archive, args.archive_root or args.bundle.name)
        if args.induced_failure_tests:
            if not args.wheels_dir:
                parser.error("--wheels-dir is required for induced-failure tests")
            induced_failure_tests(
                args.bundle,
                args.arch,
                args.wheels_dir,
                args.expected_version,
                args.skip_web_ui,
            )
        return 0
    except VerificationError as error:
        print(f"ERROR: {error}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
