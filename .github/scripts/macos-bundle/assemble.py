#!/usr/bin/env python3
"""Assemble a macOS CPython bundle from Linux using macOS wheels."""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import shutil
import subprocess
import tarfile
import zipfile
from pathlib import Path

from verify_bundle import (
    WHEEL_PACKAGES,
    check_archive,
    find_wheels,
    installed_metadata,
    normalize_name,
    read_pin_file,
    verify_bundle,
)

ARCH_PLATFORM = {"arm64": "aarch64-apple-darwin", "x86_64": "x86_64-apple-darwin"}
PBS_PLATFORM = {"arm64": "aarch64-apple-darwin", "x86_64": "x86_64-apple-darwin"}


def run(command: list[str], cwd: Path | None = None) -> None:
    print("+", " ".join(command))
    subprocess.run(command, cwd=cwd, check=True)


def validate_constraints(path: Path) -> None:
    names: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith(("#", "--")):
            continue
        requirement = line.split(";", 1)[0].strip()
        match = re.match(r"^([A-Za-z0-9][A-Za-z0-9_.-]*)\s*==", requirement)
        if not match:
            raise RuntimeError(f"constraints gate: unpinned or malformed line: {line}")
        name = normalize_name(match.group(1))
        if name in WHEEL_PACKAGES or name.startswith("omnigent"):
            raise RuntimeError(f"constraints gate: local package leaked into {path.name}: {name}")
        if name in names:
            raise RuntimeError(f"constraints gate: duplicate package {name}")
        names.add(name)
    if not names:
        raise RuntimeError("constraints gate: export is empty")
    print(f"constraints OK: {len(names)} pinned packages")


def export_constraints(repo_root: Path, path: Path) -> None:
    run(
        [
            "uv",
            "export",
            "--frozen",
            "--no-hashes",
            "--no-default-groups",
            "--no-emit-package",
            "omnigent",
            "--no-emit-package",
            "omnigent-client",
            "--no-emit-package",
            "omnigent-ui-sdk",
            "--format",
            "requirements.txt",
            "--output-file",
            str(path),
        ],
        cwd=repo_root,
    )
    validate_constraints(path)


def download_interpreter(arch: str, pins: dict[str, str], destination: Path) -> None:
    asset = pins[f"{arch}.asset"]
    expected = pins[f"{arch}.sha256"]
    url = (
        "https://github.com/astral-sh/python-build-standalone/releases/download/"
        f"{pins['release']}/{asset}"
    )
    run(
        [
            "curl",
            "--fail",
            "--location",
            "--retry",
            "5",
            "--retry-all-errors",
            "--output",
            str(destination),
            url,
        ]
    )
    digest = hashlib.sha256(destination.read_bytes()).hexdigest()
    if digest != expected:
        raise RuntimeError(f"interpreter checksum gate: {asset} is {digest}, expected {expected}")
    print(f"interpreter checksum OK: {asset} sha256={digest}")


def extract_interpreter(archive_path: Path, bundle: Path) -> None:
    with tarfile.open(archive_path, "r:gz") as archive:
        members = archive.getmembers()
        top_levels = {member.name.split("/", 1)[0] for member in members}
        if top_levels != {"python"}:
            raise RuntimeError(f"interpreter archive gate: roots are {sorted(top_levels)}")
        for member in members:
            if member.name.startswith("/") or "/../" in f"/{member.name}/":
                raise RuntimeError(f"interpreter archive gate: unsafe member {member.name}")
        archive.extractall(bundle)
    scrub_external_shebangs(bundle)
    python = bundle / "python"
    if not (python / "bin/python3").is_symlink():
        raise RuntimeError("interpreter identity gate: python/bin/python3 is not a symlink")
    if not (python / "lib/python3.12").is_dir():
        raise RuntimeError("interpreter identity gate: CPython 3.12 prefix is missing")


def scrub_external_shebangs(bundle: Path) -> None:
    scrubbed = 0
    for path in bundle.rglob("*"):
        if not path.is_file() or path.is_symlink():
            continue
        data = path.read_bytes()
        if data.startswith(b"#!") and not data.startswith(b"#!/bin/sh\n"):
            newline = data.find(b"\n")
            if newline < 0:
                raise RuntimeError(f"interpreter shebang gate: malformed {path}")
            path.write_bytes(data[newline + 1 :])
            scrubbed += 1
    print(f"interpreter shebang gate: scrubbed {scrubbed} external shebangs")


def wheel_version(wheels: dict[str, Path]) -> str:
    versions = set()
    for path in wheels.values():
        with zipfile.ZipFile(path) as wheel:
            metadata = [
                wheel.read(name)
                for name in wheel.namelist()
                if name.endswith(".dist-info/METADATA")
            ]
        for data in metadata:
            for line in data.decode().splitlines():
                if line.startswith("Version: "):
                    versions.add(line.split(": ", 1)[1].strip())
    if len(versions) != 1:
        raise RuntimeError(f"lockstep version gate: versions are {sorted(versions)}")
    return next(iter(versions))


def tag_version(version: str) -> None:
    reference = os.environ.get("GITHUB_REF", "")
    if reference.startswith("refs/tags/orvex-v"):
        expected = reference.removeprefix("refs/tags/orvex-v")
        if version != expected:
            raise RuntimeError(f"tag equality gate: wheels are {version}, tag is {expected}")


def write_launcher(bundle: Path) -> None:
    (bundle / "bin").mkdir(parents=True, exist_ok=True)
    launcher = bundle / "bin/omnigent"
    launcher.write_text(
        """#!/bin/sh
set -eu

self=$0
iterations=0
while [ -L "$self" ]; do
    iterations=$((iterations + 1))
    if [ "$iterations" -gt 40 ]; then
        echo "omnigent: symlink loop" >&2
        exit 1
    fi
    link=$(readlink "$self") || exit 1
    case "$link" in
        /*) self=$link ;;
        *)
            case "$self" in
                */*) directory=${self%/*} ;;
                *) directory=. ;;
            esac
            self=$directory/$link
            ;;
    esac
done
case "$self" in
    /*) ;;
    */*) self=$(pwd -P)/$self ;;
    *) self=$(pwd -P)/$self ;;
esac
case "$self" in
    */*) directory=${self%/*} ;;
    *) directory=. ;;
esac
bundle_root=$(CDPATH='' cd -P "$directory/.." && pwd)
invoked_name=${0##*/}
exec "$bundle_root/python/bin/python3" "$bundle_root/lib/omnigent-launch.py" "$invoked_name" "$@"
""",
        encoding="utf-8",
    )
    launcher.chmod(0o755)
    (bundle / "bin/omni").symlink_to("omnigent")


def write_runtime_files(bundle: Path, version: str, arch: str) -> None:
    (bundle / "lib").mkdir(parents=True, exist_ok=True)
    (bundle / "lib/omnigent-launch.py").write_text(
        """import runpy
import sys

invoked_name, *arguments = sys.argv[1:]
sys.argv = [invoked_name, *arguments]
runpy.run_module("omnigent", run_name="__main__")
""",
        encoding="utf-8",
    )
    (bundle / "README").write_text(
        f"""omnigent {version} macOS {arch} bundle

This archive contains CPython 3.12 and macOS-native wheels. It is assembled on
Linux and has not been executed in CI. It is unsigned and not notarized.

If a browser download is blocked by Gatekeeper, inspect the checksum first and
then remove the browser quarantine attribute once:

    xattr -dr com.apple.quarantine omnigent-{version}-macos-{arch}

The bundle is intended for the matching Apple silicon (arm64) or Intel
(x86_64) architecture. The ordinary orvex installer remains the smaller,
upgradable option when Python and uv are acceptable prerequisites.
""",
        encoding="utf-8",
    )


def write_manifest(bundle: Path, output: Path, version: str, arch: str) -> None:
    site = bundle / "python/lib/python3.12/site-packages"
    installed = installed_metadata(site)
    lines = [
        f"# omnigent {version} macOS {arch} dependency manifest",
        "# Generated from the frozen uv.lock; greenlet is marker-dependent.",
    ]
    for name, (_, package_version, _) in sorted(installed.items()):
        lines.append(f"{name}=={package_version}")
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")


def make_archive(bundle: Path, output: Path, name: str) -> None:
    with tarfile.open(output, "w:gz", dereference=False) as archive:
        archive.add(bundle, arcname=name, recursive=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--arch", choices=sorted(ARCH_PLATFORM), required=True)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--wheels-dir", type=Path, required=True)
    parser.add_argument("--pins", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--staging", type=Path, required=True)
    parser.add_argument("--skip-web-ui", action="store_true")
    parser.add_argument("--min-archive-bytes", type=int, default=0)
    parser.add_argument("--max-archive-bytes", type=int, default=0)
    args = parser.parse_args()

    args.output.mkdir(parents=True, exist_ok=True)
    args.staging.mkdir(parents=True, exist_ok=True)
    wheels = find_wheels(args.wheels_dir)
    version = wheel_version(wheels)
    tag_version(version)
    pins = read_pin_file(args.pins, args.arch)
    name = f"omnigent-{version}-macos-{args.arch}"
    bundle = args.staging / name
    if bundle.exists():
        shutil.rmtree(bundle)
    bundle.mkdir()
    interpreter_archive = args.staging / pins[f"{args.arch}.asset"]
    download_interpreter(args.arch, pins, interpreter_archive)
    extract_interpreter(interpreter_archive, bundle)
    site = bundle / "python/lib/python3.12/site-packages"
    site.mkdir(parents=True, exist_ok=True)
    constraints = args.staging / "constraints.txt"
    export_constraints(args.repo_root, constraints)
    run(
        [
            "uv",
            "pip",
            "install",
            "--no-cache",
            "--target",
            str(site),
            "--python-platform",
            PBS_PLATFORM[args.arch],
            "--python-version",
            "3.12",
            "--only-binary=:all:",
            "--constraint",
            str(constraints),
            *[str(path) for path in wheels.values()],
        ],
        cwd=args.repo_root,
    )
    generated_bin = site / "bin"
    if generated_bin.exists() or generated_bin.is_symlink():
        shutil.rmtree(generated_bin)
    scrub_external_shebangs(bundle)
    write_launcher(bundle)
    write_runtime_files(bundle, version, args.arch)
    archive_path = args.output / f"{name}.tar.gz"
    manifest_path = args.output / f"{name}.requirements.txt"
    verify_bundle(bundle, args.arch, args.wheels_dir, version, args.skip_web_ui, args.pins)
    write_manifest(bundle, manifest_path, version, args.arch)
    make_archive(bundle, archive_path, name)
    check_archive(archive_path, name)
    size = archive_path.stat().st_size
    if args.min_archive_bytes and size < args.min_archive_bytes:
        raise RuntimeError(f"archive size gate: {size} bytes is below minimum")
    if args.max_archive_bytes and size > args.max_archive_bytes:
        raise RuntimeError(f"archive size gate: {size} bytes exceeds maximum")
    print(f"assembled {archive_path} ({size} bytes) and {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
