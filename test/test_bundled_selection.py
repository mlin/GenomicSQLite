"""Test library precedence, target matching, and load-failure diagnostics.

Each case imports an isolated package copy in a fresh interpreter.
"""

import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

PACKAGE_SRC = Path(__file__).resolve().parent.parent / "bindings" / "python" / "genomicsqlite"
# The test host's own target; unspecified hosts skip the module.
HOST_ARCH = {
    "x86_64": "x86_64",
    "amd64": "x86_64",
    "aarch64": "aarch64",
    "arm64": "aarch64",
}.get(platform.machine())
if HOST_ARCH is None or platform.system() not in ("Linux", "Darwin"):
    pytest.skip("bundled selection is specified for Linux and macOS", allow_module_level=True)
if platform.system() == "Linux":
    HOST_TRIPLE = f"{HOST_ARCH}-linux-gnu"
    HOST_LIBNAME = "libgenomicsqlite.so"
    OTHER_TRIPLE = f"{'aarch64' if HOST_ARCH == 'x86_64' else 'x86_64'}-linux-gnu"
else:
    HOST_TRIPLE = f"{HOST_ARCH}-darwin"
    HOST_LIBNAME = "libgenomicsqlite.dylib"
    OTHER_TRIPLE = f"{'aarch64' if HOST_ARCH == 'x86_64' else 'x86_64'}-darwin"

# the failure-message standard (docs/bindings.md): source clause + remedies
OVERRIDE_SOURCE = "(from the LIBGENOMICSQLITE environment variable)"
BUNDLED_SOURCE = "(from the copy bundled with this package)"
SYSTEM_SOURCE = "(from the system library search path)"
REMEDIES = (
    "set LIBGENOMICSQLITE to the library file",
    "install libgenomicsqlite where the system loader finds it",
)


def real_library() -> Path | None:
    """Find an existing candidate library in the test environment or build directory."""
    candidates = []
    env = os.environ.get("LIBGENOMICSQLITE", "")
    if env:
        candidates += [Path(env), Path(env + ".so"), Path(env + ".dylib")]
    candidates.append(Path(__file__).resolve().parent.parent / "build" / HOST_LIBNAME)
    return next((p for p in candidates if p.is_file()), None)


# Inject load failure so negative cases do not depend on system-library absence.
FAIL_LOAD_PRELUDE = (
    "import sqlite3\n"
    "class _FailConn(sqlite3.Connection):\n"
    "    def load_extension(self, path):\n"
    '        raise sqlite3.OperationalError("injected load failure")\n'
    "_real_connect = sqlite3.connect\n"
    "def _connect(*a, **k):\n"
    '    k["factory"] = _FailConn\n'
    "    return _real_connect(*a, **k)\n"
    "sqlite3.connect = _connect\n"
)


def run_child(
    tmp_path: Path, plant: dict[str, bytes], env_override, prelude: str = "", fail_load=False
):
    """Import an isolated package copy and return the subprocess result.

    plant maps target directories to library bytes. Successful imports print _DLL.
    With fail_load, extension loading raises a deterministic SQLite error.
    """
    pkg = tmp_path / "pkg" / "genomicsqlite"
    pkg.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(PACKAGE_SRC / "__init__.py", pkg / "__init__.py")
    for triple, content in plant.items():
        lib = pkg / "lib" / triple / HOST_LIBNAME
        lib.parent.mkdir(parents=True, exist_ok=True)
        lib.write_bytes(content)
    env = {k: v for k, v in os.environ.items() if k != "LIBGENOMICSQLITE"}
    env["PYTHONPATH"] = str(tmp_path / "pkg")
    if env_override is not None:
        env["LIBGENOMICSQLITE"] = env_override
    if fail_load:
        prelude += FAIL_LOAD_PRELUDE
    code = prelude + "import genomicsqlite; print(genomicsqlite._DLL)"
    return subprocess.run(
        [sys.executable, "-c", code],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,  # cases assert on the return code themselves
    )


needs_real = pytest.mark.skipif(real_library() is None, reason="no loadable library available")


@needs_real
def test_override_wins(tmp_path):
    lib = real_library()
    result = run_child(tmp_path, {}, str(lib))
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == str(lib)


@needs_real
def test_override_beats_corrupt_bundle(tmp_path):
    lib = real_library()
    result = run_child(tmp_path, {HOST_TRIPLE: b"not a library"}, str(lib))
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == str(lib)


@needs_real
def test_override_empty_means_unset(tmp_path):
    planted = tmp_path / "pkg" / "genomicsqlite" / "lib" / HOST_TRIPLE / HOST_LIBNAME
    for empty in ("", "   "):
        result = run_child(tmp_path, {HOST_TRIPLE: real_library().read_bytes()}, empty)
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == str(planted)


def test_override_bogus_path_is_named(tmp_path):
    bogus = "/nonexistent/libgenomicsqlite.so"
    result = run_child(tmp_path, {}, bogus)
    assert result.returncode != 0
    assert bogus in result.stderr
    assert OVERRIDE_SOURCE in result.stderr
    for remedy in REMEDIES:
        assert remedy in result.stderr


@needs_real
def test_bogus_override_beats_valid_bundle(tmp_path):
    planted = tmp_path / "pkg" / "genomicsqlite" / "lib" / HOST_TRIPLE / HOST_LIBNAME
    bogus = "/nonexistent/libgenomicsqlite.so"
    result = run_child(tmp_path, {HOST_TRIPLE: real_library().read_bytes()}, bogus)
    assert result.returncode != 0
    assert bogus in result.stderr
    assert OVERRIDE_SOURCE in result.stderr
    assert str(planted) not in result.stderr


@needs_real
def test_bundled_host_target_selected(tmp_path):
    planted = tmp_path / "pkg" / "genomicsqlite" / "lib" / HOST_TRIPLE / HOST_LIBNAME
    plant = {HOST_TRIPLE: real_library().read_bytes(), OTHER_TRIPLE: b"not a library"}
    result = run_child(tmp_path, plant, None)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == str(planted)


def test_wrong_target_never_selected(tmp_path):
    result = run_child(tmp_path, {OTHER_TRIPLE: b"not a library"}, None, fail_load=True)
    assert result.returncode != 0
    assert OTHER_TRIPLE not in result.stderr
    assert SYSTEM_SOURCE in result.stderr


def test_missing_bundle_falls_back(tmp_path):
    # System search may succeed or fail; only the selected bare name is fixed.
    result = run_child(tmp_path, {}, None)
    if result.returncode == 0:
        assert result.stdout.strip() == "libgenomicsqlite"
    else:
        assert "could not load libgenomicsqlite" in result.stderr
        assert SYSTEM_SOURCE in result.stderr
        for remedy in REMEDIES:
            assert remedy in result.stderr


@needs_real
def test_amd64_machine_alias(tmp_path):
    if platform.system() != "Linux" or HOST_ARCH != "x86_64":
        pytest.skip("amd64 alias case is Linux x86_64-only")
    planted = tmp_path / "pkg" / "genomicsqlite" / "lib" / HOST_TRIPLE / HOST_LIBNAME
    prelude = 'import platform; platform.machine = lambda: "amd64"\n'
    result = run_child(tmp_path, {HOST_TRIPLE: real_library().read_bytes()}, None, prelude)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == str(planted)


def test_corrupt_bundled_file_is_named(tmp_path):
    planted = tmp_path / "pkg" / "genomicsqlite" / "lib" / HOST_TRIPLE / HOST_LIBNAME
    result = run_child(tmp_path, {HOST_TRIPLE: b"not a library"}, None)
    assert result.returncode != 0
    assert str(planted) in result.stderr
    assert BUNDLED_SOURCE in result.stderr
    for remedy in REMEDIES:
        assert remedy in result.stderr
