#!/bin/bash

set -euo pipefail

HERE="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$HERE"

if grep dirty <(git describe --always --dirty); then
    >&2 echo "Cannot release dirty working tree"
    exit 1
fi
if [[ ! -f genomicsqlite/lib/x86_64-linux-gnu/libgenomicsqlite.so ]] || [[ ! -f genomicsqlite/lib/aarch64-linux-gnu/libgenomicsqlite.so ]] || [[ ! -f genomicsqlite/lib/aarch64-darwin/libgenomicsqlite.dylib ]]; then
    >&2 echo "Download the portable libraries to ${HERE}/genomicsqlite/lib/x86_64-linux-gnu/, ${HERE}/genomicsqlite/lib/aarch64-linux-gnu/ & ${HERE}/genomicsqlite/lib/aarch64-darwin/ (each as plain libgenomicsqlite.so/.dylib inside its per-target directory)"
    exit 1
fi
function cleanup {
    # force us to put libs on each attempt; ensures a stale version can't be left behind
    rm -f genomicsqlite/lib/*/libgenomicsqlite.so genomicsqlite/lib/*/libgenomicsqlite.dylib
}
trap cleanup EXIT

rm -rf build dist *.egg-info
python3 setup.py sdist
if ! tar tzf dist/*.tar.gz | grep -q 'genomicsqlite/sqlite_web_dbi\.py$'; then
    >&2 echo "The sdist is missing genomicsqlite/sqlite_web_dbi.py; refusing to release."
    >&2 echo "That module is a symlink into build/_deps/sqlite_web_vfs-src/, populated by the CMake build."
    >&2 echo "This tree was never built, so setuptools dropped the dangling symlink silently."
    >&2 echo "Build this tree (or materialize the file from a built tree), rebuild the sdist, and retry."
    exit 1
fi
echo -e "\033[0;31;5m -- Pushing $(basename `ls -1 dist/*.tar.gz` .tar.gz) to PyPI! -- \033[0m"
twine upload dist/*.tar.gz
