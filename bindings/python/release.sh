#!/bin/bash

set -euo pipefail

HERE="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$HERE"

if grep dirty <(git describe --always --dirty); then
    >&2 echo "Cannot release dirty working tree"
    exit 1
fi
if [[ ! -f genomicsqlite/libgenomicsqlite-linux-x86_64.so ]] || [[ ! -f genomicsqlite/libgenomicsqlite-macos-aarch64.dylib ]]; then
    >&2 echo "Download the portable libgenomicsqlite-linux-x86_64.so & libgenomicsqlite-macos-aarch64.dylib to ${HERE}/genomicsqlite/"
    exit 1
fi
function cleanup {
    # force us to put libs on each attempt; ensures a stale version can't be left behind
    rm -f genomicsqlite/libgenomicsqlite*
}
trap cleanup EXIT

rm -rf build dist *.egg-info
python3 setup.py sdist
echo -e "\033[0;31;5m -- Pushing $(basename `ls -1 dist/*.tar.gz` .tar.gz) to PyPI! -- \033[0m"
twine upload dist/*.tar.gz
