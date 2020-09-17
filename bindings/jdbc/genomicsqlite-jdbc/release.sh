#!/bin/bash

set -euo pipefail

HERE="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$HERE"

if grep dirty <(git describe --always --dirty); then
    >&2 echo "Cannot release dirty working tree"
    exit 1
fi
if [[ ! -f src/main/resources/libgenomicsqlite.so ]] || [[ ! -f src/main/resources/libgenomicsqlite.dylib ]]; then
    >&2 echo "Download the portable libgenomicsqlite.{so,dylib} to ${HERE}/src/main/resources/"
    exit 1
fi
function cleanup {
    # force us to put libs on each attempt; ensures a stale version can't be left behind
    rm -f src/main/resources/libgenomicsqlite.*
}
trap cleanup EXIT

mvn clean
mvn deploy -Drevision=$(git describe --long --tags --match 'v[0-9]*.*')
# TODO: sync target/mvn-repo to gh-pages
