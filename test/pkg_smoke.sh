#!/bin/bash
# Installed-package smoke test: verify the Python package end-to-end the way
# an end user experiences it. Stages a copy of bindings/python, plants the
# built library under its architecture-qualified name, pip-installs into a
# throwaway target dir, then -- from a neutral working directory, with
# LIBGENOMICSQLITE unset -- imports the package, creates a database through
# the API, and reads it back through the CLI wrapper.
#
# This exists because the CTest suite sets LIBGENOMICSQLITE, which bypasses
# the package's bundled-library selector entirely; only an installed-package
# run exercises the selector, including loading the library from its
# architecture-qualified filename.
#
#   $1 python interpreter (pip + setuptools available for it)
#   $2 bindings/python source dir
#   $3 built library file to plant
#   $4 architecture-qualified name to plant it as (e.g. libgenomicsqlite-macos-aarch64.dylib)
#   $5 dir containing the sqlite3 shell to use for the CLI check
set -euo pipefail
PY="$1"; PKG_SRC="$2"; LIB="$3"; QNAME="$4"; SHELLDIR="$5"
# Canonicalize $5 to an absolute path BEFORE the cd into the scratch dir
# below: a relative $5 (the Linux call site passes "build") would otherwise
# re-resolve under the scratch dir, silently putting the wrong sqlite3
# shell on PATH for the CLI check.
SHELLDIR="$(cd "$SHELLDIR" && pwd)"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
SRC="$WORK/pkg-src"
cp -rL "$PKG_SRC" "$SRC" # -L: materialize the sqlite_web_dbi.py symlink
cp "$LIB" "$SRC/genomicsqlite/$QNAME"
# version.py wants git describe (unavailable in the staged copy) or this file
[ -f "$SRC/RELEASE-VERSION" ] || echo "0.0.0" > "$SRC/RELEASE-VERSION"
SITE="$WORK/site"
"$PY" -m pip install --quiet --no-build-isolation --target "$SITE" "$SRC"
echo "installed bundled libraries:"; ls "$SITE/genomicsqlite" | grep libgenomicsqlite

cd "$WORK" # neutral cwd: the repo tree must not shadow the installed package
DB="$WORK/smoke.db"
env -u LIBGENOMICSQLITE PYTHONPATH="$SITE" "$PY" - "$DB" <<'EOF'
import genomicsqlite, sys
con = genomicsqlite.connect(sys.argv[1])
print("genomicsqlite_version:", con.execute("SELECT genomicsqlite_version()").fetchone()[0])
con.execute("CREATE TABLE t(x)")
con.execute("INSERT INTO t VALUES('pkg-ok')")
con.commit(); con.close()
print("PYTHON PACKAGE SMOKE OK")
EOF
OUT="$(env -u LIBGENOMICSQLITE PYTHONPATH="$SITE" PATH="$SHELLDIR:$PATH" \
       "$PY" -m genomicsqlite "$DB" "SELECT 'CLI:' || (SELECT x FROM t)")"
echo "$OUT"
[ "$OUT" = "CLI:pkg-ok" ] || { echo "CLI smoke output mismatch" >&2; exit 1; }
echo "CLI SMOKE OK"
