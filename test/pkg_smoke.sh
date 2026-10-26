#!/bin/bash
# Exercise installed-package selection and CLI loading with LIBGENOMICSQLITE unset.
# CTest's override otherwise bypasses bundled-library selection.
#
# $1 Python interpreter with pip and setuptools
# $2 Python binding source directory
# $3 built library with its plain filename
# $4 canonical target directory (e.g. aarch64-darwin)
# $5 directory containing the sqlite3 shell
set -euo pipefail
PY="$1"; PKG_SRC="$2"; LIB="$3"; QDIR="$4"; SHELLDIR="$5"
# Resolve relative shell paths before cd changes their meaning.
SHELLDIR="$(cd "$SHELLDIR" && pwd)"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
SRC="$WORK/pkg-src"
cp -rL "$PKG_SRC" "$SRC" # -L: materialize the sqlite_web_dbi.py symlink
mkdir -p "$SRC/genomicsqlite/$QDIR"
cp "$LIB" "$SRC/genomicsqlite/$QDIR/"
# version.py wants git describe (unavailable in the staged copy) or this file
[ -f "$SRC/RELEASE-VERSION" ] || echo "0.0.0" > "$SRC/RELEASE-VERSION"
SITE="$WORK/site"
"$PY" -m pip install --quiet --no-build-isolation --target "$SITE" "$SRC"
echo "installed bundled libraries:"; ls "$SITE/genomicsqlite/$QDIR"

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
