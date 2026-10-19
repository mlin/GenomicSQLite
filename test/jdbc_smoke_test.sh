#!/bin/bash
# JDBC smoke test: verify the packaged GenomicSQLite JDBC driver end-to-end
# the way an end user experiences it. The driver JAR (built by the preceding
# `mvn deploy` step, with both platform libraries planted in its resources)
# must open a database with LIBGENOMICSQLITE unset -- the CTest JDBC tests
# set that override, which bypasses the driver's bundled-resource selector.
#
# The driver's runtime dependencies (sqlite-jdbc) are NOT inside the JAR and
# NOT in the deploy target repo either -- `mvn deploy` publishes only this
# project's own JAR/POM there. Resolve them here with
# dependency:build-classpath, which emits the complete runtime classpath.
# (An earlier inline version of this step searched target/mvn-repo for the
# sqlite-jdbc JAR, found nothing, and the smoke failed with "No suitable
# driver found".)
#
#   $1 JDBC module directory [bindings/jdbc/genomicsqlite-jdbc]
#
# Run from the repository root, after the JAR build (mvn deploy) step.
set -euo pipefail

MODULE_DIR="${1:-bindings/jdbc/genomicsqlite-jdbc}"
POM="$MODULE_DIR/pom.xml"
[ -f "$POM" ] || {
    echo "jdbc_smoke_test: no POM at $POM -- run from the repository root" >&2
    exit 1
}

JAR="$(ls "$MODULE_DIR"/target/genomicsqlite-jdbc-*.jar 2>/dev/null | head -n1 || true)"
[ -n "$JAR" ] || {
    echo "jdbc_smoke_test: no genomicsqlite-jdbc-*.jar in $MODULE_DIR/target -- did the mvn deploy step run?" >&2
    exit 1
}

# The JAR must carry both bundled libraries under their
# architecture-qualified resource names.
for res in libgenomicsqlite-linux-x86_64.so libgenomicsqlite-macos-aarch64.dylib; do
    unzip -l "$JAR" | grep -q "$res" || {
        echo "jdbc_smoke_test: $JAR lacks bundled resource $res" >&2
        exit 1
    }
done

WORK="$(mktemp -d)"
CP_FILE="$WORK/classpath.txt"
mvn -B -f "$POM" org.apache.maven.plugins:maven-dependency-plugin:3.11.0:build-classpath \
    -DincludeScope=runtime -Dmdep.outputFile="$CP_FILE"
[ -s "$CP_FILE" ] || {
    echo "jdbc_smoke_test: dependency:build-classpath wrote no classpath to $CP_FILE" >&2
    exit 1
}
DEPS_CP="$(cat "$CP_FILE")"
case "$DEPS_CP" in
    *sqlite-jdbc*) ;;
    *)
        echo "jdbc_smoke_test: runtime classpath lacks sqlite-jdbc: $DEPS_CP" >&2
        exit 1
        ;;
esac

cat > "$WORK/Smoke.java" <<'EOF'
import java.sql.*;
public class Smoke {
  public static void main(String[] args) throws Exception {
    Connection conn = DriverManager.getConnection("jdbc:genomicsqlite:" + args[0]);
    Statement st = conn.createStatement();
    ResultSet rs = st.executeQuery("SELECT genomicsqlite_version()");
    rs.next();
    System.out.println("genomicsqlite_version: " + rs.getString(1));
    st.executeUpdate("CREATE TABLE t(x)");
    st.executeUpdate("INSERT INTO t VALUES('jdbc-ok')");
    conn.close();
    System.out.println("JDBC SMOKE OK");
  }
}
EOF
javac -cp "$JAR" -d "$WORK" "$WORK/Smoke.java"
OUT="$(env -u LIBGENOMICSQLITE java -cp "$JAR:$DEPS_CP:$WORK" Smoke "$WORK/smoke.db")"
echo "$OUT"
grep -q "JDBC SMOKE OK" <<<"$OUT" || {
    echo "jdbc_smoke_test: smoke program did not report success" >&2
    exit 1
}
echo "JDBC SMOKE TEST OK"
