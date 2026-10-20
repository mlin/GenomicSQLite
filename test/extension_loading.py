"""Check that native extension loading is independent of the library filename."""

import shutil
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path


def main():
    library = Path(sys.argv[1]).resolve()
    # Each load runs in a fresh process: GenomicSQLite registers itself for all
    # subsequent connections and remains loaded until process exit.
    if len(sys.argv) == 3:
        con = sqlite3.connect(":memory:")
        con.enable_load_extension(True)
        if sys.argv[2] == "default":
            con.load_extension(str(library))
        else:
            con.execute("SELECT load_extension(?,?)", (str(library), sys.argv[2]))
        con.close()
        con = sqlite3.connect(":memory:")
        assert con.execute("SELECT genomicsqlite_version()").fetchone()[0]
        return

    with tempfile.TemporaryDirectory() as scratch:
        for basename in (
            "libgenomicsqlite",
            "libgenomicsqlite-linux-x86_64",
            "libgenomicsqlite-macos-aarch64",
            "arbitrary name",
        ):
            renamed = Path(scratch) / (basename + library.suffix)
            shutil.copyfile(library, renamed)
            for entrypoint in ("default", "sqlite3_genomicsqlite_init"):
                subprocess.run([sys.executable, __file__, str(renamed), entrypoint], check=True)
                load = f'.load "{renamed}"'
                if entrypoint != "default":
                    load += " " + entrypoint
                result = subprocess.run(
                    ["sqlite3", ":memory:"],
                    input=load + "\nSELECT genomicsqlite_version();\n",
                    text=True,
                    capture_output=True,
                    check=True,
                )
                assert result.stdout.strip(), result.stderr
                assert not result.stderr, result.stderr
                print(basename, entrypoint, "Python and shell OK")


if __name__ == "__main__":
    main()
