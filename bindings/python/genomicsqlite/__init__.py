#!/usr/bin/env python3
"""
GenomicSQLite Python binding
"""

import json
import os
import platform
import sqlite3
import sys
from typing import Any, Dict, NamedTuple, Optional

_HERE = os.path.dirname(__file__)
_YES = ("1", "true", "t", "yes", "y")

# Module initialization -- locate shared library file
_DLL = os.environ.get("LIBGENOMICSQLITE", "").strip()  # use env var if present
if not _DLL:
    # select the platform-appropriate library bundled with this package, which
    # the release pipeline ships under an architecture-qualified name (the same
    # naming the Rust crate uses). Selection is exact-match on OS + CPU: an
    # Intel machine never selects the ARM build, nor vice versa.
    _BUNDLED_NAME = {
        ("Linux", "x86_64"): "libgenomicsqlite-linux-x86_64.so",
        ("Linux", "aarch64"): "libgenomicsqlite-linux-aarch64.so",
        ("Darwin", "x86_64"): "libgenomicsqlite-macos-x86_64.dylib",
        ("Darwin", "arm64"): "libgenomicsqlite-macos-aarch64.dylib",
    }.get((platform.system(), platform.machine()))
    if _BUNDLED_NAME:
        _bundled_path = os.path.join(_HERE, _BUNDLED_NAME)
        if os.path.isfile(_bundled_path):
            _DLL = _bundled_path
if not _DLL:
    # otherwise, let SQLite use dlopen() to look for it
    _DLL = "libgenomicsqlite"
# open a dummy connection to :memory: just for loading the extension.
_MEMCONN = sqlite3.connect(":memory:")
_MEMCONN.enable_load_extension(True)
_MEMCONN.load_extension(_DLL)
# now that it's been loaded, the extension will automatically enable itself on any new connections.


def _execute1(conn, sql, params=None):
    """
    conn.execute(sql,params) and return row 1 column 1
    """
    assert isinstance(conn, sqlite3.Connection)
    return next(conn.execute(sql, params) if params else conn.execute(sql))[0]


__version__ = _execute1(_MEMCONN, "SELECT genomicsqlite_version()")

# load default configuration
_DEFAULT_CONFIG = json.loads(_execute1(_MEMCONN, "SELECT genomicsqlite_default_config_json()"))


def connect(dbfile: str, read_only: bool = False, **kwargs) -> sqlite3.Connection:
    """
    Open a SQLite connection & activate GenomicSQLite extension on it
    """

    # kwargs may be a mix of GenomicSQLite config settings and sqlite3.connect() kwargs. Extract
    # the GenomicSQLite settings based on the keys found in the default configuration.
    kwargs = dict(kwargs)
    config = {}
    for config_key in _DEFAULT_CONFIG:
        if config_key in kwargs:
            config[config_key] = kwargs[config_key]
            del kwargs[config_key]
    if read_only:
        config["mode"] = "ro"
    config_json = json.dumps(config)

    # formulate the URI connection string
    uri = _execute1(
        _MEMCONN,
        "SELECT genomicsqlite_uri(?,?)",
        (dbfile, config_json),
    )

    # open the connection
    conn = sqlite3.connect(uri, uri=True, **kwargs)

    # perform GenomicSQLite tuning
    tuning_sql = _execute1(
        conn,
        "SELECT genomicsqlite_tuning_sql(?)",
        (config_json,),
    )
    conn.executescript(tuning_sql)

    return conn


def attach_sql(conn: sqlite3.Connection, dbfile: str, schema_name: str, **config) -> str:
    config_json = json.dumps(config)
    return _execute1(
        conn, "SELECT genomicsqlite_attach_sql(?,?,?)", (dbfile, schema_name, config_json)
    )


def vacuum_into_sql(conn: sqlite3.Connection, destfile: str, **config) -> str:
    config_json = json.dumps(config)
    return _execute1(conn, "SELECT genomicsqlite_vacuum_into_sql(?,?)", (destfile, config_json))


def create_genomic_range_index_sql(
    conn: sqlite3.Connection,
    table: str,
    rid: str,
    beg: str,
    end: str,
    floor: Optional[int] = None,
) -> str:
    return _execute1(
        conn, "SELECT create_genomic_range_index_sql(?,?,?,?,?)", (table, rid, beg, end, floor)
    )


def genomic_range_rowids_sql(
    conn: sqlite3.Connection,
    indexed_table: str,
    qrid: Optional[str] = None,
    qbeg: Optional[str] = None,
    qend: Optional[str] = None,
    ceiling: Optional[int] = None,
    floor: Optional[int] = None,
) -> str:
    return _execute1(
        conn,
        "SELECT genomic_range_rowids_sql(?,?,?,?,?,?)",
        (indexed_table, qrid, qbeg, qend, ceiling, floor),
    )


def put_reference_assembly_sql(
    conn: sqlite3.Connection, assembly: str, schema: Optional[str] = None
) -> str:
    return _execute1(conn, "SELECT put_genomic_reference_assembly_sql(?,?)", (assembly, schema))


def put_reference_sequence_sql(
    conn: sqlite3.Connection,
    name: str,
    length: int,
    assembly: Optional[str] = None,
    refget_id: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None,
    rid: Optional[int] = None,
    schema: Optional[str] = None,
):
    meta_json = json.dumps(meta) if meta else None
    return _execute1(
        conn,
        "SELECT put_genomic_reference_sequence_sql(?,?,?,?,?,?,?)",
        (name, length, assembly, refget_id, meta_json, rid, schema),
    )


class ReferenceSequence(NamedTuple):
    rid: int
    name: str
    length: int
    assembly: Optional[str]
    refget_id: Optional[str]
    meta: Dict[str, Any]


def get_reference_sequences_by_rid(
    con: sqlite3.Connection, assembly: Optional[str] = None, schema: Optional[str] = None
) -> Dict[int, ReferenceSequence]:
    table = "_gri_refseq"
    if schema:
        table = f"{schema}.{table}"
    sql = (
        "SELECT _gri_rid, gri_refseq_name, gri_refseq_length, gri_assembly, gri_refget_id, gri_refseq_meta_json FROM "
        + table
    )
    params = []
    if assembly:
        sql += " WHERE gri_assembly = ?"
        params = (assembly,)
    ans = {}
    for row in con.execute(sql, params):
        assert isinstance(row[0], int) and row[0] not in ans, (
            "genomicsqlite: invalid or duplicate reference sequence rid"
        )
        ans[row[0]] = ReferenceSequence(
            rid=row[0],
            name=row[1],
            length=row[2],
            assembly=row[3],
            refget_id=row[4],
            meta=(json.loads(row[5]) if row[5] else {}),
        )
    return ans


def get_reference_sequences_by_name(
    con: sqlite3.Connection, assembly: Optional[str] = None, schema: Optional[str] = None
) -> Dict[str, ReferenceSequence]:
    ans = {}
    for _, item in get_reference_sequences_by_rid(con, assembly, schema).items():
        assert item.name not in ans, "genomicsqlite: non-unique reference sequence names"
        ans[item.name] = item
    return ans


_USAGE = """
Enters the sqlite3 command-line shell on a GenomicSQLite-compressed database.

sqlite3_ARG: passed through to sqlite3 (see `sqlite3 -help`)

--

Other tools:

genomicsqlite DB_FILENAME --compact [options|--help]
[Re]compress and defragment an existing SQLite3 or GenomicSQLite database. See `genomicsqlite --compact --help`

genomicsqlite DB_FILENAME --dbi [options|--help]
Generate a .dbi helper file to optimize web access for a given database. See `genomicsqlite --dbi --help`

"""


def _cli(argv=None):
    """
    Command-line entry point wrapping the `sqlite3` interactive CLI to open a GenomicSQLite
    compressed database file.

    This isn't part of the Python-facing GenomicSQLite API, and shouldn't be reimplemented in other
    language bindings.
    """

    if argv is None:
        argv = sys.argv
    if "--compact" in argv or "-compact" in argv:
        _compact(argv[1], argv[2:])
        return
    if "--dbi" in argv or "-dbi" in argv:
        _dbi(argv[1], argv[2:])
        return

    if (
        len(argv) < 2
        or not (
            next((True for pfx in ("http:", "https:") if argv[1].startswith(pfx)), False)
            or os.path.isfile(argv[1])
        )
        or next((True for a in ("-h", "-help", "--help") if a in argv), False)
    ):
        print(
            "Usage: genomicsqlite DB_FILENAME [--readonly] [sqlite3_ARG ...]\n",
            file=sys.stderr,
        )
        print(
            _USAGE.strip(),
            file=sys.stderr,
        )
        sys.exit(1)

    import shutil
    import tempfile

    if not shutil.which("sqlite3"):
        print(
            "Error: the `sqlite3` command-line shell doesn't seem to be available; ensure it's installed and found in $PATH",
            file=sys.stderr,
        )
        sys.exit(1)

    cfg = {}
    try:
        argv[argv.index("--readonly")] = "-readonly"
    except ValueError:
        pass
    if "-readonly" in argv:
        cfg["mode"] = "ro"
    cfg = json.dumps(cfg)
    uri = _execute1(_MEMCONN, "SELECT genomicsqlite_uri(?,?)", (argv[1], cfg))
    tuning_sql = _execute1(_MEMCONN, "SELECT genomicsqlite_tuning_sql(?)", (cfg,))

    # Assemble the shell startup commands, then replay them inside the shell via
    # "-cmd .read /dev/fd/N" on an anonymous temporary file whose descriptor is
    # inherited across exec:
    #   * Since SQLite 3.52, the shell quits immediately (with a success exit code,
    #     without reading any input) whenever -bail is combined with an SQL -cmd
    #     argument, which silently disabled this wrapper when the startup commands
    #     were passed as -cmd arguments directly. A dot-command -cmd does not take
    #     that code path.
    #   * The anonymous TemporaryFile leaves nothing behind in the temp directory
    #     (unlike a named -init file, which os.execvp() would strand there).
    #   * The single -init slot stays free, so a user-supplied -init in argv
    #     composes with -- instead of displacing -- this startup sequence.
    tuning_sql = tuning_sql.strip()
    if not tuning_sql.endswith(";"):
        # in script context, an unterminated statement would absorb the following line
        tuning_sql += ";"
    init_lines = [
        # quoted: the shell's dot-command tokenizer splits on unquoted whitespace,
        # so paths containing spaces would otherwise truncate at the first space.
        f'.load "{_DLL}"',
        f'.open "{uri}"',
        ".once /dev/null",
        tuning_sql,
        '.prompt "GenomicSQLite> "',
    ]
    if sys.stdout.isatty() and not next(
        (arg for arg in argv[2:] if not arg.startswith("-")), False
    ):
        # interactive mode: print version banner, then turn on column headers
        init_lines += [
            "SELECT 'GenomicSQLite ' || genomicsqlite_version();",
            ".headers on",
        ]

    with tempfile.TemporaryFile("w+") as init_file:
        init_file.write("\n".join(init_lines) + "\n")
        init_file.flush()
        # rewind: on some platforms (notably macOS) reading /dev/fd/N shares this
        # descriptor's file offset, so the shell must inherit it positioned at 0
        os.lseek(init_file.fileno(), 0, os.SEEK_SET)
        # Python file objects are non-inheritable by default (PEP 446); the
        # descriptor must survive the os.execvp() below
        os.set_inheritable(init_file.fileno(), True)

        cmd = ["sqlite3", "-bail", "-cmd", f".read /dev/fd/{init_file.fileno()}"]
        cmd.append(":memory:")  # placeholder so remaining positional args are recognized as such
        cmd += argv[2:]

        if "DEBUG" in os.environ:
            print(
                " ".join(
                    (
                        (
                            (arg if " " not in arg else f"'{arg}'")
                            + (" \\\n    " if len(arg) > 50 else "")
                        )
                        for arg in cmd
                    )
                )
            )
        os.execvp("sqlite3", cmd)


def _compact(dbfilename, argv):
    import argparse
    import urllib.parse

    parser = argparse.ArgumentParser(
        prog="genomicsqlite DB_FILENAME --compact",
        description="[Re]compress and defragment an existing SQLite3 or GenomicSQLite database.",
    )
    parser.add_argument("-compact", "--compact", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument(
        "-o", dest="out_filename", type=str, help="compacted database path [DB_FILENAME.compact]"
    )
    parser.add_argument(
        "-l",
        "--level",
        dest="zstd_level",
        metavar="N",
        type=int,
        default=6,
        choices=range(-5, 20),
        help="compression level (-5 to 19) [6]",
    )
    parser.add_argument(
        "--inner-page-KiB",
        type=int,
        choices=(1, 2, 4, 8, 16, 32, 64),
        default=16,
        help="inner page size [16]",
    )
    parser.add_argument(
        "--outer-page-KiB",
        type=int,
        choices=(1, 2, 4, 8, 16, 32, 64),
        default=64,
        help="outer page size [64]",
    )
    parser.add_argument(
        "--no-defrag", action="store_true", help="skip defragmentation of compressed data"
    )
    parser.add_argument(
        "-@", dest="threads", type=int, default=-1, help="worker threads [host CPUs up to 8]"
    )
    parser.add_argument(
        "-f",
        dest="force",
        action="store_true",
        help="overwrite existing file at target path, if any",
    )
    parser.add_argument("-q", dest="quiet", action="store_true", help="suppress progress messages")
    args = parser.parse_args(argv)
    cfg = {"page_cache_MiB": 64}
    # reduced page_cache_MiB: memory usage goes unexpectedly high otherwise. Theory: SQLite
    # miscalculates the size of the page cache for the new database file when we use VACUUM INTO,
    # maybe due to some confusion involving the cache_size & page_size of the two database files.
    for k in ("zstd_level", "inner_page_KiB", "outer_page_KiB", "threads"):
        cfg[k] = vars(args)[k]

    # sniff whether input db is currently compressed with zstd_vfs or not
    con = None
    web = dbfilename.startswith("http:") or dbfilename.startswith("https:")
    if not web:
        con = sqlite3.connect(f"file:{urllib.parse.quote(dbfilename)}?mode=ro", uri=True)
        if next(con.execute("PRAGMA application_id"))[0] != 0x7A737464:
            # db is uncompressed: apply tuning PRAGMAs
            if not args.quiet:
                print(f"Opened uncompressed database {dbfilename}", file=sys.stderr)
                sys.stderr.flush()
            con.executescript(
                _execute1(
                    con,
                    "SELECT genomicsqlite_tuning_sql(?)",
                    (json.dumps(cfg),),
                )
            )
        else:
            # db is zstd_vfs outer db; proceed to open using our own connect()
            con.close()
            con = None
    if not con:
        if not args.quiet:
            print(f"Opening compressed database {dbfilename} ...", file=sys.stderr)
            sys.stderr.flush()
        con = connect(dbfilename, read_only=True, **cfg)

    # VACUUM INTO to [re]compress
    destfilename = args.out_filename
    if not destfilename:
        destfilename = dbfilename
        if web:
            destfilename = os.path.basename(destfilename)
            try:
                destfilename = destfilename[: destfilename.index("?")]
            except ValueError:
                pass
            assert destfilename
        destfilename += ".compact"

    if not args.quiet:
        print(f"Compressing into {destfilename} ...", file=sys.stderr)
        sys.stderr.flush()
    if args.force and os.path.isfile(destfilename):
        os.unlink(destfilename)
    con.executescript(vacuum_into_sql(con, destfilename, **cfg))
    con.close()

    # VACUUM compressed outer db
    if not args.no_defrag:
        if not args.quiet:
            print("Defragmenting compressed data...", file=sys.stderr)
            sys.stderr.flush()
        con = sqlite3.connect(destfilename, uri=False)
        con.executescript(
            "PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA locking_mode=EXCLUSIVE"
        )
        con.executescript("VACUUM")

    con.close()
    if not args.quiet:
        print("OK", file=sys.stderr)


def _dbi(dbfilename, argv):
    from .sqlite_web_dbi import main as dbi_main  # pylint: disable=E0401,E0611

    argv = [elt for elt in argv if elt not in ("-dbi", "--dbi")]
    dbi_main(["genomicsqlite DBFILE --dbi", dbfilename] + argv)


if __name__ == "__main__":
    _cli(sys.argv)
