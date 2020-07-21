#!/usr/bin/env python3
"""
GenomicSQLite Python binding
"""
import os
import sys
import platform
import json
import sqlite3
from typing import Optional, NamedTuple, Dict, Any
from ctypes.util import find_library

HERE = os.path.dirname(__file__)

# One-time global initialization -- locate shared library file; preferably the copy installed with
# this package, otherwise look in the usual places.
_DLL = None
if platform.system() == "Linux" and os.path.isfile(os.path.join(HERE, "libgenomicsqlite.so")):
    _DLL = os.path.join(HERE, "libgenomicsqlite.so")
if not _DLL:
    _DLL = find_library("genomicsqlite")
assert _DLL, "couldn't locate genomicsqlite shared-library file"
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
    config_json = json.dumps(config)

    # formulate the URI connection string
    uri = _execute1(_MEMCONN, "SELECT genomicsqlite_uri(?,?)", (dbfile, config_json),)
    if read_only:
        uri += "&mode=ro"

    # open the connection
    conn = sqlite3.connect(uri, uri=True, **kwargs)

    # perform GenomicSQLite tuning
    tuning_sql = _execute1(conn, "SELECT genomicsqlite_tuning_sql(?)", (config_json,),)
    conn.executescript(tuning_sql)

    return conn


def vacuum_into_sql(conn: sqlite3.Connection, destfile: str, **config) -> None:
    config_json = json.dumps(config)
    return _execute1(conn, "SELECT genomicsqlite_vacuum_into_sql(?,?)", (destfile, config_json))


def create_genomic_range_index_sql(
    conn: sqlite3.Connection,
    table: str,
    rid: str,
    beg: str,
    end: str,
    max_depth: Optional[int] = None,
) -> str:
    return _execute1(
        conn, "SELECT create_genomic_range_index_sql(?,?,?,?,?)", (table, rid, beg, end, max_depth)
    )


def genomic_range_rowids_sql(
    conn: sqlite3.Connection,
    indexed_table: str,
    qrid: Optional[str] = None,
    qbeg: Optional[str] = None,
    qend: Optional[str] = None,
    safe: bool = False,
) -> str:
    return _execute1(
        conn,
        f"SELECT genomic_range_rowids_{'safe_' if safe else ''}sql(?,?,?,?)",
        (indexed_table, qrid, qbeg, qend),
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
        assert (
            isinstance(row[0], int) and row[0] not in ans
        ), "genomicsqlite: invalid or duplicate reference sequence rid"
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


def _cli():
    """
    Command-line entry point wrapping the `sqlite3` interactive CLI to open a GenomicSQLite
    compressed database file.

    This isn't part of the Python-facing GenomicSQLite API, and shouldn't be reimplemented in other
    language bindings.
    """

    if len(sys.argv) < 2 or not os.path.isfile(sys.argv[1]):
        print("Usage: genomicsqlite DBFILENAME [-readonly] [sqlite3_ARG ...]", file=sys.stderr)
        print(
            "Enters the sqlite3 interactive CLI on a GenomicSQLite-compressed database.",
            file=sys.stderr,
        )
        sys.exit(1)

    dbfilename = sys.argv[1]
    if os.path.islink(dbfilename):
        target = os.path.realpath(dbfilename)
        print(f"[warning] following symlink {dbfilename} -> {target}")
        dbfilename = target

    uri = _execute1(_MEMCONN, "SELECT genomicsqlite_uri(?)", (dbfilename,))
    tuning_sql = _execute1(_MEMCONN, "SELECT genomicsqlite_tuning_sql()")

    if "-readonly" in sys.argv:
        uri += "&mode=ro"
    cmd = [
        "sqlite3",
        "-cmd",
        f".load {_DLL}",
        "-cmd",
        f".open {uri}",
        "-cmd",
        tuning_sql,
        "-cmd",
        ".headers on",
        "-cmd",
        ".mode tabs",
        "-cmd",
        ".databases",
        "-cmd",
        'SELECT "GenomicSQLite " || genomicsqlite_version()',
        "-cmd",
        '.prompt "GenomicSQLite> "',
    ]
    cmd.extend(sys.argv[2:])
    if sys.stdout.isatty():
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


if __name__ == "__main__":
    _cli()
