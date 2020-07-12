"""
GenomicSQLite Python binding
"""
import sqlite3
from typing import Optional, NamedTuple, Dict
from ctypes.util import find_library

# load the extension shared-library
_DLL = find_library("genomicsqlite")
assert _DLL, "couldn't locate genomicsqlite shared-library file"
# open a dummy connection to :memory: just for setting up the extension
_MEMCONN = sqlite3.connect(":memory:")
_MEMCONN.enable_load_extension(True)
_MEMCONN.load_extension(_DLL)
# check SQLite version
_MEMCONN.executescript("SELECT genomicsqlite_version_check()")


def connect(
    dbfile: str,
    read_only: bool = False,
    unsafe_load: bool = False,
    page_cache_size: Optional[int] = None,
    threads: Optional[int] = None,
    zstd_level: Optional[int] = None,
    inner_page_size: Optional[int] = None,
    outer_page_size: Optional[int] = None,
    **kwargs,
) -> sqlite3.Connection:
    assert not (read_only and unsafe_load)
    uri = _execute1(
        _MEMCONN,
        "SELECT genomicsqlite_uri(?,?,?,?,?)",
        (dbfile, 1 if unsafe_load else 0, threads, zstd_level, outer_page_size),
    )
    if read_only:
        uri += "&mode=ro"
    conn = sqlite3.connect(uri, uri=True, **kwargs)
    tuning_sql = _execute1(
        conn,
        "SELECT genomicsqlite_tuning_sql(?,?,?,?,?)",
        (None, 1 if unsafe_load else 0, page_cache_size, threads, inner_page_size),
    )
    conn.executescript(tuning_sql)
    return conn


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


def create_genomic_range_index(conn: sqlite3.Connection, *args, **kwargs) -> None:
    conn.executescript(create_genomic_range_index_sql(conn, *args, **kwargs))


def genomic_range_rowids_sql(
    conn: sqlite3.Connection,
    indexed_table: str,
    qrid: Optional[str] = None,
    qbeg: Optional[str] = None,
    qend: Optional[str] = None,
) -> str:
    return _execute1(
        conn, "SELECT genomic_range_rowids_sql(?,?,?,?)", (indexed_table, qrid, qbeg, qend)
    )


def put_reference_assembly_sql(
    conn: sqlite3.Connection, assembly: str, schema: Optional[str] = None
) -> str:
    return _execute1(conn, "SELECT put_genomic_reference_assembly_sql(?,?)", (assembly, schema))


def put_reference_assembly(conn: sqlite3.Connection, *args, **kwargs) -> None:
    conn.executescript(put_reference_assembly_sql(conn, *args, **kwargs))


def put_reference_sequence_sql(
    conn: sqlite3.Connection,
    name: str,
    length: int,
    assembly: Optional[str] = None,
    refget_id: Optional[str] = None,
    rid: Optional[int] = None,
    schema: Optional[str] = None,
):
    return _execute1(
        conn,
        "SELECT put_genomic_reference_sequence_sql(?,?,?,?,?,?)",
        (name, length, assembly, refget_id, rid, schema),
    )


def put_reference_sequence(conn: sqlite3.Connection, *args, **kwargs) -> None:
    conn.executescript(put_reference_sequence_sql(conn, *args, **kwargs))


class ReferenceSequence(NamedTuple):
    rid: int
    name: str
    length: int
    assembly: Optional[str]
    refget_id: Optional[str]


def get_reference_sequences_by_rid(
    con: sqlite3.Connection, assembly: Optional[str] = None, schema: Optional[str] = None
) -> Dict[int, ReferenceSequence]:
    table = "__gri_refseq"
    if schema:
        table = f"{schema}.{table}"
    sql = "SELECT rid, name, length, assembly, refget_id FROM " + table
    params = []
    if assembly:
        sql += " WHERE assembly = ?"
        params = (assembly,)
    ans = {}
    for row in con.execute(sql, params):
        assert (
            isinstance(row[0], int) and row[0] not in ans
        ), "genomicsqlite: invalid or duplicate reference sequence rid"
        ans[row[0]] = ReferenceSequence(
            rid=row[0], name=row[1], length=row[2], assembly=row[3], refget_id=row[4]
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


def _execute1(conn, sql, params=None):
    """
    conn.execute(sql,params) and return row 1 column 1
    """
    return next(conn.execute(sql, params))[0]
