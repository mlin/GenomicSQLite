import sqlite3
from typing import Optional, NamedTuple, Dict
from ctypes import cdll, cast, c_int, c_char_p, c_void_p, byref, memmove
from ctypes.util import find_library


def connect(
    dbfile: str,
    read_only: bool = False,
    unsafe_load: bool = False,
    page_cache_size: int = -1048576,
    threads: int = -1,
    zstd_level: int = 6,
    inner_page_size=16384,
    outer_page_size=32768,
    **kwargs,
) -> sqlite3.Connection:
    assert isinstance(dbfile, str)
    assert isinstance(zstd_level, int)
    assert -7 <= zstd_level <= 22
    assert isinstance(page_cache_size, int)
    assert isinstance(threads, int)
    assert isinstance(inner_page_size, int)
    assert isinstance(outer_page_size, int)

    assert not (read_only and unsafe_load)
    uri = _uri(dbfile, unsafe_load, threads, zstd_level, outer_page_size)
    if read_only:
        uri += "&mode=ro"
    conn = sqlite3.connect(uri, uri=True, **kwargs)
    conn.executescript(_tuning(unsafe_load, page_cache_size, threads, inner_page_size))
    return conn


def _uri(dbfile, unsafe_load, threads, zstd_level, outer_page_size) -> str:
    dbfile = dbfile.encode()

    func = _link("GenomicSQLiteURI")
    func.restype = c_void_p  # char* that we need to free

    buffer = func(
        c_char_p(dbfile),
        c_int(1 if unsafe_load else 0),
        c_int(threads),
        c_int(zstd_level),
        c_int(outer_page_size),
    )
    try:
        return _decodestr(buffer)
    finally:
        _sqlite3_free(buffer)


def _tuning(unsafe_load, page_cache_size, threads, inner_page_size, schema=None):
    func = _link("GenomicSQLiteTuning")
    func.restype = c_void_p  # char* that we need to free

    buffer = func(
        c_char_p(schema),
        c_int(1 if unsafe_load else 0),
        c_int(page_cache_size),
        c_int(threads),
        c_int(inner_page_size),
    )
    try:
        return _decodestr(buffer)
    finally:
        _sqlite3_free(buffer)


def create_genomic_range_index(
    table: str, rid: str, beg: str, end: str, max_depth: int = -1,
) -> str:
    table = _encodestr(table, True)
    rid = _encodestr(rid, True)
    beg = _encodestr(beg, True)
    end = _encodestr(end, True)
    assert isinstance(max_depth, int)

    func = _link("CreateGenomicRangeIndex")
    func.restype = c_void_p  # char* that we need to free

    buffer = func(c_char_p(table), c_char_p(rid), c_char_p(beg), c_char_p(end), c_int(max_depth))
    try:
        return _decodestr(buffer)
    finally:
        _sqlite3_free(buffer)


def genomic_range_rowids(
    indexed_table: str,
    con: Optional[sqlite3.Connection] = None,
    qrid: Optional[str] = None,
    qbeg: Optional[str] = None,
    qend: Optional[str] = None,
) -> str:
    indexed_table = _encodestr(indexed_table, True)
    qrid = _encodestr(qrid)
    qbeg = _encodestr(qbeg)
    qend = _encodestr(qend)

    func = _link("GenomicRangeRowids")
    func.restype = c_void_p  # char* that we need to free

    buffer = func(
        c_char_p(indexed_table),
        c_int(_sqlite3_cptr(con)),
        c_char_p(qrid),
        c_char_p(qbeg),
        c_char_p(qend),
    )
    try:
        return _decodestr(buffer)
    finally:
        _sqlite3_free(buffer)


def put_reference_assembly(assembly: str, schema: Optional[str] = None) -> str:
    assembly = _encodestr(assembly, True)
    schema = _encodestr(schema)

    func = _link("PutReferenceAssembly")
    func.restype = c_void_p

    buffer = func(c_char_p(assembly), c_char_p(schema))
    try:
        ans = cast(buffer, c_char_p).value
        if not ans:
            raise KeyError(assembly)
        return _decodestr(buffer)
    finally:
        _sqlite3_free(buffer)


def put_reference_sequence(
    name: str,
    length: int,
    assembly: Optional[str] = None,
    refget_id: Optional[str] = None,
    rid: int = -1,
    schema: Optional[str] = None,
):
    name = _encodestr(name, True)
    assembly = _encodestr(assembly)
    refget_id = _encodestr(refget_id)

    func = _link("PutReferenceSequence")
    func.restype = c_void_p

    buffer = func(
        c_char_p(name),
        c_int(length),
        c_char_p(assembly),
        c_char_p(refget_id),
        c_int(rid),
        c_char_p(schema),
    )
    try:
        return _decodestr(buffer)
    finally:
        _sqlite3_free(buffer)


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


def _sqlite3_free(buf):
    _link("sqlite3_free", dll="sqlite3")(buf)


def _encodestr(str_or_none, required=False):
    if str_or_none is not None:
        assert isinstance(str_or_none, str)
        str_or_none = str_or_none.encode()
    else:
        assert not required
    return str_or_none


def _decodestr(void_p):
    value = cast(void_p, c_char_p).value
    assert isinstance(value, bytes)
    ans = value.decode()
    if ans:
        return ans
    value = cast(c_void_p(void_p + 1), c_char_p).value
    assert isinstance(value, bytes)
    raise RuntimeError(value.decode())


_LINK_TABLE = {}


def _link(func_name, dll="genomicsqlite"):
    if func_name not in _LINK_TABLE:
        _LINK_TABLE[func_name] = getattr(cdll.LoadLibrary(find_library(dll)), func_name)
    return _LINK_TABLE[func_name]


def _sqlite3_cptr(con: Optional[sqlite3.Connection]) -> int:
    if con:
        assert isinstance(con, sqlite3.Connection)
        cptr = c_int(-1)
        # read the sqlite3* from ((char*)&con)+sizeof(PyObject_HEAD)
        # https://github.com/python/cpython/blob/ba1c2c85b39fbcb31584c20f8a63fb87f9cb9c02/Modules/_sqlite/connection.h#L36-L39
        # this is horrible...but succinct!
        memmove(byref(cptr), id(con) + 16, 8)
        return cptr.value
    return 0


def _sqlite3_cptr_selftest(con):
    lib = cdll.LoadLibrary(find_library("sqlite3"))
    lib.sqlite3_set_last_insert_rowid(c_int(_sqlite3_cptr(con)), c_int(0xBADF00D))
    lib.sqlite3_last_insert_rowid.restype = c_int
    assert lib.sqlite3_last_insert_rowid(c_int(_sqlite3_cptr(con))) == 0xBADF00D


def _init():
    # load C extension
    dll = find_library("genomicsqlite")
    assert dll, "couldn't locate shared-library file for genomicsqlite"
    con = sqlite3.connect(":memory:")
    _sqlite3_cptr_selftest(con)
    con.enable_load_extension(True)
    con.load_extension(dll)
    # check SQLite version
    check_version = _link("GenomicSQLiteVersionCheck")
    check_version.restype = c_char_p
    version_msg = check_version()
    assert not version_msg, version_msg


_init()
