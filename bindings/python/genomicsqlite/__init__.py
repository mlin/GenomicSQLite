import sqlite3
from typing import Optional
from ctypes import cdll, cast, c_int, c_char_p, c_void_p, byref, memmove
from ctypes.util import find_library


def connect(
    dbfile: str,
    read_only: bool = False,
    zstd_level: int = 6,
    page_cache_size: int = 0,
    threads: int = -1,
    unsafe_load: bool = False,
    **kwargs
) -> sqlite3.Connection:
    assert isinstance(dbfile, str)
    assert isinstance(zstd_level, int)
    assert -5 <= zstd_level <= 22
    assert isinstance(page_cache_size, int)
    assert isinstance(threads, int)

    assert not (read_only and unsafe_load)
    uri = _uri(dbfile, zstd_level, threads, unsafe_load)
    if read_only:
        uri += "&mode=ro"
    conn = sqlite3.connect(uri, uri=True, **kwargs)
    conn.executescript(_tuning(page_cache_size, threads, unsafe_load))
    return conn


def _uri(dbfile, zstd_level, threads, unsafe_load) -> str:
    dbfile = dbfile.encode()

    func = _link("GenomicSQLiteURI")
    func.restype = c_void_p  # char* that we need to free

    buffer = func(
        c_char_p(dbfile), c_int(zstd_level), c_int(threads), c_int(1 if unsafe_load else 0)
    )
    try:
        return _decodestr(buffer)
    finally:
        _sqlite3_free(buffer)


def _tuning(page_cache_size, threads, unsafe_load, schema=None):
    func = _link("GenomicSQLiteTuning")
    func.restype = c_void_p  # char* that we need to free

    buffer = func(
        c_int(page_cache_size), c_int(threads), c_int(1 if unsafe_load else 0), c_char_p(schema)
    )
    try:
        return _decodestr(buffer)
    finally:
        _sqlite3_free(buffer)


def create_genomic_range_index(
    table: str,
    assembly: Optional[str] = None,
    max_level: int = -1,
    rid_col: Optional[str] = None,
    beg_col: Optional[str] = None,
    end_col: Optional[str] = None,
) -> str:
    assert isinstance(table, str)
    table = table.encode()
    assert isinstance(max_level, int)
    assembly = _encodestr(assembly)
    rid_col = _encodestr(rid_col)
    beg_col = _encodestr(beg_col)
    end_col = _encodestr(end_col)

    func = _link("CreateGenomicRangeIndex")
    func.restype = c_void_p  # char* that we need to free

    buffer = func(
        c_char_p(table),
        c_char_p(assembly),
        c_int(max_level),
        c_char_p(rid_col),
        c_char_p(beg_col),
        c_char_p(end_col),
    )
    try:
        return _decodestr(buffer)
    finally:
        _sqlite3_free(buffer)


def overlapping_genomic_ranges(
    con: sqlite3.Connection,
    indexed_table: str,
    qrid: Optional[str] = None,
    qbeg: Optional[str] = None,
    qend: Optional[str] = None,
    _join: bool = False,
) -> str:
    assert isinstance(con, sqlite3.Connection)
    assert indexed_table and isinstance(indexed_table, str)
    qrid = _encodestr(qrid)
    qbeg = _encodestr(qbeg)
    qend = _encodestr(qend)

    func = _link(("On" if _join else "") + "OverlappingGenomicRanges")
    func.restype = c_void_p  # char* that we need to free

    buffer = func(
        c_int(_sqlite3_cptr(con)),
        c_char_p(indexed_table.encode()),
        c_char_p(qrid),
        c_char_p(qbeg),
        c_char_p(qend),
    )
    try:
        return _decodestr(buffer)
    finally:
        _sqlite3_free(buffer)


def on_overlapping_genomic_ranges(
    con: sqlite3.Connection, indexed_right_table: str, left_rid: str, left_beg: str, left_end: str,
) -> str:
    return overlapping_genomic_ranges(
        con, indexed_right_table, left_rid, left_beg, left_end, _join=True
    )


def put_reference_assembly(assembly: str, schema: Optional[str] = None) -> str:
    assert isinstance(assembly, str)
    assembly = assembly.encode()

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
    assembly: str,
    length: int,
    refget_id: Optional[str] = None,
    first: bool = True,
    rid: int = -1,
    schema: Optional[str] = None,
):
    assert isinstance(name, str)
    name = name.encode()
    assert isinstance(assembly, str)
    assembly = assembly.encode()
    assert length >= 0
    refget_id = _encodestr(refget_id)

    func = _link("PutReferenceSequence")
    func.restype = c_void_p

    buffer = func(
        c_char_p(name),
        c_char_p(assembly),
        c_char_p(refget_id),
        c_int(length),
        c_int(1 if first else 0),
        c_int(rid),
        c_char_p(schema),
    )
    try:
        return _decodestr(buffer)
    finally:
        _sqlite3_free(buffer)


def _sqlite3_free(buf):
    _link("sqlite3_free", dll="sqlite3")(buf)


def _encodestr(str_or_none):
    if str_or_none is not None:
        assert isinstance(str_or_none, str)
        str_or_none = str_or_none.encode()
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


def _sqlite3_cptr(con: sqlite3.Connection) -> int:
    assert isinstance(con, sqlite3.Connection)
    cptr = c_int(-1)
    # read the sqlite3* from ((char*)&con)+sizeof(PyObject_HEAD)
    # https://github.com/python/cpython/blob/ba1c2c85b39fbcb31584c20f8a63fb87f9cb9c02/Modules/_sqlite/connection.h#L36-L39
    # this is horrible...but succinct!
    memmove(byref(cptr), id(con) + 16, 8)
    return cptr.value


def _sqlite3_cptr_selftest(con):
    lib = cdll.LoadLibrary(find_library("sqlite3"))
    lib.sqlite3_set_last_insert_rowid(c_int(_sqlite3_cptr(con)), c_int(0xBADF00D))
    lib.sqlite3_last_insert_rowid.restype = c_int
    assert lib.sqlite3_last_insert_rowid(c_int(_sqlite3_cptr(con))) == 0xBADF00D


def _init():
    # load C extension
    con = sqlite3.connect(":memory:")
    _sqlite3_cptr_selftest(con)
    con.enable_load_extension(True)
    con.load_extension(find_library("genomicsqlite"))
    # check SQLite version
    check_version = _link("GenomicSQLiteVersionCheck")
    check_version.restype = c_char_p
    version_msg = check_version()
    assert not version_msg, version_msg


_init()
