import os
import random
import sqlite3

import genomicsqlite
import pytest

HERE = os.path.dirname(__file__)
BUILD = os.path.abspath(os.path.join(HERE, "..", "build"))


def test_gri_lvl():
    # Test the _gri_lvl generated column which calculates each feature's level number based on its
    # length.
    con = sqlite3.connect(":memory:")
    con.executescript(
        "CREATE TABLE features(rid INTEGER, beg INTEGER, end INTEGER, expected_lvl INTEGER)"
    )
    for lvl in range(16):
        for ofs in (-2, -1, 0, 1):
            featlen = 16**lvl + ofs
            tup = (420, 420 + featlen, (0 - lvl if ofs < 1 else 0 - lvl - 1))
            con.execute("INSERT INTO features VALUES(42,?,?,?)", tup)
    con.executescript(
        genomicsqlite.create_genomic_range_index_sql(con, "features", "rid", "beg", "end")
    )
    assert (
        next(
            con.execute("SELECT count(*) FROM features WHERE expected_lvl == ifnull(_gri_lvl,999)")
        )[0]
        == 62
    )
    assert (
        next(
            con.execute("SELECT count(*) FROM features WHERE expected_lvl != ifnull(_gri_lvl,999)")
        )[0]
        == 2
    )


@pytest.mark.skipif(
    sqlite3.sqlite_version in ("3.40.0", "3.40.1"), reason="SQLite query planning regression"
)
def test_indexing():
    con = sqlite3.connect(":memory:")
    _fill_exons(con)
    con.commit()

    query = genomicsqlite.genomic_range_rowids_sql(con, "exons")
    query = "SELECT id FROM exons WHERE exons._rowid_ IN\n" + query
    print("\n" + query)

    # The query should only search the relevant GRI levels
    indexed = 0
    for expl in con.execute("EXPLAIN QUERY PLAN " + query, ("chr17", 43044294, 43048294)):
        print(expl[3])
        if "USING INDEX exons__gri" in expl[3]:
            indexed += 1
    assert indexed == 3

    # The query should be covered by the index except for one final fetch of exons.id
    opcodes = list(con.execute("EXPLAIN " + query, ("chr17", 43044294, 43048294)))
    # for expl in opcodes:
    #    if expl[1] in {"OpenRead", "OpenPseudo", "Column"}:
    #        print(expl)
    accessed_cursors = [opcode[2] for opcode in opcodes if opcode[1] == "Column"]
    table_rootpages = {
        row[0] for row in con.execute("SELECT rootpage FROM sqlite_master WHERE type='table'")
    }
    table_cursors = {
        opcode[2] for opcode in opcodes if opcode[1] == "OpenRead" and opcode[3] in table_rootpages
    }
    assert sum(1 for cursor in accessed_cursors if cursor not in table_cursors) > 1  # get from idx
    assert sum(1 for cursor in accessed_cursors if cursor in table_cursors) == 1  # get from table

    # (Also, it should produce correct results)
    control_query = "SELECT id FROM exons NOT INDEXED WHERE rid = ? AND NOT (end < ? OR beg > ?) ORDER BY _rowid_"
    random.seed(0xBADF00D)
    total_results = 0
    for _ in range(50000):
        beg = random.randint(43044294 - 10000, 43044294 + 10000)
        end = beg + random.randint(1, random.choice([10, 100, 1000, 10000]))
        ids = [row[0] for row in con.execute(query, ("chr17", beg, end))]
        control_ids = [row[0] for row in con.execute(control_query, ("chr17", beg, end))]
        assert ids == control_ids
        total_results += len(control_ids)
    assert total_results == 189935

    with pytest.raises(sqlite3.OperationalError):
        genomicsqlite.genomic_range_rowids_sql(con, "nonexistent_table")


@pytest.mark.filterwarnings("error::DeprecationWarning")
def test_rowids_sql_binding_styles():
    # Regression test: the default generated SQL, binding ?1/?2/?3 from a tuple, and the
    # explicitly-named generated SQL, binding :qrid/:qbeg/:qend from a dict, must select
    # identical rows. (The named-placeholder wrapper defaults briefly tried in PR #29 were
    # withdrawn; see there for the CPython bug behind the original motivation.)
    con = sqlite3.connect(":memory:")
    _fill_exons(con)
    con.commit()

    query = genomicsqlite.genomic_range_rowids_sql(con, "exons")
    query = "SELECT id FROM exons WHERE exons._rowid_ IN\n" + query
    tuple_results = list(con.execute(query, ("chr17", 43044294, 43048294)))

    named_query = genomicsqlite.genomic_range_rowids_sql(
        con, "exons", qrid=":qrid", qbeg=":qbeg", qend=":qend"
    )
    named_query = "SELECT id FROM exons WHERE exons._rowid_ IN\n" + named_query
    dict_results = list(
        con.execute(named_query, {"qrid": "chr17", "qbeg": 43044294, "qend": 43048294})
    )

    assert tuple_results
    assert dict_results == tuple_results


def test_abutment():
    # Test GRI query correctness with various cases of features abutting the query range
    con = sqlite3.connect(":memory:")
    con.executescript("CREATE TABLE features(rid INTEGER, beg INTEGER, end INTEGER)")
    pos0 = 10000000000
    for lvl in range(9):
        for ofs in (-1, 0, 1):
            for tup in ((pos0 - 16**lvl, pos0 + ofs), (pos0 + 123 + ofs, pos0 + 123 + 16**9)):
                con.execute("INSERT INTO features VALUES(42,?,?)", tup)
    con.executescript(
        genomicsqlite.create_genomic_range_index_sql(con, "features", "rid", "beg", "end")
    )
    query = genomicsqlite.genomic_range_rowids_sql(
        con, "features", "42", str(pos0), str(pos0 + 123)
    )[1:-1]
    control_query = f"SELECT _rowid_ FROM features NOT INDEXED WHERE rid = 42 AND NOT (end < {pos0} OR beg > {pos0 + 123}) ORDER BY _rowid_"
    assert list(con.execute(query)) == list(con.execute(control_query))


def test_level_detection():
    # test corner cases for the bit of genomic_range_rowids() which detects the level range of
    # extant features

    con = sqlite3.connect(":memory:")
    con.executescript("CREATE TABLE features(rid INTEGER, beg INTEGER, end INTEGER)")
    con.executescript(
        genomicsqlite.create_genomic_range_index_sql(con, "features", "rid", "beg", "end")
    )

    def fanout(query):
        return sum(
            1
            for expl in con.execute("EXPLAIN QUERY PLAN " + query, (None, None, None))
            if "((_gri_rid,_gri_lvl,_gri_beg)>(?,?,?) AND (_gri_rid,_gri_lvl,_gri_beg)<(?,?,?))"
            in expl[3]
        )

    assert fanout(genomicsqlite.genomic_range_rowids_sql(con, "features")[1:-1]) == 16

    con.executescript(
        "INSERT INTO features VALUES(NULL, NULL, NULL); INSERT INTO features VALUES(NULL, 0, 10000000000)"
    )
    assert fanout(genomicsqlite.genomic_range_rowids_sql(con, "features")[1:-1]) == 16
    assert not list(
        con.execute(genomicsqlite.genomic_range_rowids_sql(con, "features")[1:-1], (None, 123, 456))
    )

    con.executescript("INSERT INTO features VALUES(42, 1048568, 1048584)")
    query = genomicsqlite.genomic_range_rowids_sql(con, "features")[1:-1]
    print("\n" + query)
    assert "-1" in query and "-0x10)" in query
    assert not list(
        con.execute(
            genomicsqlite.genomic_range_rowids_sql(con, "features")[1:-1], (42, None, 1048584)
        )
    )

    assert fanout(query) == 1

    con.executescript("""
        INSERT INTO features VALUES(44, 1048568, 1048584);
        INSERT INTO features VALUES(44, 0, 64000)
        """)
    query = genomicsqlite.genomic_range_rowids_sql(con, "features")[1:-1]
    print("\n" + query)
    assert "-4," in query and "-0x10000)" in query
    assert "-3," in query and "-0x1000)" in query
    assert "-2," in query and "-0x100)" in query
    assert "-1," in query and "-0x10)" in query
    assert fanout(query) == 4

    assert fanout(genomicsqlite.genomic_range_rowids_sql(con, "features", ceiling=6)[1:-1]) == 7
    assert (
        fanout(genomicsqlite.genomic_range_rowids_sql(con, "features", ceiling=6, floor=3)[1:-1])
        == 4
    )

    con.executescript("""
        INSERT INTO features VALUES(43, NULL, 10000000000);
        INSERT INTO features VALUES(44, 0, NULL)
        """)
    assert fanout(genomicsqlite.genomic_range_rowids_sql(con, "features")[1:-1]) == 4

    con.executescript("""
        INSERT INTO features VALUES(43, 0, 10000000000);
        INSERT INTO features VALUES(43, 32, 33)
        """)
    query = genomicsqlite.genomic_range_rowids_sql(con, "features")[1:-1]
    print("\n" + query)
    assert fanout(query) == 10

    con.executescript("""
        INSERT INTO features VALUES(43, 0, 10000000000);
        INSERT INTO features VALUES(43, 32, 32)
        """)
    query = genomicsqlite.genomic_range_rowids_sql(con, "features")[1:-1]
    assert fanout(query) == 10
    assert len(list(con.execute(query, (43, 32, 33)))) == 4
    assert len(list(con.execute(query, (43, 33, 33)))) == 3


def test_refseq():
    con = sqlite3.connect(":memory:")

    create_assembly = genomicsqlite.put_reference_assembly_sql(con, "GRCh38_no_alt_analysis_set")
    lines = create_assembly.strip().split("\n")
    print("\n".join([line for line in lines if "INSERT INTO" in line][:24]))
    assert len([line for line in lines if "INSERT INTO" in line]) == 195
    print("\n".join([line for line in lines if "INSERT INTO" not in line]))
    assert len([line for line in lines if "INSERT INTO" not in line]) == 2
    con.executescript(create_assembly)

    _fill_exons(con, floor=2)
    con.commit()

    refseq_by_rid = genomicsqlite.get_reference_sequences_by_rid(con)
    refseq_by_name = genomicsqlite.get_reference_sequences_by_name(con)
    for refseq in refseq_by_rid.values():
        assert refseq_by_rid[refseq.rid] == refseq
        assert refseq_by_name[refseq.name] == refseq
        if refseq.name == "chr17":
            assert refseq.rid == 17
            assert refseq.length == 83257441
            assert refseq.assembly == "GRCh38_no_alt_analysis_set"
            assert refseq.refget_id == "f9a0fb01553adb183568e3eb9d8626db"
    assert len(refseq_by_rid) == 195

    query = (
        "SELECT _gri_refseq._gri_rid, rid, beg, end, id FROM exons, _gri_refseq WHERE exons.rid = gri_refseq_name AND exons._rowid_ IN "
        + genomicsqlite.genomic_range_rowids_sql(con, "exons")
    )
    print("\n" + query)
    assert len([line for line in query.split("\n") if "BETWEEN" in line]) == 2
    assert len(list(con.execute(query, ("chr17", 43115725, 43125370)))) == 56


def test_join():
    for len_gri in (False, True):
        con = sqlite3.connect(":memory:")
        con.executescript(
            genomicsqlite.put_reference_assembly_sql(con, "GRCh38_no_alt_analysis_set")
        )
        _fill_exons(con, table="exons")
        _fill_exons(con, floor=2, table="exons2", len_gri=len_gri)
        con.commit()

        query = (
            "SELECT exons.id, exons2.id FROM exons LEFT JOIN exons2 ON exons2._rowid_ IN\n"
            + genomicsqlite.genomic_range_rowids_sql(
                con, "exons2", "exons.rid", "exons.beg", "exons.end"
            )
            + " AND exons.id != exons2.id ORDER BY exons.id, exons2.id"
        )
        print(query)
        indexed = 0
        for expl in con.execute("EXPLAIN QUERY PLAN " + query):
            print(expl[3])
            if (
                "((_gri_rid,_gri_lvl,_gri_beg)>(?,?,?) AND (_gri_rid,_gri_lvl,_gri_beg)<(?,?,?))"
                in expl[3]
            ):
                indexed += 1
        assert indexed == 2
        results = list(con.execute(query))
        assert len(results) == 5191
        assert len([result for result in results if result[1] is None]) == 5
        control = "SELECT exons.id, exons2.id FROM exons LEFT JOIN exons2 NOT INDEXED ON NOT (exons2.end < exons.beg OR exons2.beg > exons.end) AND exons.id != exons2.id ORDER BY exons.id, exons2.id"
        control = list(con.execute(control))
        assert results == control


def test_connect(tmp_path):
    dbfile = str(tmp_path / "test.gsql")
    con = genomicsqlite.connect(dbfile, unsafe_load=True)
    con.executescript(genomicsqlite.put_reference_assembly_sql(con, "GRCh38_no_alt_analysis_set"))
    _fill_exons(con)
    con.commit()
    con.close()

    con = genomicsqlite.connect(dbfile, read_only=True)
    query = (
        "WITH exons2 AS (SELECT * from exons) SELECT exons.id, exons2.id FROM exons2 LEFT JOIN exons ON exons._rowid_ IN\n"
        + genomicsqlite.genomic_range_rowids_sql(
            con, "exons", "exons2.rid", "exons2.beg", "exons2.end"
        )
        + " AND exons.id != exons2.id ORDER BY exons.id, exons2.id"
    )
    results = list(con.execute(query))
    assert len(results) == 5191


def test_attach(tmp_path):
    dbfile = str(tmp_path / "test.gsql")
    con = genomicsqlite.connect(dbfile, unsafe_load=True)
    _fill_exons(con, gri=False)
    con.commit()

    attach_script = genomicsqlite.attach_sql(
        con, str(tmp_path / "test_attached.gsql"), "db2", unsafe_load=True
    )
    con.executescript(attach_script)
    con.executescript("CREATE TABLE db2.exons2 AS SELECT * FROM exons")
    con.executescript(
        genomicsqlite.create_genomic_range_index_sql(con, "db2.exons2", "rid", "beg", "end")
    )
    ref_script = genomicsqlite.put_reference_assembly_sql(
        con, "GRCh38_no_alt_analysis_set", schema="db2"
    )
    con.executescript(ref_script)

    query = (
        "SELECT exons.id, db2.exons2.id FROM exons LEFT JOIN db2.exons2 ON db2.exons2._rowid_ IN\n"
        + genomicsqlite.genomic_range_rowids_sql(
            con, "db2.exons2", "exons.rid", "exons.beg", "exons.end"
        )
        + " AND exons.id != db2.exons2.id ORDER BY exons.id, db2.exons2.id"
    )
    results = list(con.execute(query))
    assert len(results) == 5191

    refseq_by_name = genomicsqlite.get_reference_sequences_by_name(con, schema="db2")
    assert len(refseq_by_name) > 24


def test_gri_levels_in_sql(tmp_path):
    dbfile = str(tmp_path / "test.gsql")
    con = genomicsqlite.connect(dbfile, unsafe_load=True)
    _fill_exons(con)
    con.commit()

    # test caching & invalidation:
    results = list(con.execute("SELECT * FROM genomic_range_index_levels('exons')"))
    assert results == [(3, 1)]
    results = list(con.execute("SELECT * FROM genomic_range_index_levels('exons')"))
    assert results == [(3, 1)]
    results = list(con.execute("SELECT * FROM genomic_range_index_levels('main.exons')"))
    assert results == [(3, 1)]
    tch1 = con.total_changes
    con.execute("INSERT INTO exons VALUES('ether',0,4097,4097,'ether')")
    tch2 = con.total_changes
    assert tch2 > tch1
    results = list(con.execute("SELECT * FROM genomic_range_index_levels('exons')"))
    assert results == [(4, 1)]
    con.commit()
    results = list(con.execute("SELECT * FROM genomic_range_index_levels('exons')"))
    assert results == [(4, 1)]
    con.execute("DELETE FROM exons WHERE rid = 'ether'")
    con.commit()
    results = list(con.execute("SELECT * FROM genomic_range_index_levels('main.exons')"))
    assert results == [(3, 1)]
    results = list(con.execute("SELECT * FROM genomic_range_index_levels('exons')"))
    assert results == [(3, 1)]

    with pytest.raises(sqlite3.OperationalError, match="no such table"):
        con.execute("SELECT * FROM genomic_range_index_levels('nonexistent')")

    con.executescript("CREATE TABLE empty(rid TEXT, beg INTEGER, end INTEGER)")
    with pytest.raises(sqlite3.OperationalError, match="missing genomic range index"):
        con.execute("SELECT _gri_ceiling, _gri_floor FROM genomic_range_index_levels('empty')")

    con.executescript(
        genomicsqlite.create_genomic_range_index_sql(con, "empty", "rid", "beg", "end")
    )
    results = list(
        con.execute("SELECT _gri_ceiling, _gri_floor FROM genomic_range_index_levels('empty')")
    )
    assert results == [(15, 0)]


def _levels(con, name="t"):
    return next(iter(con.execute(f"SELECT * FROM genomic_range_index_levels('{name}')")))


def _levels_indexed(con, name="t"):
    # the documented levels-fed pattern: bounds from genomic_range_index_levels() fed into
    # genomic_range_rowids()
    ceiling, floor = _levels(con, name)
    return sorted(
        row[0]
        for row in con.execute(
            f"SELECT * FROM genomic_range_rowids('{name}','chr1',500,501,?,?)", (ceiling, floor)
        )
    )


def _levels_control(con, name="t"):
    return sorted(
        row[0]
        for row in con.execute(
            f"SELECT rowid FROM {name} NOT INDEXED"
            " WHERE rid='chr1' AND NOT (end < 500 OR beg > 501)"
        )
    )


def _fill_levels_fixture(con, qname="t"):
    # two rows whose genomic range index levels are (ceiling, floor) = (3, 0); the range query
    # chr1:500-501 overlaps only the first row
    con.execute(f"CREATE TABLE {qname}(rid TEXT, beg INTEGER, end INTEGER)")
    con.executescript(genomicsqlite.create_genomic_range_index_sql(con, qname, "rid", "beg", "end"))
    con.execute(f"INSERT INTO {qname} VALUES('chr1',0,1000)")
    con.execute(f"INSERT INTO {qname} VALUES('chr1',5000,5001)")


def _fill_temp_levels_fixture(con):
    # temp table with the same name as the fixture's main table, holding only a level-0 row
    con.execute("CREATE TEMP TABLE t(rid TEXT, beg INTEGER, end INTEGER)")
    con.executescript(
        genomicsqlite.create_genomic_range_index_sql(con, "temp.t", "rid", "beg", "end")
    )
    con.execute("INSERT INTO temp.t VALUES('chr1',100,101)")


def test_gri_levels_cache_rollback(tmp_path):
    # Levels computed over uncommitted writes must never be served after those writes are
    # rolled back, whether by ROLLBACK TO a savepoint or by a full ROLLBACK.
    con = genomicsqlite.connect(str(tmp_path / "test.gsql"), isolation_level=None)
    _fill_levels_fixture(con)
    assert _levels(con) == (3, 0)
    assert _levels_indexed(con) == _levels_control(con) == [1]

    con.execute("BEGIN")
    con.execute("SAVEPOINT s")
    con.execute("DELETE FROM t WHERE beg=0")
    assert _levels(con) == (0, 0)  # levels of the in-transaction snapshot
    con.execute("ROLLBACK TO SAVEPOINT s")
    assert _levels(con) == (3, 0)
    assert _levels_indexed(con) == _levels_control(con) == [1]
    con.execute("RELEASE s")
    assert _levels(con) == (3, 0)
    con.execute("COMMIT")
    assert _levels(con) == (3, 0)
    assert _levels_indexed(con) == _levels_control(con) == [1]

    con.execute("BEGIN")
    con.execute("DELETE FROM t WHERE beg=0")
    assert _levels(con) == (0, 0)
    con.execute("ROLLBACK")
    assert _levels(con) == (3, 0)
    assert _levels_indexed(con) == _levels_control(con) == [1]
    con.close()


def test_gri_levels_cache_bare_savepoint(tmp_path):
    # A bare SAVEPOINT starts a transaction that RELEASE commits. Its uncommitted
    # levels must not be served after ROLLBACK TO restores the rows.
    con = genomicsqlite.connect(str(tmp_path / "test.gsql"), isolation_level=None)
    _fill_levels_fixture(con)
    assert _levels(con) == (3, 0)

    con.execute("SAVEPOINT s")
    con.execute("DELETE FROM t WHERE beg=0")
    assert _levels(con) == (0, 0)
    con.execute("ROLLBACK TO SAVEPOINT s")
    assert _levels(con) == (3, 0)
    assert _levels_indexed(con) == _levels_control(con) == [1]
    con.execute("RELEASE s")
    assert _levels(con) == (3, 0)
    assert _levels_indexed(con) == _levels_control(con) == [1]
    con.close()


def test_gri_levels_cache_cross_connection(tmp_path):
    # Another connection's committed writes still invalidate this connection's cached levels.
    # (Plain connections: a GenomicSQLite database file admits only one writer connection.)
    dbfile = str(tmp_path / "test.db")
    con1 = sqlite3.connect(dbfile, isolation_level=None)
    _fill_levels_fixture(con1)
    assert _levels(con1) == (3, 0)

    con2 = sqlite3.connect(dbfile, isolation_level=None)
    con2.execute("DELETE FROM t WHERE beg=0")
    assert _levels(con1) == (0, 0)
    assert _levels_indexed(con1) == _levels_control(con1) == []
    con1.close()
    con2.close()


def test_gri_levels_cache_temp_table(tmp_path):
    # An unqualified name resolves to a same-named temp table; consulting it must not disturb
    # the main table's levels.
    con = genomicsqlite.connect(str(tmp_path / "test.gsql"), isolation_level=None)
    _fill_levels_fixture(con)
    _fill_temp_levels_fixture(con)

    assert _levels(con, "t") == (0, 0)  # resolves to temp.t; temp's own levels
    assert _levels(con, "temp.t") == (0, 0)
    assert _levels(con, "main.t") == (3, 0)
    assert _levels_indexed(con, "main.t") == _levels_control(con, "main.t") == [1]
    con.close()


def test_gri_levels_cache_temp_shadowing(tmp_path):
    # Mirror image: main's levels are consulted first, then a temp table shadows the name.
    con = genomicsqlite.connect(str(tmp_path / "test.gsql"), isolation_level=None)
    _fill_levels_fixture(con)
    assert _levels(con) == (3, 0)
    _fill_temp_levels_fixture(con)

    assert _levels(con, "t") == (0, 0)  # now resolves to temp.t
    assert _levels(con, "main.t") == (3, 0)
    assert _levels_indexed(con, "main.t") == _levels_control(con, "main.t") == [1]
    con.close()


def test_gri_levels_cache_attached(tmp_path):
    # Levels for an attached database's table, consulted through the unqualified name, must
    # not be served for a different file later attached under the same schema name.
    aux1 = str(tmp_path / "aux1.gsql")
    con = genomicsqlite.connect(aux1, isolation_level=None)
    con.execute("CREATE TABLE t(rid TEXT, beg INTEGER, end INTEGER)")
    con.executescript(genomicsqlite.create_genomic_range_index_sql(con, "t", "rid", "beg", "end"))
    con.execute("INSERT INTO t VALUES('chr1',100,101)")  # levels (0, 0)
    con.close()

    aux2 = str(tmp_path / "aux2.gsql")
    con = genomicsqlite.connect(aux2, isolation_level=None)
    _fill_levels_fixture(con)  # levels (3, 0)
    con.close()

    con = genomicsqlite.connect(str(tmp_path / "test.gsql"), isolation_level=None)
    con.execute(f"ATTACH '{aux1}' AS aux")
    assert _levels(con, "t") == (0, 0)  # main has no t; resolves to aux.t
    con.execute("DETACH aux")
    con.execute(f"ATTACH '{aux2}' AS aux")
    assert _levels(con, "aux.t") == (3, 0)
    assert _levels(con, "t") == (3, 0)
    assert _levels_indexed(con, "aux.t") == _levels_control(con, "aux.t") == [1]
    con.close()


def test_gri_levels_cache_trigger_abort(tmp_path):
    # Trigger queries see the DELETE's uncommitted snapshot even in autocommit mode.
    # Repeated reads must not cache it: RAISE(ABORT) restores the rows without
    # advancing either validity counter.
    con = sqlite3.connect(str(tmp_path / "test.db"), isolation_level=None)
    _fill_levels_fixture(con)
    assert _levels(con) == (3, 0)
    assert _levels_indexed(con) == _levels_control(con) == [1]

    # The trigger-creation commit invalidates the warmed entry, forcing an in-trigger computation.
    con.execute(
        "CREATE TRIGGER trg AFTER DELETE ON t BEGIN"
        " SELECT * FROM genomic_range_index_levels('t');"
        " SELECT * FROM genomic_range_index_levels('t');"
        " SELECT RAISE(ABORT, 'boom');"
        " END"
    )
    with pytest.raises(sqlite3.Error, match="boom"):
        con.execute("DELETE FROM t WHERE beg=0")
    assert con.execute("SELECT count(*) FROM t").fetchone()[0] == 2  # DELETE rolled back
    assert _levels(con) == (3, 0)
    assert _levels_indexed(con) == _levels_control(con) == [1]
    con.close()


@pytest.mark.skipif(sqlite3.sqlite_version_info < (3, 35), reason="RETURNING requires SQLite 3.35+")
def test_gri_levels_cache_trigger_abort_returning(tmp_path):
    # This DELETE ... RETURNING aborts in its trigger before returning any rows.
    # Its trigger queries must still bypass the cache.
    con = sqlite3.connect(str(tmp_path / "test.db"), isolation_level=None)
    _fill_levels_fixture(con)
    assert _levels(con) == (3, 0)

    con.execute(
        "CREATE TRIGGER trg AFTER DELETE ON t BEGIN"
        " SELECT * FROM genomic_range_index_levels('t');"
        " SELECT * FROM genomic_range_index_levels('t');"
        " SELECT RAISE(ABORT, 'boom');"
        " END"
    )
    with pytest.raises(sqlite3.Error, match="boom"):
        con.execute("DELETE FROM t WHERE beg=0 RETURNING rowid").fetchall()
    assert con.execute("SELECT count(*) FROM t").fetchone()[0] == 2
    assert _levels(con) == (3, 0)
    assert _levels_indexed(con) == _levels_control(con) == [1]
    con.close()


def test_gri_levels_cache_trigger_abort_view(tmp_path):
    # Reading the levels through a view must still detect the enclosing DELETE
    # and bypass the cache inside its trigger.
    con = sqlite3.connect(str(tmp_path / "test.db"), isolation_level=None)
    _fill_levels_fixture(con)
    con.execute("CREATE VIEW gri_levels AS SELECT * FROM genomic_range_index_levels('t')")
    assert _levels(con) == (3, 0)

    con.execute(
        "CREATE TRIGGER trg AFTER DELETE ON t BEGIN"
        " SELECT * FROM gri_levels;"
        " SELECT * FROM gri_levels;"
        " SELECT RAISE(ABORT, 'boom');"
        " END"
    )
    with pytest.raises(sqlite3.Error, match="boom"):
        con.execute("DELETE FROM t WHERE beg=0")
    assert con.execute("SELECT count(*) FROM t").fetchone()[0] == 2
    assert _levels(con) == (3, 0)
    assert _levels_indexed(con) == _levels_control(con) == [1]
    con.close()


def test_gri_levels_cache_nocase_override(tmp_path):
    # SQLite's identifier comparison is fixed; the SQL NOCASE collation is replaceable.
    # With case-sensitive NOCASE, a SQL name comparison would miss temp.t for 'T',
    # misclassify it as main.T, and cache temp's levels under main's key.
    con = sqlite3.connect(str(tmp_path / "test.db"), isolation_level=None)
    con.create_collation("NOCASE", lambda a, b: (a > b) - (a < b))  # case-sensitive
    _fill_levels_fixture(con, "T")
    _fill_temp_levels_fixture(con)

    assert _levels(con, "T") == (0, 0)  # unqualified: resolves to temp.t, as SQLite resolves it
    assert _levels(con, "main.T") == (3, 0)
    assert _levels_indexed(con, "main.T") == _levels_control(con, "main.T") == [1]
    con.close()


def test_gri_levels_cache_temp_shadow_after_warm(tmp_path):
    # Creating a same-named temp table moves neither validity counter (DDL is not DML, and
    # the temp schema lives in a different file), so only re-resolving the name when the
    # cache is consulted keeps the unqualified call from serving main's warmed entry.
    con = sqlite3.connect(str(tmp_path / "test.db"), isolation_level=None)
    _fill_levels_fixture(con)
    assert _levels(con) == (3, 0)  # warms main's cache entry

    con.execute("CREATE TEMP TABLE t(rid TEXT, beg INTEGER, end INTEGER)")
    con.executescript(
        genomicsqlite.create_genomic_range_index_sql(con, "temp.t", "rid", "beg", "end")
    )
    assert _levels(con, "t") == (15, 0)  # the empty temp table's levels, not main's (3, 0)
    assert _levels(con, "main.t") == (3, 0)
    assert _levels_indexed(con, "main.t") == _levels_control(con, "main.t") == [1]
    con.close()


def test_query_in_sql(tmp_path):
    dbfile = str(tmp_path / "test.gsql")
    con = genomicsqlite.connect(dbfile, unsafe_load=True)
    _fill_exons(con)
    con.commit()

    query = "SELECT id FROM exons WHERE exons._rowid_ IN genomic_range_rowids('exons',?,?,?)"
    results = list(con.execute(query, ("chr17", 43044294, 43048294)))

    control_query = genomicsqlite.genomic_range_rowids_sql(con, "exons")
    control_query = "SELECT id FROM exons WHERE exons._rowid_ IN\n" + control_query
    control_results = list(con.execute(control_query, ("chr17", 43044294, 43048294)))

    assert results == control_results

    for expl in con.execute(
        "EXPLAIN QUERY PLAN SELECT _rowid_ FROM genomic_range_rowids('exons',?,?,?) ORDER BY _rowid_",
        ("chr17", 43044294, 43048294),
    ):
        assert "USE TEMP B-TREE FOR ORDER BY" not in expl[3]

    assert next(
        expl[3]
        for expl in con.execute(
            "EXPLAIN QUERY PLAN SELECT _rowid_ FROM genomic_range_rowids('exons',?,?,?) ORDER BY _rowid_ DESC",
            ("chr17", 43044294, 43048294),
        )
        if "USE TEMP B-TREE FOR ORDER BY" in expl[3]
    )

    with pytest.raises(sqlite3.OperationalError, match="domain error"):
        con.execute(
            "SELECT * FROM genomic_range_rowids('exons', 'chr17', 43044294, 43048294, 16, 0)"
        )

    dbfile2 = str(tmp_path / "test2.gsql")
    con2 = genomicsqlite.connect(dbfile2, unsafe_load=True)
    _fill_exons(con2)
    con2.commit()
    con2.close()

    con.executescript(genomicsqlite.attach_sql(con, dbfile2, "db2", immutable=True))
    query = """
        SELECT main.exons.id, db2.exons.id
            FROM main.exons LEFT JOIN db2.exons ON
            db2.exons._rowid_ IN genomic_range_rowids('db2.exons', main.exons.rid, main.exons.beg, main.exons.end)
            AND main.exons.id != db2.exons.id ORDER BY main.exons.id, db2.exons.id
        """
    results = list(con.execute(query))
    assert len(results) == 5191

    with pytest.raises(sqlite3.OperationalError, match="no such table"):
        con.execute(
            "SELECT * FROM genomic_range_rowids('nonexistent', 'chr17', 43044294, 43048294)"
        )

    con.executescript("CREATE TABLE empty(rid TEXT, beg INTEGER, end INTEGER)")
    with pytest.raises(sqlite3.OperationalError, match="no such index"):
        con.execute("SELECT * FROM genomic_range_rowids('empty', 'chr17', 43044294, 43048294)")

    con.executescript(
        genomicsqlite.create_genomic_range_index_sql(con, "empty", "rid", "beg", "end")
    )
    results = list(
        con.execute("SELECT * FROM genomic_range_rowids('empty', 'chr17', 43044294, 43048294)")
    )
    assert not results


def _fill_exons(con, floor=None, table="exons", gri=True, len_gri=False):
    con.execute(
        f"CREATE TABLE {table}(rid TEXT NOT NULL, beg INTEGER NOT NULL, end INTEGER NOT NULL, len INTEGER NOT NULL, id TEXT NOT NULL)"
    )
    for line in _EXONS.strip().split("\n"):
        line = line.split("\t")
        con.execute(
            f"INSERT INTO {table}(rid,beg,end,len,id) VALUES(?,?,?,?,?)",
            (line[0], int(line[1]) - 1, int(line[2]), int(line[2]) - int(line[1]) + 1, line[3]),
        )
    if gri:
        con.executescript(
            genomicsqlite.create_genomic_range_index_sql(
                con, table, "rid", "beg", ("beg+len" if len_gri else "end"), floor=floor
            )
        )


_EXONS = """
chr17	43044294	43045802	ENST00000352993.7_exon_0_0_chr17_43044295_r	0	-
chr17	43047642	43047703	ENST00000352993.7_exon_1_0_chr17_43047643_r	0	-
chr17	43049120	43049194	ENST00000352993.7_exon_2_0_chr17_43049121_r	0	-
chr17	43051062	43051117	ENST00000352993.7_exon_3_0_chr17_43051063_r	0	-
chr17	43057051	43057135	ENST00000352993.7_exon_4_0_chr17_43057052_r	0	-
chr17	43063332	43063373	ENST00000352993.7_exon_5_0_chr17_43063333_r	0	-
chr17	43063873	43063951	ENST00000352993.7_exon_6_0_chr17_43063874_r	0	-
chr17	43067607	43067695	ENST00000352993.7_exon_7_0_chr17_43067608_r	0	-
chr17	43070927	43071238	ENST00000352993.7_exon_8_0_chr17_43070928_r	0	-
chr17	43074330	43074521	ENST00000352993.7_exon_9_0_chr17_43074331_r	0	-
chr17	43076487	43076614	ENST00000352993.7_exon_10_0_chr17_43076488_r	0	-
chr17	43082403	43082575	ENST00000352993.7_exon_11_0_chr17_43082404_r	0	-
chr17	43090943	43091032	ENST00000352993.7_exon_12_0_chr17_43090944_r	0	-
chr17	43095845	43095922	ENST00000352993.7_exon_13_0_chr17_43095846_r	0	-
chr17	43097243	43097289	ENST00000352993.7_exon_14_0_chr17_43097244_r	0	-
chr17	43099774	43099880	ENST00000352993.7_exon_15_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000352993.7_exon_16_0_chr17_43104122_r	0	-
chr17	43104867	43104956	ENST00000352993.7_exon_17_0_chr17_43104868_r	0	-
chr17	43106455	43106533	ENST00000352993.7_exon_18_0_chr17_43106456_r	0	-
chr17	43115725	43115779	ENST00000352993.7_exon_19_0_chr17_43115726_r	0	-
chr17	43124016	43124115	ENST00000352993.7_exon_20_0_chr17_43124017_r	0	-
chr17	43125270	43125370	ENST00000352993.7_exon_21_0_chr17_43125271_r	0	-
chr17	43044294	43045802	ENST00000357654.8_exon_0_0_chr17_43044295_r	0	-
chr17	43047642	43047703	ENST00000357654.8_exon_1_0_chr17_43047643_r	0	-
chr17	43049120	43049194	ENST00000357654.8_exon_2_0_chr17_43049121_r	0	-
chr17	43051062	43051117	ENST00000357654.8_exon_3_0_chr17_43051063_r	0	-
chr17	43057051	43057135	ENST00000357654.8_exon_4_0_chr17_43057052_r	0	-
chr17	43063332	43063373	ENST00000357654.8_exon_5_0_chr17_43063333_r	0	-
chr17	43063873	43063951	ENST00000357654.8_exon_6_0_chr17_43063874_r	0	-
chr17	43067607	43067695	ENST00000357654.8_exon_7_0_chr17_43067608_r	0	-
chr17	43070927	43071238	ENST00000357654.8_exon_8_0_chr17_43070928_r	0	-
chr17	43074330	43074521	ENST00000357654.8_exon_9_0_chr17_43074331_r	0	-
chr17	43076487	43076614	ENST00000357654.8_exon_10_0_chr17_43076488_r	0	-
chr17	43082403	43082575	ENST00000357654.8_exon_11_0_chr17_43082404_r	0	-
chr17	43090943	43091032	ENST00000357654.8_exon_12_0_chr17_43090944_r	0	-
chr17	43091434	43094860	ENST00000357654.8_exon_13_0_chr17_43091435_r	0	-
chr17	43095845	43095922	ENST00000357654.8_exon_14_0_chr17_43095846_r	0	-
chr17	43097243	43097289	ENST00000357654.8_exon_15_0_chr17_43097244_r	0	-
chr17	43099774	43099880	ENST00000357654.8_exon_16_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000357654.8_exon_17_0_chr17_43104122_r	0	-
chr17	43104867	43104956	ENST00000357654.8_exon_18_0_chr17_43104868_r	0	-
chr17	43106455	43106533	ENST00000357654.8_exon_19_0_chr17_43106456_r	0	-
chr17	43115725	43115779	ENST00000357654.8_exon_20_0_chr17_43115726_r	0	-
chr17	43124016	43124115	ENST00000357654.8_exon_21_0_chr17_43124017_r	0	-
chr17	43125270	43125483	ENST00000357654.8_exon_22_0_chr17_43125271_r	0	-
chr17	43044294	43045802	ENST00000471181.7_exon_0_0_chr17_43044295_r	0	-
chr17	43047642	43047703	ENST00000471181.7_exon_1_0_chr17_43047643_r	0	-
chr17	43049120	43049194	ENST00000471181.7_exon_2_0_chr17_43049121_r	0	-
chr17	43051062	43051117	ENST00000471181.7_exon_3_0_chr17_43051063_r	0	-
chr17	43057051	43057135	ENST00000471181.7_exon_4_0_chr17_43057052_r	0	-
chr17	43063332	43063373	ENST00000471181.7_exon_5_0_chr17_43063333_r	0	-
chr17	43063873	43063951	ENST00000471181.7_exon_6_0_chr17_43063874_r	0	-
chr17	43067607	43067695	ENST00000471181.7_exon_7_0_chr17_43067608_r	0	-
chr17	43070927	43071238	ENST00000471181.7_exon_8_0_chr17_43070928_r	0	-
chr17	43074330	43074521	ENST00000471181.7_exon_9_0_chr17_43074331_r	0	-
chr17	43076487	43076611	ENST00000471181.7_exon_10_0_chr17_43076488_r	0	-
chr17	43079333	43079399	ENST00000471181.7_exon_11_0_chr17_43079334_r	0	-
chr17	43082403	43082575	ENST00000471181.7_exon_12_0_chr17_43082404_r	0	-
chr17	43090943	43091032	ENST00000471181.7_exon_13_0_chr17_43090944_r	0	-
chr17	43091434	43094860	ENST00000471181.7_exon_14_0_chr17_43091435_r	0	-
chr17	43095845	43095922	ENST00000471181.7_exon_15_0_chr17_43095846_r	0	-
chr17	43097243	43097289	ENST00000471181.7_exon_16_0_chr17_43097244_r	0	-
chr17	43099774	43099880	ENST00000471181.7_exon_17_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000471181.7_exon_18_0_chr17_43104122_r	0	-
chr17	43104867	43104956	ENST00000471181.7_exon_19_0_chr17_43104868_r	0	-
chr17	43106455	43106533	ENST00000471181.7_exon_20_0_chr17_43106456_r	0	-
chr17	43115725	43115779	ENST00000471181.7_exon_21_0_chr17_43115726_r	0	-
chr17	43124016	43124115	ENST00000471181.7_exon_22_0_chr17_43124017_r	0	-
chr17	43125270	43125483	ENST00000471181.7_exon_23_0_chr17_43125271_r	0	-
chr17	43044804	43045802	ENST00000468300.5_exon_0_0_chr17_43044805_r	0	-
chr17	43047642	43047703	ENST00000468300.5_exon_1_0_chr17_43047643_r	0	-
chr17	43051062	43051117	ENST00000468300.5_exon_2_0_chr17_43051063_r	0	-
chr17	43057051	43057135	ENST00000468300.5_exon_3_0_chr17_43057052_r	0	-
chr17	43063332	43063373	ENST00000468300.5_exon_4_0_chr17_43063333_r	0	-
chr17	43063873	43063951	ENST00000468300.5_exon_5_0_chr17_43063874_r	0	-
chr17	43067607	43067695	ENST00000468300.5_exon_6_0_chr17_43067608_r	0	-
chr17	43070927	43071238	ENST00000468300.5_exon_7_0_chr17_43070928_r	0	-
chr17	43074330	43074521	ENST00000468300.5_exon_8_0_chr17_43074331_r	0	-
chr17	43076487	43076611	ENST00000468300.5_exon_9_0_chr17_43076488_r	0	-
chr17	43082403	43082575	ENST00000468300.5_exon_10_0_chr17_43082404_r	0	-
chr17	43090943	43091032	ENST00000468300.5_exon_11_0_chr17_43090944_r	0	-
chr17	43094743	43094860	ENST00000468300.5_exon_12_0_chr17_43094744_r	0	-
chr17	43095845	43095922	ENST00000468300.5_exon_13_0_chr17_43095846_r	0	-
chr17	43097243	43097289	ENST00000468300.5_exon_14_0_chr17_43097244_r	0	-
chr17	43099774	43099880	ENST00000468300.5_exon_15_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000468300.5_exon_16_0_chr17_43104122_r	0	-
chr17	43104867	43104956	ENST00000468300.5_exon_17_0_chr17_43104868_r	0	-
chr17	43106455	43106533	ENST00000468300.5_exon_18_0_chr17_43106456_r	0	-
chr17	43115725	43115779	ENST00000468300.5_exon_19_0_chr17_43115726_r	0	-
chr17	43124016	43124115	ENST00000468300.5_exon_20_0_chr17_43124017_r	0	-
chr17	43125276	43125451	ENST00000468300.5_exon_21_0_chr17_43125277_r	0	-
chr17	43045085	43045802	ENST00000644379.1_exon_0_0_chr17_43045086_r	0	-
chr17	43047642	43047703	ENST00000644379.1_exon_1_0_chr17_43047643_r	0	-
chr17	43049120	43049194	ENST00000644379.1_exon_2_0_chr17_43049121_r	0	-
chr17	43051062	43051117	ENST00000644379.1_exon_3_0_chr17_43051063_r	0	-
chr17	43057051	43057135	ENST00000644379.1_exon_4_0_chr17_43057052_r	0	-
chr17	43063332	43063373	ENST00000644379.1_exon_5_0_chr17_43063333_r	0	-
chr17	43063873	43063951	ENST00000644379.1_exon_6_0_chr17_43063874_r	0	-
chr17	43067607	43067695	ENST00000644379.1_exon_7_0_chr17_43067608_r	0	-
chr17	43070927	43071238	ENST00000644379.1_exon_8_0_chr17_43070928_r	0	-
chr17	43074330	43074521	ENST00000644379.1_exon_9_0_chr17_43074331_r	0	-
chr17	43076487	43076614	ENST00000644379.1_exon_10_0_chr17_43076488_r	0	-
chr17	43079333	43079399	ENST00000644379.1_exon_11_0_chr17_43079334_r	0	-
chr17	43082403	43082575	ENST00000644379.1_exon_12_0_chr17_43082404_r	0	-
chr17	43090943	43091032	ENST00000644379.1_exon_13_0_chr17_43090944_r	0	-
chr17	43091434	43091851	ENST00000644379.1_exon_14_0_chr17_43091435_r	0	-
chr17	43045562	43045802	ENST00000586385.5_exon_0_0_chr17_43045563_r	0	-
chr17	43047642	43047703	ENST00000586385.5_exon_1_0_chr17_43047643_r	0	-
chr17	43049120	43049194	ENST00000586385.5_exon_2_0_chr17_43049121_r	0	-
chr17	43051062	43051117	ENST00000586385.5_exon_3_0_chr17_43051063_r	0	-
chr17	43057051	43057135	ENST00000586385.5_exon_4_0_chr17_43057052_r	0	-
chr17	43063332	43063373	ENST00000586385.5_exon_5_0_chr17_43063333_r	0	-
chr17	43063873	43063951	ENST00000586385.5_exon_6_0_chr17_43063874_r	0	-
chr17	43125181	43125329	ENST00000586385.5_exon_7_0_chr17_43125182_r	0	-
chr17	43045562	43045802	ENST00000591534.5_exon_0_0_chr17_43045563_r	0	-
chr17	43047642	43047703	ENST00000591534.5_exon_1_0_chr17_43047643_r	0	-
chr17	43049120	43049194	ENST00000591534.5_exon_2_0_chr17_43049121_r	0	-
chr17	43051062	43051117	ENST00000591534.5_exon_3_0_chr17_43051063_r	0	-
chr17	43057051	43057135	ENST00000591534.5_exon_4_0_chr17_43057052_r	0	-
chr17	43063332	43063373	ENST00000591534.5_exon_5_0_chr17_43063333_r	0	-
chr17	43063873	43063951	ENST00000591534.5_exon_6_0_chr17_43063874_r	0	-
chr17	43067607	43067695	ENST00000591534.5_exon_7_0_chr17_43067608_r	0	-
chr17	43070927	43071238	ENST00000591534.5_exon_8_0_chr17_43070928_r	0	-
chr17	43074330	43074521	ENST00000591534.5_exon_9_0_chr17_43074331_r	0	-
chr17	43125270	43125329	ENST00000591534.5_exon_10_0_chr17_43125271_r	0	-
chr17	43045562	43045802	ENST00000591849.5_exon_0_0_chr17_43045563_r	0	-
chr17	43047642	43047703	ENST00000591849.5_exon_1_0_chr17_43047643_r	0	-
chr17	43049120	43049194	ENST00000591849.5_exon_2_0_chr17_43049121_r	0	-
chr17	43050061	43050190	ENST00000591849.5_exon_3_0_chr17_43050062_r	0	-
chr17	43125270	43125329	ENST00000591849.5_exon_4_0_chr17_43125271_r	0	-
chr17	43045628	43045802	ENST00000493795.5_exon_0_0_chr17_43045629_r	0	-
chr17	43047642	43047703	ENST00000493795.5_exon_1_0_chr17_43047643_r	0	-
chr17	43049120	43049194	ENST00000493795.5_exon_2_0_chr17_43049121_r	0	-
chr17	43051062	43051117	ENST00000493795.5_exon_3_0_chr17_43051063_r	0	-
chr17	43057051	43057135	ENST00000493795.5_exon_4_0_chr17_43057052_r	0	-
chr17	43063332	43063373	ENST00000493795.5_exon_5_0_chr17_43063333_r	0	-
chr17	43063873	43063951	ENST00000493795.5_exon_6_0_chr17_43063874_r	0	-
chr17	43067607	43067695	ENST00000493795.5_exon_7_0_chr17_43067608_r	0	-
chr17	43070927	43071238	ENST00000493795.5_exon_8_0_chr17_43070928_r	0	-
chr17	43074330	43074521	ENST00000493795.5_exon_9_0_chr17_43074331_r	0	-
chr17	43076487	43076614	ENST00000493795.5_exon_10_0_chr17_43076488_r	0	-
chr17	43082403	43082575	ENST00000493795.5_exon_11_0_chr17_43082404_r	0	-
chr17	43090943	43091032	ENST00000493795.5_exon_12_0_chr17_43090944_r	0	-
chr17	43091434	43094860	ENST00000493795.5_exon_13_0_chr17_43091435_r	0	-
chr17	43095845	43095922	ENST00000493795.5_exon_14_0_chr17_43095846_r	0	-
chr17	43097243	43097289	ENST00000493795.5_exon_15_0_chr17_43097244_r	0	-
chr17	43099774	43099880	ENST00000493795.5_exon_16_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000493795.5_exon_17_0_chr17_43104122_r	0	-
chr17	43104867	43104956	ENST00000493795.5_exon_18_0_chr17_43104868_r	0	-
chr17	43106455	43106533	ENST00000493795.5_exon_19_0_chr17_43106456_r	0	-
chr17	43124016	43124115	ENST00000493795.5_exon_20_0_chr17_43124017_r	0	-
chr17	43125276	43125402	ENST00000493795.5_exon_21_0_chr17_43125277_r	0	-
chr17	43045677	43045802	ENST00000461221.5_exon_0_0_chr17_43045678_r	0	-
chr17	43047642	43047703	ENST00000461221.5_exon_1_0_chr17_43047643_r	0	-
chr17	43049120	43049194	ENST00000461221.5_exon_2_0_chr17_43049121_r	0	-
chr17	43051062	43051117	ENST00000461221.5_exon_3_0_chr17_43051063_r	0	-
chr17	43057051	43057135	ENST00000461221.5_exon_4_0_chr17_43057052_r	0	-
chr17	43063332	43063373	ENST00000461221.5_exon_5_0_chr17_43063333_r	0	-
chr17	43063873	43063951	ENST00000461221.5_exon_6_0_chr17_43063874_r	0	-
chr17	43067607	43067695	ENST00000461221.5_exon_7_0_chr17_43067608_r	0	-
chr17	43070927	43071238	ENST00000461221.5_exon_8_0_chr17_43070928_r	0	-
chr17	43074330	43074521	ENST00000461221.5_exon_9_0_chr17_43074331_r	0	-
chr17	43076487	43076614	ENST00000461221.5_exon_10_0_chr17_43076488_r	0	-
chr17	43082403	43082575	ENST00000461221.5_exon_11_0_chr17_43082404_r	0	-
chr17	43090943	43091032	ENST00000461221.5_exon_12_0_chr17_43090944_r	0	-
chr17	43091434	43094860	ENST00000461221.5_exon_13_0_chr17_43091435_r	0	-
chr17	43095845	43095922	ENST00000461221.5_exon_14_0_chr17_43095846_r	0	-
chr17	43097243	43097289	ENST00000461221.5_exon_15_0_chr17_43097244_r	0	-
chr17	43099774	43099877	ENST00000461221.5_exon_16_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000461221.5_exon_17_0_chr17_43104122_r	0	-
chr17	43104867	43104956	ENST00000461221.5_exon_18_0_chr17_43104868_r	0	-
chr17	43106477	43106533	ENST00000461221.5_exon_19_0_chr17_43106478_r	0	-
chr17	43115725	43115779	ENST00000461221.5_exon_20_0_chr17_43115726_r	0	-
chr17	43124016	43124115	ENST00000461221.5_exon_21_0_chr17_43124017_r	0	-
chr17	43125181	43125288	ENST00000461221.5_exon_22_0_chr17_43125182_r	0	-
chr17	43045677	43045802	ENST00000491747.6_exon_0_0_chr17_43045678_r	0	-
chr17	43047642	43047703	ENST00000491747.6_exon_1_0_chr17_43047643_r	0	-
chr17	43049120	43049194	ENST00000491747.6_exon_2_0_chr17_43049121_r	0	-
chr17	43051062	43051117	ENST00000491747.6_exon_3_0_chr17_43051063_r	0	-
chr17	43057051	43057135	ENST00000491747.6_exon_4_0_chr17_43057052_r	0	-
chr17	43063332	43063373	ENST00000491747.6_exon_5_0_chr17_43063333_r	0	-
chr17	43063873	43063951	ENST00000491747.6_exon_6_0_chr17_43063874_r	0	-
chr17	43067607	43067695	ENST00000491747.6_exon_7_0_chr17_43067608_r	0	-
chr17	43070927	43071238	ENST00000491747.6_exon_8_0_chr17_43070928_r	0	-
chr17	43074330	43074521	ENST00000491747.6_exon_9_0_chr17_43074331_r	0	-
chr17	43076487	43076611	ENST00000491747.6_exon_10_0_chr17_43076488_r	0	-
chr17	43082403	43082575	ENST00000491747.6_exon_11_0_chr17_43082404_r	0	-
chr17	43090943	43091032	ENST00000491747.6_exon_12_0_chr17_43090944_r	0	-
chr17	43094743	43094860	ENST00000491747.6_exon_13_0_chr17_43094744_r	0	-
chr17	43095845	43095922	ENST00000491747.6_exon_14_0_chr17_43095846_r	0	-
chr17	43097243	43097289	ENST00000491747.6_exon_15_0_chr17_43097244_r	0	-
chr17	43099774	43099880	ENST00000491747.6_exon_16_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000491747.6_exon_17_0_chr17_43104122_r	0	-
chr17	43104867	43104956	ENST00000491747.6_exon_18_0_chr17_43104868_r	0	-
chr17	43106455	43106533	ENST00000491747.6_exon_19_0_chr17_43106456_r	0	-
chr17	43115725	43115779	ENST00000491747.6_exon_20_0_chr17_43115726_r	0	-
chr17	43124016	43124115	ENST00000491747.6_exon_21_0_chr17_43124017_r	0	-
chr17	43125276	43125356	ENST00000491747.6_exon_22_0_chr17_43125277_r	0	-
chr17	43063343	43063373	ENST00000484087.5_exon_0_0_chr17_43063344_r	0	-
chr17	43063873	43063951	ENST00000484087.5_exon_1_0_chr17_43063874_r	0	-
chr17	43067607	43067695	ENST00000484087.5_exon_2_0_chr17_43067608_r	0	-
chr17	43070927	43071238	ENST00000484087.5_exon_3_0_chr17_43070928_r	0	-
chr17	43074330	43074521	ENST00000484087.5_exon_4_0_chr17_43074331_r	0	-
chr17	43076487	43076614	ENST00000484087.5_exon_5_0_chr17_43076488_r	0	-
chr17	43082403	43082575	ENST00000484087.5_exon_6_0_chr17_43082404_r	0	-
chr17	43090943	43091032	ENST00000484087.5_exon_7_0_chr17_43090944_r	0	-
chr17	43094743	43094860	ENST00000484087.5_exon_8_0_chr17_43094744_r	0	-
chr17	43099774	43099877	ENST00000484087.5_exon_9_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000484087.5_exon_10_0_chr17_43104122_r	0	-
chr17	43104867	43104916	ENST00000484087.5_exon_11_0_chr17_43104868_r	0	-
chr17	43063343	43063373	ENST00000478531.5_exon_0_0_chr17_43063344_r	0	-
chr17	43063873	43063951	ENST00000478531.5_exon_1_0_chr17_43063874_r	0	-
chr17	43067607	43067695	ENST00000478531.5_exon_2_0_chr17_43067608_r	0	-
chr17	43070927	43071238	ENST00000478531.5_exon_3_0_chr17_43070928_r	0	-
chr17	43074330	43074521	ENST00000478531.5_exon_4_0_chr17_43074331_r	0	-
chr17	43076487	43076614	ENST00000478531.5_exon_5_0_chr17_43076488_r	0	-
chr17	43082403	43082575	ENST00000478531.5_exon_6_0_chr17_43082404_r	0	-
chr17	43090943	43091032	ENST00000478531.5_exon_7_0_chr17_43090944_r	0	-
chr17	43094743	43094860	ENST00000478531.5_exon_8_0_chr17_43094744_r	0	-
chr17	43095845	43095922	ENST00000478531.5_exon_9_0_chr17_43095846_r	0	-
chr17	43097243	43097289	ENST00000478531.5_exon_10_0_chr17_43097244_r	0	-
chr17	43099774	43099877	ENST00000478531.5_exon_11_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000478531.5_exon_12_0_chr17_43104122_r	0	-
chr17	43104867	43104956	ENST00000478531.5_exon_13_0_chr17_43104868_r	0	-
chr17	43106455	43106533	ENST00000478531.5_exon_14_0_chr17_43106456_r	0	-
chr17	43115725	43115779	ENST00000478531.5_exon_15_0_chr17_43115726_r	0	-
chr17	43124016	43124115	ENST00000478531.5_exon_16_0_chr17_43124017_r	0	-
chr17	43125276	43125359	ENST00000478531.5_exon_17_0_chr17_43125277_r	0	-
chr17	43063359	43063373	ENST00000493919.5_exon_0_0_chr17_43063360_r	0	-
chr17	43063873	43063951	ENST00000493919.5_exon_1_0_chr17_43063874_r	0	-
chr17	43067607	43067695	ENST00000493919.5_exon_2_0_chr17_43067608_r	0	-
chr17	43070927	43071238	ENST00000493919.5_exon_3_0_chr17_43070928_r	0	-
chr17	43074330	43074521	ENST00000493919.5_exon_4_0_chr17_43074331_r	0	-
chr17	43076487	43076614	ENST00000493919.5_exon_5_0_chr17_43076488_r	0	-
chr17	43082403	43082575	ENST00000493919.5_exon_6_0_chr17_43082404_r	0	-
chr17	43090943	43091032	ENST00000493919.5_exon_7_0_chr17_43090944_r	0	-
chr17	43094743	43094860	ENST00000493919.5_exon_8_0_chr17_43094744_r	0	-
chr17	43095845	43095922	ENST00000493919.5_exon_9_0_chr17_43095846_r	0	-
chr17	43097243	43097289	ENST00000493919.5_exon_10_0_chr17_43097244_r	0	-
chr17	43099774	43099880	ENST00000493919.5_exon_11_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000493919.5_exon_12_0_chr17_43104122_r	0	-
chr17	43104867	43104956	ENST00000493919.5_exon_13_0_chr17_43104868_r	0	-
chr17	43106455	43106533	ENST00000493919.5_exon_14_0_chr17_43106456_r	0	-
chr17	43124016	43124115	ENST00000493919.5_exon_15_0_chr17_43124017_r	0	-
chr17	43125276	43125402	ENST00000493919.5_exon_16_0_chr17_43125277_r	0	-
chr17	43076487	43076614	ENST00000487825.5_exon_0_0_chr17_43076488_r	0	-
chr17	43082403	43082575	ENST00000487825.5_exon_1_0_chr17_43082404_r	0	-
chr17	43090943	43091032	ENST00000487825.5_exon_2_0_chr17_43090944_r	0	-
chr17	43094743	43094860	ENST00000487825.5_exon_3_0_chr17_43094744_r	0	-
chr17	43099774	43099880	ENST00000487825.5_exon_4_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000487825.5_exon_5_0_chr17_43104122_r	0	-
chr17	43104867	43104916	ENST00000487825.5_exon_6_0_chr17_43104868_r	0	-
chr17	43076536	43076611	ENST00000461574.1_exon_0_0_chr17_43076537_r	0	-
chr17	43082403	43082575	ENST00000461574.1_exon_1_0_chr17_43082404_r	0	-
chr17	43090943	43091032	ENST00000461574.1_exon_2_0_chr17_43090944_r	0	-
chr17	43091434	43091824	ENST00000461574.1_exon_3_0_chr17_43091435_r	0	-
chr17	43091097	43094860	ENST00000354071.7_exon_0_0_chr17_43091098_r	0	-
chr17	43095845	43095922	ENST00000354071.7_exon_1_0_chr17_43095846_r	0	-
chr17	43097243	43097289	ENST00000354071.7_exon_2_0_chr17_43097244_r	0	-
chr17	43099774	43099880	ENST00000354071.7_exon_3_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000354071.7_exon_4_0_chr17_43104122_r	0	-
chr17	43104867	43104956	ENST00000354071.7_exon_5_0_chr17_43104868_r	0	-
chr17	43106455	43106533	ENST00000354071.7_exon_6_0_chr17_43106456_r	0	-
chr17	43115725	43115779	ENST00000354071.7_exon_7_0_chr17_43115726_r	0	-
chr17	43124016	43124115	ENST00000354071.7_exon_8_0_chr17_43124017_r	0	-
chr17	43125270	43125315	ENST00000354071.7_exon_9_0_chr17_43125271_r	0	-
chr17	43093012	43094860	ENST00000634433.1_exon_0_0_chr17_43093013_r	0	-
chr17	43099774	43099880	ENST00000634433.1_exon_1_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000634433.1_exon_2_0_chr17_43104122_r	0	-
chr17	43104867	43104956	ENST00000634433.1_exon_3_0_chr17_43104868_r	0	-
chr17	43106455	43106533	ENST00000634433.1_exon_4_0_chr17_43106456_r	0	-
chr17	43115725	43115779	ENST00000634433.1_exon_5_0_chr17_43115726_r	0	-
chr17	43124016	43124115	ENST00000634433.1_exon_6_0_chr17_43124017_r	0	-
chr17	43170125	43170245	ENST00000634433.1_exon_7_0_chr17_43170126_r	0	-
chr17	43093569	43094860	ENST00000412061.3_exon_0_0_chr17_43093570_r	0	-
chr17	43095845	43095866	ENST00000412061.3_exon_1_0_chr17_43095846_r	0	-
chr17	43093583	43094860	ENST00000470026.5_exon_0_0_chr17_43093584_r	0	-
chr17	43095845	43095922	ENST00000470026.5_exon_1_0_chr17_43095846_r	0	-
chr17	43097243	43097289	ENST00000470026.5_exon_2_0_chr17_43097244_r	0	-
chr17	43099774	43099880	ENST00000470026.5_exon_3_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000470026.5_exon_4_0_chr17_43104122_r	0	-
chr17	43104867	43104956	ENST00000470026.5_exon_5_0_chr17_43104868_r	0	-
chr17	43106455	43106533	ENST00000470026.5_exon_6_0_chr17_43106456_r	0	-
chr17	43115725	43115779	ENST00000470026.5_exon_7_0_chr17_43115726_r	0	-
chr17	43124016	43124115	ENST00000470026.5_exon_8_0_chr17_43124017_r	0	-
chr17	43125181	43125323	ENST00000470026.5_exon_9_0_chr17_43125182_r	0	-
chr17	43093584	43094860	ENST00000652672.1_exon_0_0_chr17_43093585_r	0	-
chr17	43095845	43095922	ENST00000652672.1_exon_1_0_chr17_43095846_r	0	-
chr17	43097243	43097289	ENST00000652672.1_exon_2_0_chr17_43097244_r	0	-
chr17	43099774	43099880	ENST00000652672.1_exon_3_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000652672.1_exon_4_0_chr17_43104122_r	0	-
chr17	43104867	43104956	ENST00000652672.1_exon_5_0_chr17_43104868_r	0	-
chr17	43106455	43106533	ENST00000652672.1_exon_6_0_chr17_43106456_r	0	-
chr17	43115725	43115779	ENST00000652672.1_exon_7_0_chr17_43115726_r	0	-
chr17	43121557	43121676	ENST00000652672.1_exon_8_0_chr17_43121558_r	0	-
chr17	43124016	43124115	ENST00000652672.1_exon_9_0_chr17_43124017_r	0	-
chr17	43125276	43125483	ENST00000652672.1_exon_10_0_chr17_43125277_r	0	-
chr17	43093585	43094860	ENST00000477152.5_exon_0_0_chr17_43093586_r	0	-
chr17	43095845	43095922	ENST00000477152.5_exon_1_0_chr17_43095846_r	0	-
chr17	43097243	43097289	ENST00000477152.5_exon_2_0_chr17_43097244_r	0	-
chr17	43099774	43099880	ENST00000477152.5_exon_3_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000477152.5_exon_4_0_chr17_43104122_r	0	-
chr17	43104867	43104956	ENST00000477152.5_exon_5_0_chr17_43104868_r	0	-
chr17	43115725	43115779	ENST00000477152.5_exon_6_0_chr17_43115726_r	0	-
chr17	43124016	43124115	ENST00000477152.5_exon_7_0_chr17_43124017_r	0	-
chr17	43125270	43125364	ENST00000477152.5_exon_8_0_chr17_43125271_r	0	-
chr17	43094111	43094860	ENST00000492859.5_exon_0_0_chr17_43094112_r	0	-
chr17	43095845	43095922	ENST00000492859.5_exon_1_0_chr17_43095846_r	0	-
chr17	43097243	43097289	ENST00000492859.5_exon_2_0_chr17_43097244_r	0	-
chr17	43099774	43099880	ENST00000492859.5_exon_3_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000492859.5_exon_4_0_chr17_43104122_r	0	-
chr17	43104867	43104956	ENST00000492859.5_exon_5_0_chr17_43104868_r	0	-
chr17	43106455	43106533	ENST00000492859.5_exon_6_0_chr17_43106456_r	0	-
chr17	43110464	43110580	ENST00000492859.5_exon_7_0_chr17_43110465_r	0	-
chr17	43115725	43115779	ENST00000492859.5_exon_8_0_chr17_43115726_r	0	-
chr17	43124016	43124115	ENST00000492859.5_exon_9_0_chr17_43124017_r	0	-
chr17	43125270	43125300	ENST00000492859.5_exon_10_0_chr17_43125271_r	0	-
chr17	43094111	43094860	ENST00000497488.1_exon_0_0_chr17_43094112_r	0	-
chr17	43125270	43125300	ENST00000497488.1_exon_1_0_chr17_43125271_r	0	-
chr17	43094111	43094860	ENST00000494123.5_exon_0_0_chr17_43094112_r	0	-
chr17	43095845	43095922	ENST00000494123.5_exon_1_0_chr17_43095846_r	0	-
chr17	43097243	43097289	ENST00000494123.5_exon_2_0_chr17_43097244_r	0	-
chr17	43099774	43099880	ENST00000494123.5_exon_3_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000494123.5_exon_4_0_chr17_43104122_r	0	-
chr17	43104867	43104956	ENST00000494123.5_exon_5_0_chr17_43104868_r	0	-
chr17	43106455	43106533	ENST00000494123.5_exon_6_0_chr17_43106456_r	0	-
chr17	43115725	43115779	ENST00000494123.5_exon_7_0_chr17_43115726_r	0	-
chr17	43124016	43124115	ENST00000494123.5_exon_8_0_chr17_43124017_r	0	-
chr17	43125276	43125450	ENST00000494123.5_exon_9_0_chr17_43125277_r	0	-
chr17	43094169	43094860	ENST00000473961.5_exon_0_0_chr17_43094170_r	0	-
chr17	43099774	43099877	ENST00000473961.5_exon_1_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000473961.5_exon_2_0_chr17_43104122_r	0	-
chr17	43104867	43104891	ENST00000473961.5_exon_3_0_chr17_43104868_r	0	-
chr17	43094481	43094860	ENST00000642945.1_exon_0_0_chr17_43094482_r	0	-
chr17	43095845	43095922	ENST00000642945.1_exon_1_0_chr17_43095846_r	0	-
chr17	43097243	43097289	ENST00000642945.1_exon_2_0_chr17_43097244_r	0	-
chr17	43099774	43099880	ENST00000642945.1_exon_3_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000642945.1_exon_4_0_chr17_43104122_r	0	-
chr17	43104867	43104956	ENST00000642945.1_exon_5_0_chr17_43104868_r	0	-
chr17	43106455	43106533	ENST00000642945.1_exon_6_0_chr17_43106456_r	0	-
chr17	43112486	43112606	ENST00000642945.1_exon_7_0_chr17_43112487_r	0	-
chr17	43115725	43115779	ENST00000642945.1_exon_8_0_chr17_43115726_r	0	-
chr17	43124016	43124115	ENST00000642945.1_exon_9_0_chr17_43124017_r	0	-
chr17	43125276	43125343	ENST00000642945.1_exon_10_0_chr17_43125277_r	0	-
chr17	43095845	43095922	ENST00000476777.5_exon_0_0_chr17_43095846_r	0	-
chr17	43097243	43097289	ENST00000476777.5_exon_1_0_chr17_43097244_r	0	-
chr17	43099774	43099877	ENST00000476777.5_exon_2_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000476777.5_exon_3_0_chr17_43104122_r	0	-
chr17	43104867	43104956	ENST00000476777.5_exon_4_0_chr17_43104868_r	0	-
chr17	43106455	43106533	ENST00000476777.5_exon_5_0_chr17_43106456_r	0	-
chr17	43115725	43115779	ENST00000476777.5_exon_6_0_chr17_43115726_r	0	-
chr17	43124016	43124115	ENST00000476777.5_exon_7_0_chr17_43124017_r	0	-
chr17	43125270	43125353	ENST00000476777.5_exon_8_0_chr17_43125271_r	0	-
"""
