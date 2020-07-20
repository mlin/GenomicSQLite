import math
import os
import sqlite3
import random
import pytest
import genomicsqlite

HERE = os.path.dirname(__file__)
BUILD = os.path.abspath(os.path.join(HERE, "..", "build"))

BIN_OFFSETS = [
    0,
    1,
    1 + 16,
    1 + 16 + 256,
    1 + 16 + 256 + 4096,
    1 + 16 + 256 + 4096 + 65536,
    1 + 16 + 256 + 4096 + 65536 + 1048576,
    1 + 16 + 256 + 4096 + 65536 + 1048576 + 16777216,
    1 + 16 + 256 + 4096 + 65336 + 1048576 + 16777216 + 268435456,
]
POS_OFFSETS = [
    0,
    134217728,
    8388608,
    524288,
    32768,
    2048,
    128,
    8,
    0,
]


def test_genomic_range_bin():
    examples = [
        (1048576, 1048577, 65536 + BIN_OFFSETS[-1]),
        (1048575, 1048577, 4095 + BIN_OFFSETS[-2]),
        (1048560, 1048575, 65535 + BIN_OFFSETS[-1]),
        (1048560, 1048576, 4095 + BIN_OFFSETS[-2]),
        (1048559, 1048560, 4095 + BIN_OFFSETS[-2]),
        (1, 68719476734, 0),
        (4294967296 + 134217728, 4294967296 * 2 + 134217728 - 1, 2),
        (4294967296, 4294967296 * 2 - 1, 0),
        (2, 1, -1),
        (0, 68719476736, -1),
        (-1, 42, -1),
    ]
    con = sqlite3.connect(":memory:")
    stmt = "SELECT genomic_range_bin(?,?)"
    for (beg, end, gri_bin) in examples:
        if gri_bin >= 0:
            assert next(con.execute(stmt, (beg, end)))[0] == gri_bin, str((beg, end))
        else:
            with pytest.raises(sqlite3.OperationalError):
                con.execute(stmt, (beg, end))

    examples = [
        (0, BIN_OFFSETS[0]),
        (1, BIN_OFFSETS[1]),
        (2, BIN_OFFSETS[2]),
        (3, BIN_OFFSETS[3]),
        (4, BIN_OFFSETS[4]),
        (5, 15 + BIN_OFFSETS[5]),
        (6, 255 + BIN_OFFSETS[6]),
        (7, 4095 + BIN_OFFSETS[7]),
        (8, 65536 + BIN_OFFSETS[8]),
    ]
    for (max_depth, gri_bin) in examples:
        assert (
            next(con.execute("SELECT genomic_range_bin(?,?,?)", (1048576, 1048577, max_depth)))[0]
            == gri_bin
        ), str(max_depth)


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
    accessed_cursors = list(opcode[2] for opcode in opcodes if opcode[1] == "Column")
    table_rootpages = set(
        row[0] for row in con.execute("SELECT rootpage FROM sqlite_master WHERE type='table'")
    )
    table_cursors = set(
        opcode[2] for opcode in opcodes if opcode[1] == "OpenRead" and opcode[3] in table_rootpages
    )
    assert sum(1 for cursor in accessed_cursors if cursor not in table_cursors) > 1  # get from idx
    assert sum(1 for cursor in accessed_cursors if cursor in table_cursors) == 1  # get from table

    # (Also, it should produce correct results)
    control_query = "SELECT id FROM exons NOT INDEXED WHERE rid = ? AND NOT (end < ? OR beg > ?) ORDER BY _rowid_"
    random.seed(0xBADF00D)
    total_results = 0
    for _ in range(50000):
        beg = random.randint(43044294 - 10000, 43044294 + 10000)
        end = beg + random.randint(1, random.choice([10, 100, 1000, 10000]))
        ids = list(row[0] for row in con.execute(query, ("chr17", beg, end)))
        control_ids = list(row[0] for row in con.execute(control_query, ("chr17", beg, end)))
        assert ids == control_ids
        total_results += len(control_ids)
    assert total_results == 189935

    with pytest.raises(sqlite3.OperationalError):
        genomicsqlite.genomic_range_rowids_sql(con, "nonexistent_table")


def test_depth_detection():
    # test corner cases for the bit of genomic_range_rowids() which detects the depth range

    con = sqlite3.connect(":memory:")
    con.executescript("CREATE TABLE features(rid INTEGER, beg INTEGER, end INTEGER)")
    con.executescript(
        genomicsqlite.create_genomic_range_index_sql(con, "features", "rid", "beg", "end")
    )

    def fanout(query):
        return sum(
            1
            for expl in con.execute("EXPLAIN QUERY PLAN " + query, (None, None, None))
            if "((_gri_rid,_gri_bin)>(?,?) AND (_gri_rid,_gri_bin)<(?,?))" in expl[3]
        )

    assert fanout(genomicsqlite.genomic_range_rowids_sql(con, "features")[1:-1]) == 9

    con.executescript(
        "INSERT INTO features VALUES(NULL, NULL, NULL); INSERT INTO features VALUES(NULL, 0, 10000000000)"
    )
    assert fanout(genomicsqlite.genomic_range_rowids_sql(con, "features")[1:-1]) == 9
    assert not list(
        con.execute(genomicsqlite.genomic_range_rowids_sql(con, "features")[1:-1], (None, 123, 456))
    )

    con.executescript("INSERT INTO features VALUES(42, 1048568, 1048584)")
    query = genomicsqlite.genomic_range_rowids_sql(con, "features")[1:-1]
    print("\n" + query)
    assert " / 4096)" in query  # level 6
    assert not list(
        con.execute(
            genomicsqlite.genomic_range_rowids_sql(con, "features")[1:-1], (42, None, 1048584)
        )
    )

    assert fanout(query) == 1

    con.executescript(
        """
        INSERT INTO features VALUES(44, 1048568, 1048584);
        INSERT INTO features VALUES(44, 0, 64000)
        """
    )
    query = genomicsqlite.genomic_range_rowids_sql(con, "features")[1:-1]
    print("\n" + query)
    assert " / 65536)" in query  # level 5
    assert " / 4096)" in query  # level 6
    assert fanout(query) == 2

    assert fanout(genomicsqlite.genomic_range_rowids_sql(con, "features", safe=True)[1:-1]) == 9

    con.executescript(
        """
        INSERT INTO features VALUES(43, NULL, 10000000000);
        INSERT INTO features VALUES(44, 0, NULL)
        """
    )
    assert fanout(genomicsqlite.genomic_range_rowids_sql(con, "features")[1:-1]) == 2

    con.executescript(
        """
    INSERT INTO features VALUES(43, 0, 10000000000);
    INSERT INTO features VALUES(43, 32, 48)
    """
    )
    query = genomicsqlite.genomic_range_rowids_sql(con, "features")[1:-1]
    print("\n" + query)
    assert " / 16)" not in query
    assert fanout(query) == 8

    con.executescript(
        """
        INSERT INTO features VALUES(43, 0, 10000000000);
        INSERT INTO features VALUES(43, 32, 47)
        """
    )
    query = genomicsqlite.genomic_range_rowids_sql(con, "features")[1:-1]
    assert fanout(query) == 9


def test_boundaries():
    # test abutting intervals near bin boundaries

    con = sqlite3.connect(":memory:")
    con.executescript("CREATE TABLE features(rid INTEGER, beg INTEGER, end INTEGER)")

    insert = "INSERT INTO features(rid,beg,end) values(?,?,?)"
    for depth in range(2, 9):
        boundary = 3 * (16 ** (9 - depth)) + POS_OFFSETS[depth]
        for ofs in range(-2, 3):
            featlen = math.ceil(16 ** (9 - depth) / 2)
            con.execute(insert, (42, boundary + ofs - featlen, boundary + ofs))
            con.execute(insert, (42, boundary + ofs, boundary + ofs + featlen))

    con.executescript(
        genomicsqlite.create_genomic_range_index_sql(con, "features", "rid", "beg", "end")
    )

    query = genomicsqlite.genomic_range_rowids_sql(con, "features")[1:-1]
    control = "SELECT _rowid_ FROM features NOT INDEXED WHERE rid = ? AND NOT (? > end OR ? < beg) ORDER BY _rowid_"
    total_results = 0
    for depth in range(2, 9):
        boundary = 3 * (16 ** (9 - depth)) + POS_OFFSETS[depth]
        for qlen in range(3):
            for ofs in range(-3, 4):
                tup = (42, boundary + ofs, boundary + ofs + qlen)
                query_results = list(con.execute(query, tup))
                control_results = list(con.execute(control, tup))
                assert query_results == control_results
                total_results += len(query_results)
    assert total_results == 938


def test_refseq():
    con = sqlite3.connect(":memory:")

    create_assembly = genomicsqlite.put_reference_assembly_sql(con, "GRCh38_no_alt_analysis_set")
    lines = create_assembly.strip().split("\n")
    print("\n".join([line for line in lines if "INSERT INTO" in line][:24]))
    assert len([line for line in lines if "INSERT INTO" in line]) == 195
    print("\n".join([line for line in lines if "INSERT INTO" not in line]))
    assert len([line for line in lines if "INSERT INTO" not in line]) == 2
    con.executescript(create_assembly)

    _fill_exons(con, max_depth=7)
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
        "SELECT __gri_refseq._gri_rid, rid, beg, end, id FROM exons, __gri_refseq WHERE exons.rid = gri_refseq_name AND exons._rowid_ IN "
        + genomicsqlite.genomic_range_rowids_sql(con, "exons")
    )
    print("\n" + query)
    assert len([line for line in query.split("\n") if "BETWEEN" in line]) == 3
    assert len(list(con.execute(query, ("chr17", 43115725, 43125370)))) == 56


def test_join():
    con = sqlite3.connect(":memory:")
    con.executescript(genomicsqlite.put_reference_assembly_sql(con, "GRCh38_no_alt_analysis_set"))
    _fill_exons(con, table="exons")
    _fill_exons(con, max_depth=7, table="exons2")
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
        if "USING INDEX exons2__gri" in expl[3]:
            indexed += 1
    assert indexed == 3
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
    del con

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


def _fill_exons(con, max_depth=-1, table="exons"):
    con.execute(
        f"CREATE TABLE {table}(rid TEXT NOT NULL, beg INTEGER NOT NULL, end INTEGER NOT NULL, id TEXT NOT NULL)"
    )
    for line in _EXONS.strip().split("\n"):
        line = line.split("\t")
        con.execute(
            f"INSERT INTO {table}(rid,beg,end,id) VALUES(?,?,?,?)",
            (line[0], int(line[1]) - 1, int(line[2]), line[3]),
        )
    con.executescript(
        genomicsqlite.create_genomic_range_index_sql(
            con, table, "rid", "beg", "end", max_depth=max_depth
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
