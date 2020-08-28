import os
import sqlite3
import subprocess
import random
import pytest
import genomicsqlite

HERE = os.path.dirname(__file__)


@pytest.fixture
def txdb(tmp_path):
    ans = str(tmp_path / "TxDb.Hsapiens.UCSC.hg38.knownGene.sqlite")
    subprocess.run(
        f"pigz -dc {HERE+'/data/TxDb.Hsapiens.UCSC.hg38.knownGene.sqlite.gz'} > {ans}",
        shell=True,
        check=True,
    )
    return ans


@pytest.fixture
def genomicsqlite_txdb(txdb):
    """
    import bioC TxDb sourced originally from:
    http://bioconductor.org/packages/release/BiocViews.html#___TxDb
    """
    outfile = txdb[:-7] + ".genomicsqlite"
    conn = sqlite3.connect(txdb, uri=True)
    conn.executescript(genomicsqlite.vacuum_into_sql(conn, outfile))
    conn.close()
    # create GRIs on the three feature tables
    conn = genomicsqlite.connect(outfile)
    conn.executescript(
        genomicsqlite.create_genomic_range_index_sql(
            conn, "transcript", "tx_chrom", "tx_start", "tx_end", floor=2
        )
    )
    conn.executescript(
        genomicsqlite.create_genomic_range_index_sql(
            conn, "cds", "cds_chrom", "cds_start", "cds_end", floor=2
        )
    )
    # intentionally left exon unindexed
    conn.close()
    return outfile


def test_txdbquery(genomicsqlite_txdb):
    conn = genomicsqlite.connect(genomicsqlite_txdb, read_only=True)
    assert next(conn.execute("PRAGMA page_size"))[0] == 16384

    # one query
    results = list(
        t[0]
        for t in conn.execute(
            "SELECT tx_name FROM transcript WHERE _rowid_ IN "
            + genomicsqlite.genomic_range_rowids_sql(conn, "transcript")
            + " ORDER BY tx_name",
            ("chr12", 111803912, 111804012),
        )
    )
    print(results)
    assert results == sorted(
        ["ENST00000416293.7", "ENST00000261733.7", "ENST00000548536.1", "ENST00000549106.1"]
    )

    # random queries
    chroms = list(
        conn.execute(
            "SELECT tx_chrom, length FROM (SELECT tx_chrom, MAX(tx_end) AS length FROM transcript GROUP BY tx_chrom) WHERE length > 1000000"
        )
    )

    random.seed(0xBADF00D)
    for tbl in ("transcript", "cds"):
        query = genomicsqlite.genomic_range_rowids_sql(conn, tbl)[1:-1]
        fanout = 0
        for expl in conn.execute("EXPLAIN QUERY PLAN " + query, (None, None, None)):
            print(expl[3])
            if (
                "((_gri_rid,_gri_lvl,_gri_beg)>(?,?,?) AND (_gri_rid,_gri_lvl,_gri_beg)<(?,?,?))"
                in expl[3]
            ):
                fanout += 1
        assert (tbl, fanout) in (("transcript", 5), ("cds", 3))

        pfx = "tx" if tbl == "transcript" else tbl
        control_query = f"SELECT _rowid_ FROM {tbl} NOT INDEXED WHERE {pfx}_chrom = ? AND NOT ({pfx}_end < ? OR {pfx}_start > ?) ORDER BY _rowid_"

        total_results = 0
        for _ in range(2000):
            chrom = random.choice(chroms)
            beg = random.randint(0, chrom[1] - 65536)
            end = beg + random.randint(1, random.choice([16, 256, 4096, 65536]))
            ids = list(row[0] for row in conn.execute(query, (chrom[0], beg, end)))
            control_ids = list(row[0] for row in conn.execute(control_query, (chrom[0], beg, end)))
            assert ids == control_ids
            total_results += len(control_ids)
        assert total_results in (7341, 2660)

    # join exon to cds ("which exons are coding?")
    exon_cds_counts = (
        "SELECT exon._rowid_ AS exon_id, COUNT(cds._rowid_) AS contained_cds FROM exon LEFT JOIN cds ON cds._rowid_ IN "
        + genomicsqlite.genomic_range_rowids_sql(
            conn, "cds", "exon_chrom", "exon_start", "exon_end"
        )
        + " AND (exon_start = cds_start AND exon_end >= cds_start OR exon_start <= cds_start AND exon_end = cds_end) GROUP BY exon._rowid_"
    )
    print(exon_cds_counts)
    fanout = 0
    for expl in conn.execute("EXPLAIN QUERY PLAN " + exon_cds_counts):
        print(expl[3])
        if (
            "((_gri_rid,_gri_lvl,_gri_beg)>(?,?,?) AND (_gri_rid,_gri_lvl,_gri_beg)<(?,?,?))"
            in expl[3]
        ):
            fanout += 1
    assert fanout == 3
    cds_exon_count_hist = list(
        conn.execute(
            f"SELECT contained_cds, count(exon_id) AS exon_count FROM ({exon_cds_counts}) GROUP BY contained_cds ORDER BY contained_cds"
        )
    )
    for elt in cds_exon_count_hist:
        print(elt)
    assert cds_exon_count_hist[:2] == [(0, 270532), (1, 310059)]

    # repeat with TVFs
    exon_cds_counts = (
        """
        SELECT exon._rowid_ AS exon_id, COUNT(cds._rowid_) AS contained_cds
        FROM genomic_range_index_levels("cds"), exon LEFT JOIN cds
            ON cds._rowid_ IN genomic_range_rowids("cds", exon_chrom, exon_start, exon_end, gri_ceiling, gri_floor)
            AND (exon_start = cds_start AND exon_end >= cds_start OR exon_start <= cds_start AND exon_end = cds_end)
        GROUP BY exon._rowid_
        """
    )
    for expl in conn.execute("EXPLAIN QUERY PLAN " + exon_cds_counts):
        print(expl[3])
    cds_exon_count_hist = list(
        conn.execute(
            f"SELECT contained_cds, count(exon_id) AS exon_count FROM ({exon_cds_counts}) GROUP BY contained_cds ORDER BY contained_cds"
        )
    )
    assert cds_exon_count_hist[:2] == [(0, 270532), (1, 310059)]
