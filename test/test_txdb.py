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
    conn = sqlite3.connect(txdb)
    conn.executescript(genomicsqlite.vacuum_into_sql(conn, outfile))
    conn.close()
    # create GRIs on the three feature tables
    conn = genomicsqlite.connect(outfile)
    conn.executescript(
        genomicsqlite.create_genomic_range_index_sql(
            conn, "transcript", "tx_chrom", "tx_start", "tx_end"
        )
    )
    conn.executescript(
        genomicsqlite.create_genomic_range_index_sql(
            conn, "exon", "exon_chrom", "exon_start", "exon_end"
        )
    )
    conn.executescript(
        genomicsqlite.create_genomic_range_index_sql(
            conn, "cds", "cds_chrom", "cds_start", "cds_end"
        )
    )
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

    # join cds to exon
    cds_exon_counts = (
        "SELECT cds._rowid_ AS cds_id, COUNT(exon._rowid_) AS containing_exons FROM cds, exon WHERE exon._rowid_ IN "
        + genomicsqlite.genomic_range_rowids_sql(conn, "exon", "cds_chrom", "cds_start", "cds_end")
        + " AND cds_start >= exon_start and cds_end <= exon_end GROUP BY cds._rowid_"
    )
    cds_exon_count_hist = list(
        conn.execute(
            f"SELECT containing_exons, count(cds_id) AS cds_count FROM ({cds_exon_counts}) GROUP BY containing_exons ORDER BY containing_exons"
        )
    )
    for elt in cds_exon_count_hist:
        print(elt)
    assert cds_exon_count_hist[0] == (1, 168266)
    assert cds_exon_count_hist[1] == (2, 71159)
