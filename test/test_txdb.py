import os
import sqlite3
import subprocess
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
    outfile = txdb[:-7] + ".genomicsqlite"
    conn = sqlite3.connect(txdb)
    conn.executescript("PRAGMA page_size = 16384; PRAGMA auto_vacuum = FULL")
    outuri = next(conn.execute("SELECT genomicsqlite_uri(?,?)", (outfile, '{"unsafe_load":true}')))[
        0
    ]
    conn.execute("VACUUM INTO ?", (outuri,))
    conn.close()
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

    assert next(conn.execute("PRAGMA page_size"))[0] == 16384
