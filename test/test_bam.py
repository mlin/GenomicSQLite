import os
import subprocess
import genomicsqlite

HERE = os.path.dirname(__file__)
BUILD = os.path.abspath(os.path.join(HERE, "..", "build"))


def sam_into_sqlite(infilename, outfilename, *options):
    cmd = (
        [os.path.join(BUILD, "loaders/sam_into_sqlite")] + list(options) + [infilename, outfilename]
    )
    print(" ".join(cmd))
    subprocess.run(
        cmd,
        check=True,
    )
    print(outfilename)
    # test CLI
    cmd = (
        "python3",
        os.path.join(HERE, "../bindings/python/genomicsqlite/__init__.py"),
        outfilename,
        "-readonly",
        "SELECT * from sqlite_master",
    )
    print(" ".join(cmd))
    subprocess.run(
        cmd,
        check=True,
    )


def test_bam(tmp_path):
    bamfile = os.path.join(HERE, "data/NA12878.chr21:20000000-22500000.bam")
    dbfile = str(tmp_path / "test.bam.sqlite")

    sam_into_sqlite(bamfile, str(dbfile), "--table-prefix", "NA12878_")

    con = genomicsqlite.connect(dbfile, read_only=True)

    count = next(con.execute("SELECT COUNT(*) FROM NA12878_reads"))[0]
    assert count == 592861

    count = next(con.execute("SELECT COUNT(DISTINCT qname) FROM NA12878_reads_seqs"))[0]
    assert count == 299205

    mq_hist = dict(
        con.execute(
            """
        SELECT mq, COUNT(*) as count FROM
            (SELECT ifnull(json_extract(tags_json, '$.MQ'),0) AS mq
             FROM NA12878_reads NATURAL JOIN NA12878_reads_tags WHERE (flag & 3840) = 0)
        GROUP BY mq ORDER BY mq DESC
    """
        )
    )
    assert (mq_hist[0], mq_hist[60]) == (2734, 520522)
