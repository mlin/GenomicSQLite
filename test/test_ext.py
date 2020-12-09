import genomicsqlite


def test_bundled_extensions():
    con = genomicsqlite.connect(":memory:")

    con.executescript(
        """
        CREATE TABLE test(json TEXT);
        INSERT INTO test(json) VALUES('{"chrom":"chr10"}');
        INSERT INTO test(json) VALUES('{"chrom":"chr2"}');
        INSERT INTO test(json) VALUES('{"chrom":"chrMT"}');
        """
    )

    results = con.execute(
        "SELECT json_extract(json,'$.chrom') AS chrom FROM test ORDER BY chrom COLLATE UINT"
    )
    assert list(results) == [("chr2",), ("chr10",), ("chrMT",)]
