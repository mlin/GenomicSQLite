import genomicsqlite
import sqlite3
import pytest


def test_parse_genomic_range():
    con = genomicsqlite.connect(":memory:")
    q = "SELECT parse_genomic_range(?,?)"
    for (txt, chrom, begin_pos, end_pos) in [
        ("chr1:2,345-06,789", "chr1", 2344, 6789),
        ("π:1-9,223,372,036,854,775,799", "π", 0, 9223372036854775799),
    ]:
        assert next(con.execute(q, (txt, 1)))[0] == chrom
        assert next(con.execute(q, (txt, 2)))[0] == begin_pos
        assert next(con.execute(q, (txt, 3)))[0] == end_pos

    for txt in [
        "",
        ":",
        "-",
        ":-",
        ":1-2",
        "chr1",
        "chr1:0-1",
        "chr1:1,234",
        "chr1:1,234-",
        "chr1:1,234-5,67",
        "chr1 :2,345-06,789",
        "chr1:2,345-06,789\t",
        "chr1:2345-deadbeef",
        "chr1:1-9,223,372,036,854,775,800",
    ]:
        with pytest.raises(sqlite3.OperationalError):
            con.execute(q, (txt, 1))

    with pytest.raises(sqlite3.OperationalError):
        con.execute(q, ("chr1:2-3", 0))
