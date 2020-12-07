import sqlite3
import pytest
import genomicsqlite


def test_parse_genomic_range():
    con = genomicsqlite.connect(":memory:")
    for (txt, chrom, begin_pos, end_pos) in [
        ("chr1:2,345-06,789", "chr1", 2344, 6789),
        ("π:1-9,223,372,036,854,775,799", "π", 0, 9223372036854775799),
    ]:
        assert next(con.execute("SELECT parse_genomic_range_sequence(?)", (txt,)))[0] == chrom
        assert next(con.execute("SELECT parse_genomic_range_begin(?)", (txt,)))[0] == begin_pos
        assert next(con.execute("SELECT parse_genomic_range_end(?)", (txt,)))[0] == end_pos

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
        with pytest.raises(sqlite3.OperationalError) as exc:
            con.execute("SELECT parse_genomic_range_sequence(?)", (txt,))
        assert "parse_genomic_range():" in str(exc.value)
        with pytest.raises(sqlite3.OperationalError):
            con.execute("SELECT parse_genomic_range_begin(?)", (txt,))
        assert "parse_genomic_range():" in str(exc.value)
        with pytest.raises(sqlite3.OperationalError):
            con.execute("SELECT parse_genomic_range_end(?)", (txt,))
        assert "parse_genomic_range():" in str(exc.value)
