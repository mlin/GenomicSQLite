import random
import math
import genomicsqlite


def test_twobit(tmp_path):
    con = genomicsqlite.connect(":memory:")

    random.seed(42)
    for seqlen in (random.randint(1, 1250) for _ in range(5000)):
        rna = random.choice((False, True))

        nucs = (
            ("a", "A", "g", "G", "c", "C", "u", "U")
            if rna
            else ("a", "A", "g", "G", "c", "C", "t", "T")
        )
        seq = "".join(random.choice(nucs) for _ in range(seqlen))
        assert (rna and ("t" not in seq and "T" not in seq)) or (
            not rna and ("u" not in seq and "U" not in seq)
        )

        crumbs = next(con.execute("SELECT dna_twobit(?)", (seq,)))[0]
        assert isinstance(crumbs, bytes)
        assert len(crumbs) == math.ceil(len(seq) / 4) + 1

        query = f"SELECT {'twobit_rna' if rna else 'twobit_dna'}(dna_twobit(?))"
        decoded = next(con.execute(query, (seq,)))[0]
        assert decoded == seq.upper()

        # test built-in substr
        sub_ofs = random.randint(0, seqlen)
        sub_len = random.randint(0, seqlen)
        decoded = next(con.execute("SELECT twobit_dna(?,?,?)", (seq, sub_ofs, sub_len)))[0]
        control = next(con.execute("SELECT substr(twobit_dna(?),?,?)", (seq, sub_ofs, sub_len)))[0]
        assert decoded == control

        # test with negative offset/length -- https://sqlite.org/lang_corefunc.html#substr
        decoded = next(con.execute("SELECT twobit_dna(?,?,?)", (seq, 0 - sub_ofs, sub_len)))[0]
        control = next(
            con.execute("SELECT substr(twobit_dna(?),?,?)", (seq, 0 - sub_ofs, sub_len))
        )[0]
        assert decoded == control
        decoded = next(con.execute("SELECT twobit_dna(?,?,?)", (seq, sub_ofs, 0 - sub_len)))[0]
        control = next(
            con.execute("SELECT substr(twobit_dna(?),?,?)", (seq, sub_ofs, 0 - sub_len))
        )[0]
        assert decoded == control

    assert next(con.execute("SELECT dna_twobit('acgt 1')"))[0] == "acgt 1"
    assert next(con.execute("SELECT twobit_dna('acgt 1')"))[0] == "acgt 1"
    assert next(con.execute("SELECT twobit_dna('acgt 1',1,6)"))[0] == "acgt 1"
    assert next(con.execute("SELECT twobit_dna('acgt 1',3,3)"))[0] == "gt "
    assert next(con.execute("SELECT twobit_dna('acgt 1',-2,-3)"))[0] == "cgt"

    # exhaustively test offset/length corner cases
    for x in range(-9, 9):
        for y in range(-9, 9):
            decoded = next(con.execute("SELECT twobit_rna(dna_twobit('gattaca'),?,?)", (x, y)))[0]
            control = next(con.execute("SELECT substr('GAUUACA',?,?)", (x, y)))[0]
            assert decoded == control, str((x, y))
