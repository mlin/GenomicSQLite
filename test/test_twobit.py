import random
import math
import genomicsqlite


def test_twobit_random():
    con = genomicsqlite.connect(":memory:")

    random.seed(42)
    for seqlen in (random.randint(2, 1250) for _ in range(5000)):
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

        crumbs = next(con.execute("SELECT nucleotides_twobit(?)", (seq,)))[0]
        assert isinstance(crumbs, bytes)
        assert len(crumbs) == math.ceil(len(seq) / 4) + 1

        assert next(con.execute("SELECT twobit_length(nucleotides_twobit(?))", (seq,)))[0] == len(
            seq
        )

        query = f"SELECT {'twobit_rna' if rna else 'twobit_dna'}(nucleotides_twobit(?))"
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


def test_twobit_corner_cases():
    con = genomicsqlite.connect(":memory:")

    for nuc in "AGCTagct":
        assert next(con.execute("SELECT length(nucleotides_twobit(?))", (nuc,)))[0] == 1
        assert (
            next(con.execute("SELECT twobit_dna(nucleotides_twobit(?))", (nuc,)))[0] == nuc.upper()
        )
    assert next(con.execute("SELECT nucleotides_twobit('')"))[0] == ""
    assert next(con.execute("SELECT nucleotides_twobit('acgt 1')"))[0] == "acgt 1"
    assert next(con.execute("SELECT twobit_dna('acgt 1')"))[0] == "acgt 1"
    assert next(con.execute("SELECT twobit_dna('acgt 1',1,6)"))[0] == "acgt 1"
    assert next(con.execute("SELECT twobit_dna('acgt 1',3,3)"))[0] == "gt "
    assert next(con.execute("SELECT twobit_dna('acgt 1',-2,-3)"))[0] == "cgt"

    # exhaustively test offset/length corner cases
    for xtest in range(-9, 9):
        for ytest in range(-9, 9):
            decoded = next(
                con.execute("SELECT twobit_rna(nucleotides_twobit('gattaca'),?,?)", (xtest, ytest))
            )[0]
            control = next(con.execute("SELECT substr('GAUUACA',?,?)", (xtest, ytest)))[0]
            assert decoded == control, str((xtest, ytest))


def test_twobit_column():
    # test populating a column with mixed BLOB and TEXT values
    con = genomicsqlite.connect(":memory:")

    con.executescript("CREATE TABLE test(test_twobit BLOB)")
    for elt in list("Tu") + ["foo", "bar", "gATuaCa"]:
        con.execute("INSERT INTO test(test_twobit) VALUES(nucleotides_twobit(?))", (elt,))

    column = list(con.execute("SELECT test_twobit FROM test"))
    assert isinstance(column[0][0], bytes), str([type(x[0]) for x in column])
    assert isinstance(column[-1][0], bytes)
    assert isinstance(column[-2][0], str)
    assert column[-2][0] == "bar"

    assert list(con.execute("SELECT twobit_dna(test_twobit) FROM test")) == [
        ("T",),
        ("T",),
        ("foo",),
        ("bar",),
        ("GATTACA",),
    ]
