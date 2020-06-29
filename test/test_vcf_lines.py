import os
import subprocess
import genomicsqlite

HERE = os.path.dirname(__file__)
BUILD = os.path.abspath(os.path.join(HERE, "..", "build"))


def vcf_lines_into_sqlite(infilename, outfilename, *options):
    cmd = f"bgzip -dc {infilename} | {os.path.join(BUILD, 'loaders/vcf_lines_into_sqlite')} {' '.join(list(options))} {outfilename}"
    print(cmd)
    subprocess.run(cmd, check=True, shell=True)
    print(outfilename)


def test_gnomad_sites_small(tmp_path):
    dbfile = str(tmp_path / "gnomad_lines.gsql")

    vcf_lines_into_sqlite(
        os.path.join(HERE, "data/gnomad.r3.0.sites.ALDH2.vcf.bgz"),
        str(dbfile),
        "--table",
        "gnomad_vcf_lines",
    )

    con = genomicsqlite.connect(dbfile, read_only=True)
    query = "SELECT rowid, line FROM" + genomicsqlite.overlapping_genomic_ranges(
        con, "gnomad_vcf_lines"
    )
    rs671 = ("chr12", 111803912, 111804012)
    print(query)
    for expl in con.execute("EXPLAIN QUERY PLAN " + query, rs671):
        print(expl[3])
    results = list(con.execute(query, rs671))
    results_rowids = set(vt[0] for vt in results)
    assert next(vt for vt in results if vt[1] and "rs671" in vt[1])

    control = "SELECT rowid FROM gnomad_vcf_lines WHERE NOT ((POS+rlen) < ? OR POS > ?)"
    control_rowids = set(vt[0] for vt in con.execute(control, (rs671[1], rs671[2])))
    assert len(control_rowids) == 22
    assert results_rowids == control_rowids


def test_gvcf_dv(tmp_path):
    dbfile = str(tmp_path / "dv_gvcf_lines.gsql")
    vcf_lines_into_sqlite(os.path.join(HERE, "data/NA12878.dv0.8.0.chr21.g.vcf.gz"), str(dbfile))
    rows = 962897
    con = genomicsqlite.connect(dbfile, read_only=True)
    assert next(con.execute("SELECT COUNT(*) FROM vcf_lines"))[0] == rows


def test_gvcf_hc(tmp_path):
    dbfile = str(tmp_path / "gatk_gvcf_lines.gsql")
    vcf_lines_into_sqlite(
        os.path.join(HERE, "data/hc.NA12878.chr22:25000000-30000000.g.vcf.gz"), str(dbfile)
    )
    rows = 823481
    con = genomicsqlite.connect(dbfile, read_only=True)
    assert next(con.execute("SELECT COUNT(*) FROM vcf_lines"))[0] == rows


def test_pvcf_glnexus(tmp_path):
    dbfile = str(tmp_path / "glnexus_pvcf_lines.gsql")
    vcf_lines_into_sqlite(os.path.join(HERE, "data/dv_glnexus.1KGP.ALDH2.vcf.gz"), str(dbfile))
    rows = 1994
    con = genomicsqlite.connect(dbfile, read_only=True)
    assert next(con.execute("SELECT COUNT(*) FROM vcf_lines")) == (rows,)
    assert next(con.execute("SELECT SUM(rlen) FROM vcf_lines")) == (2560,)


def test_pvcf_gatk(tmp_path):
    dbfile = str(tmp_path / "gatk_pvcf_lines.gsql")
    vcf_lines_into_sqlite(os.path.join(HERE, "data/gatk.1KGP.ALDH2.vcf.gz"), str(dbfile))
    rows = 2088
    con = genomicsqlite.connect(dbfile, read_only=True)
    assert next(con.execute("SELECT COUNT(*) FROM vcf_lines"))[0] == rows
