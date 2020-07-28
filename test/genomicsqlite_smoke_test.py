#!/usr/bin/env python3
"""
Display platform, environment, & version info for debugging GenomicSQLite installation. Load a
small test database and check GRI query plan on it.
"""
import sys
import time
import os
import subprocess
import traceback
import platform
import textwrap


def main():
    header = f"""\
        -- GenomicSQLite smoke test --
        timestamp: {time.asctime(time.gmtime())}
        platform: {platform.platform()}
        uname: {os.uname()}
        python: {platform.python_implementation()} {platform.python_version()}"""
    print(textwrap.dedent(header))

    print("cpu", end="")
    try:
        if platform.system() == "Linux":
            with open("/proc/cpuinfo") as cpuinfo:
                modelname = (
                    line.strip().replace("\t", "")
                    for line in cpuinfo
                    if line.lower().strip().startswith("model name")
                )
                print(" " + next(modelname, ": ???"))
        elif platform.system() == "Darwin":
            sysctl = subprocess.run(
                ["sysctl", "-n", "machdep.cpu.brand_string"],
                check=True,
                universal_newlines=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            if sysctl.returncode == 0 and sysctl.stdout.strip():
                print(": " + sysctl.stdout.strip())
            else:
                print(": ???")
        else:
            print(": ???")
    except Exception:
        print(": ???")

    env_keys = [
        k
        for k in os.environ
        if ("genomicsqlite" in k.lower())
        or k in ("PATH", "PYTHONPATH", "LD_LIBRARY_PATH", "DYLD_LIBRARY_PATH")
    ]
    if env_keys:
        print("environment:")
        for k in env_keys:
            print(f"  {k}={os.environ[k]}")

    print("sqlite3: ", end="")
    import sqlite3

    conn = sqlite3.connect(":memory:")
    print(next(conn.execute("SELECT sqlite_version()"))[0])

    print("genomicsqlite: ", end="")
    try:
        import genomicsqlite
    except ImportError as exn:
        print(f"\n\nUnable to import genomicsqlite: {exn}")
        print("The Python genomicsqlite package may need to be installed via pip3 or conda,")
        print("or its location may need to be added to PYTHONPATH.")
        sys.exit(1)

    print(genomicsqlite.__version__)
    print(f"genomicsqlite library: {genomicsqlite._DLL}")

    dbfile = os.path.join(
        os.environ.get("TMPDIR", "/tmp"), f"genomicsqlite_smoke_test.{time.monotonic()}"
    )
    print(f"\ntest database: {dbfile}")
    try:
        dbconn = genomicsqlite.connect(dbfile)
        for (table, bed) in (("exons1", _EXONS1), ("exons2", _EXONS2)):
            dbconn.execute(
                f"CREATE TABLE {table}(chrom TEXT, pos INTEGER, end INTEGER, id TEXT PRIMARY KEY)"
            )
            for line in bed.strip().split("\n"):
                line = line.split("\t")
                dbconn.execute(
                    f"INSERT INTO {table}(chrom,pos,end,id) VALUES(?,?,?,?)",
                    (line[0], int(line[1]) - 1, int(line[2]), line[3]),
                )
            dbconn.executescript(
                genomicsqlite.create_genomic_range_index_sql(dbconn, table, "chrom", "pos", "end")
            )
        dbconn.commit()

        query = (
            "SELECT exons1.id, exons2.id FROM exons1 LEFT JOIN exons2 ON exons2._rowid_ IN\n"
            + textwrap.indent(
                genomicsqlite.genomic_range_rowids_sql(
                    dbconn, "exons2", "exons1.chrom", "exons1.pos", "exons1.end"
                ),
                " ",
            )
            + "\nORDER BY exons1.id, exons2.id"
        )
        print("GRI query:\n" + textwrap.indent(query, "  "))
        print("GRI query plan:")
        gri_constraints = 0
        for expl in dbconn.execute("EXPLAIN QUERY PLAN " + query):
            print("  " + expl[3])
            if (
                "((_gri_rid,_gri_lvl,_gri_beg)>(?,?,?) AND (_gri_rid,_gri_lvl,_gri_beg)<(?,?,?))"
                in expl[3]
            ):
                gri_constraints += 1

        results = list(dbconn.execute(query))
        control = "SELECT exons1.id, exons2.id FROM exons1 LEFT JOIN exons2 NOT INDEXED ON NOT (exons2.end < exons1.pos OR exons2.pos > exons1.end) ORDER BY exons1.id, exons2.id"
        control_results = list(dbconn.execute(control))
        assert len(control_results) == 1139
        assert results == control_results

        if gri_constraints != 3:
            print("GRI query opcodes:")
            for expl in dbconn.execute("EXPLAIN " + query):
                print("  " + str(expl))

            print(
                "\n** WARNING: GRI yielded correct results, but with a possibly suboptimal query plan."
            )
            print(
                "** Please redirect this log to a file and send to maintainers @ https://github.com/mlin/GenomicSQLite\n"
            )

            sys.exit(2)

        dbconn.close()
        print("\nGenomicSQLite smoke test OK =)\n")
    finally:
        os.remove(dbfile)


_EXONS1 = """
chr17	43044294	43045802	ENST00000352993.7_exon_0_0_chr17_43044295_r	0	-
chr17	43047642	43047703	ENST00000352993.7_exon_1_0_chr17_43047643_r	0	-
chr17	43049120	43049194	ENST00000352993.7_exon_2_0_chr17_43049121_r	0	-
chr17	43051062	43051117	ENST00000352993.7_exon_3_0_chr17_43051063_r	0	-
chr17	43057051	43057135	ENST00000352993.7_exon_4_0_chr17_43057052_r	0	-
chr17	43063332	43063373	ENST00000352993.7_exon_5_0_chr17_43063333_r	0	-
chr17	43063873	43063951	ENST00000352993.7_exon_6_0_chr17_43063874_r	0	-
chr17	43067607	43067695	ENST00000352993.7_exon_7_0_chr17_43067608_r	0	-
chr17	43070927	43071238	ENST00000352993.7_exon_8_0_chr17_43070928_r	0	-
chr17	43074330	43074521	ENST00000352993.7_exon_9_0_chr17_43074331_r	0	-
chr17	43076487	43076614	ENST00000352993.7_exon_10_0_chr17_43076488_r	0	-
chr17	43082403	43082575	ENST00000352993.7_exon_11_0_chr17_43082404_r	0	-
chr17	43090943	43091032	ENST00000352993.7_exon_12_0_chr17_43090944_r	0	-
chr17	43095845	43095922	ENST00000352993.7_exon_13_0_chr17_43095846_r	0	-
chr17	43097243	43097289	ENST00000352993.7_exon_14_0_chr17_43097244_r	0	-
chr17	43099774	43099880	ENST00000352993.7_exon_15_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000352993.7_exon_16_0_chr17_43104122_r	0	-
chr17	43104867	43104956	ENST00000352993.7_exon_17_0_chr17_43104868_r	0	-
chr17	43106455	43106533	ENST00000352993.7_exon_18_0_chr17_43106456_r	0	-
chr17	43115725	43115779	ENST00000352993.7_exon_19_0_chr17_43115726_r	0	-
chr17	43124016	43124115	ENST00000352993.7_exon_20_0_chr17_43124017_r	0	-
chr17	43125270	43125370	ENST00000352993.7_exon_21_0_chr17_43125271_r	0	-
chr17	43044294	43045802	ENST00000357654.8_exon_0_0_chr17_43044295_r	0	-
chr17	43047642	43047703	ENST00000357654.8_exon_1_0_chr17_43047643_r	0	-
chr17	43049120	43049194	ENST00000357654.8_exon_2_0_chr17_43049121_r	0	-
chr17	43051062	43051117	ENST00000357654.8_exon_3_0_chr17_43051063_r	0	-
chr17	43057051	43057135	ENST00000357654.8_exon_4_0_chr17_43057052_r	0	-
chr17	43063332	43063373	ENST00000357654.8_exon_5_0_chr17_43063333_r	0	-
chr17	43063873	43063951	ENST00000357654.8_exon_6_0_chr17_43063874_r	0	-
chr17	43067607	43067695	ENST00000357654.8_exon_7_0_chr17_43067608_r	0	-
chr17	43070927	43071238	ENST00000357654.8_exon_8_0_chr17_43070928_r	0	-
chr17	43074330	43074521	ENST00000357654.8_exon_9_0_chr17_43074331_r	0	-
chr17	43076487	43076614	ENST00000357654.8_exon_10_0_chr17_43076488_r	0	-
chr17	43082403	43082575	ENST00000357654.8_exon_11_0_chr17_43082404_r	0	-
chr17	43090943	43091032	ENST00000357654.8_exon_12_0_chr17_43090944_r	0	-
chr17	43091434	43094860	ENST00000357654.8_exon_13_0_chr17_43091435_r	0	-
chr17	43095845	43095922	ENST00000357654.8_exon_14_0_chr17_43095846_r	0	-
chr17	43097243	43097289	ENST00000357654.8_exon_15_0_chr17_43097244_r	0	-
chr17	43099774	43099880	ENST00000357654.8_exon_16_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000357654.8_exon_17_0_chr17_43104122_r	0	-
chr17	43104867	43104956	ENST00000357654.8_exon_18_0_chr17_43104868_r	0	-
chr17	43106455	43106533	ENST00000357654.8_exon_19_0_chr17_43106456_r	0	-
chr17	43115725	43115779	ENST00000357654.8_exon_20_0_chr17_43115726_r	0	-
chr17	43124016	43124115	ENST00000357654.8_exon_21_0_chr17_43124017_r	0	-
chr17	43125270	43125483	ENST00000357654.8_exon_22_0_chr17_43125271_r	0	-
chr17	43044294	43045802	ENST00000471181.7_exon_0_0_chr17_43044295_r	0	-
chr17	43047642	43047703	ENST00000471181.7_exon_1_0_chr17_43047643_r	0	-
chr17	43049120	43049194	ENST00000471181.7_exon_2_0_chr17_43049121_r	0	-
chr17	43051062	43051117	ENST00000471181.7_exon_3_0_chr17_43051063_r	0	-
chr17	43057051	43057135	ENST00000471181.7_exon_4_0_chr17_43057052_r	0	-
chr17	43063332	43063373	ENST00000471181.7_exon_5_0_chr17_43063333_r	0	-
chr17	43063873	43063951	ENST00000471181.7_exon_6_0_chr17_43063874_r	0	-
chr17	43067607	43067695	ENST00000471181.7_exon_7_0_chr17_43067608_r	0	-
chr17	43070927	43071238	ENST00000471181.7_exon_8_0_chr17_43070928_r	0	-
chr17	43074330	43074521	ENST00000471181.7_exon_9_0_chr17_43074331_r	0	-
chr17	43076487	43076611	ENST00000471181.7_exon_10_0_chr17_43076488_r	0	-
chr17	43079333	43079399	ENST00000471181.7_exon_11_0_chr17_43079334_r	0	-
chr17	43082403	43082575	ENST00000471181.7_exon_12_0_chr17_43082404_r	0	-
chr17	43090943	43091032	ENST00000471181.7_exon_13_0_chr17_43090944_r	0	-
chr17	43091434	43094860	ENST00000471181.7_exon_14_0_chr17_43091435_r	0	-
chr17	43095845	43095922	ENST00000471181.7_exon_15_0_chr17_43095846_r	0	-
chr17	43097243	43097289	ENST00000471181.7_exon_16_0_chr17_43097244_r	0	-
chr17	43099774	43099880	ENST00000471181.7_exon_17_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000471181.7_exon_18_0_chr17_43104122_r	0	-
chr17	43104867	43104956	ENST00000471181.7_exon_19_0_chr17_43104868_r	0	-
chr17	43106455	43106533	ENST00000471181.7_exon_20_0_chr17_43106456_r	0	-
chr17	43115725	43115779	ENST00000471181.7_exon_21_0_chr17_43115726_r	0	-
chr17	43124016	43124115	ENST00000471181.7_exon_22_0_chr17_43124017_r	0	-
chr17	43125270	43125483	ENST00000471181.7_exon_23_0_chr17_43125271_r	0	-
chr17	43044804	43045802	ENST00000468300.5_exon_0_0_chr17_43044805_r	0	-
chr17	43047642	43047703	ENST00000468300.5_exon_1_0_chr17_43047643_r	0	-
chr17	43051062	43051117	ENST00000468300.5_exon_2_0_chr17_43051063_r	0	-
chr17	43057051	43057135	ENST00000468300.5_exon_3_0_chr17_43057052_r	0	-
chr17	43063332	43063373	ENST00000468300.5_exon_4_0_chr17_43063333_r	0	-
chr17	43063873	43063951	ENST00000468300.5_exon_5_0_chr17_43063874_r	0	-
chr17	43067607	43067695	ENST00000468300.5_exon_6_0_chr17_43067608_r	0	-
chr17	43070927	43071238	ENST00000468300.5_exon_7_0_chr17_43070928_r	0	-
chr17	43074330	43074521	ENST00000468300.5_exon_8_0_chr17_43074331_r	0	-
chr17	43076487	43076611	ENST00000468300.5_exon_9_0_chr17_43076488_r	0	-
chr17	43082403	43082575	ENST00000468300.5_exon_10_0_chr17_43082404_r	0	-
chr17	43090943	43091032	ENST00000468300.5_exon_11_0_chr17_43090944_r	0	-
chr17	43094743	43094860	ENST00000468300.5_exon_12_0_chr17_43094744_r	0	-
chr17	43095845	43095922	ENST00000468300.5_exon_13_0_chr17_43095846_r	0	-
chr17	43097243	43097289	ENST00000468300.5_exon_14_0_chr17_43097244_r	0	-
chr17	43099774	43099880	ENST00000468300.5_exon_15_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000468300.5_exon_16_0_chr17_43104122_r	0	-
chr17	43104867	43104956	ENST00000468300.5_exon_17_0_chr17_43104868_r	0	-
chr17	43106455	43106533	ENST00000468300.5_exon_18_0_chr17_43106456_r	0	-
chr17	43115725	43115779	ENST00000468300.5_exon_19_0_chr17_43115726_r	0	-
chr17	43124016	43124115	ENST00000468300.5_exon_20_0_chr17_43124017_r	0	-
chr17	43125276	43125451	ENST00000468300.5_exon_21_0_chr17_43125277_r	0	-
chr17	43045085	43045802	ENST00000644379.1_exon_0_0_chr17_43045086_r	0	-
chr17	43047642	43047703	ENST00000644379.1_exon_1_0_chr17_43047643_r	0	-
chr17	43049120	43049194	ENST00000644379.1_exon_2_0_chr17_43049121_r	0	-
chr17	43051062	43051117	ENST00000644379.1_exon_3_0_chr17_43051063_r	0	-
chr17	43057051	43057135	ENST00000644379.1_exon_4_0_chr17_43057052_r	0	-
chr17	43063332	43063373	ENST00000644379.1_exon_5_0_chr17_43063333_r	0	-
chr17	43063873	43063951	ENST00000644379.1_exon_6_0_chr17_43063874_r	0	-
chr17	43067607	43067695	ENST00000644379.1_exon_7_0_chr17_43067608_r	0	-
chr17	43070927	43071238	ENST00000644379.1_exon_8_0_chr17_43070928_r	0	-
chr17	43074330	43074521	ENST00000644379.1_exon_9_0_chr17_43074331_r	0	-
chr17	43076487	43076614	ENST00000644379.1_exon_10_0_chr17_43076488_r	0	-
chr17	43079333	43079399	ENST00000644379.1_exon_11_0_chr17_43079334_r	0	-
chr17	43082403	43082575	ENST00000644379.1_exon_12_0_chr17_43082404_r	0	-
chr17	43090943	43091032	ENST00000644379.1_exon_13_0_chr17_43090944_r	0	-
chr17	43091434	43091851	ENST00000644379.1_exon_14_0_chr17_43091435_r	0	-
chr17	43045562	43045802	ENST00000586385.5_exon_0_0_chr17_43045563_r	0	-
chr17	43047642	43047703	ENST00000586385.5_exon_1_0_chr17_43047643_r	0	-
chr17	43049120	43049194	ENST00000586385.5_exon_2_0_chr17_43049121_r	0	-
chr17	43051062	43051117	ENST00000586385.5_exon_3_0_chr17_43051063_r	0	-
chr17	43057051	43057135	ENST00000586385.5_exon_4_0_chr17_43057052_r	0	-
chr17	43063332	43063373	ENST00000586385.5_exon_5_0_chr17_43063333_r	0	-
chr17	43063873	43063951	ENST00000586385.5_exon_6_0_chr17_43063874_r	0	-
chr17	43125181	43125329	ENST00000586385.5_exon_7_0_chr17_43125182_r	0	-
chr17	43045562	43045802	ENST00000591534.5_exon_0_0_chr17_43045563_r	0	-
chr17	43047642	43047703	ENST00000591534.5_exon_1_0_chr17_43047643_r	0	-
chr17	43049120	43049194	ENST00000591534.5_exon_2_0_chr17_43049121_r	0	-
chr17	43051062	43051117	ENST00000591534.5_exon_3_0_chr17_43051063_r	0	-
chr17	43057051	43057135	ENST00000591534.5_exon_4_0_chr17_43057052_r	0	-
chr17	43063332	43063373	ENST00000591534.5_exon_5_0_chr17_43063333_r	0	-
chr17	43063873	43063951	ENST00000591534.5_exon_6_0_chr17_43063874_r	0	-
chr17	43067607	43067695	ENST00000591534.5_exon_7_0_chr17_43067608_r	0	-
chr17	43070927	43071238	ENST00000591534.5_exon_8_0_chr17_43070928_r	0	-
chr17	43074330	43074521	ENST00000591534.5_exon_9_0_chr17_43074331_r	0	-
chr17	43125270	43125329	ENST00000591534.5_exon_10_0_chr17_43125271_r	0	-
chr17	43045562	43045802	ENST00000591849.5_exon_0_0_chr17_43045563_r	0	-
chr17	43047642	43047703	ENST00000591849.5_exon_1_0_chr17_43047643_r	0	-
chr17	43049120	43049194	ENST00000591849.5_exon_2_0_chr17_43049121_r	0	-
chr17	43050061	43050190	ENST00000591849.5_exon_3_0_chr17_43050062_r	0	-
chr17	43125270	43125329	ENST00000591849.5_exon_4_0_chr17_43125271_r	0	-
chr17	43045628	43045802	ENST00000493795.5_exon_0_0_chr17_43045629_r	0	-
chr17	43047642	43047703	ENST00000493795.5_exon_1_0_chr17_43047643_r	0	-
chr17	43049120	43049194	ENST00000493795.5_exon_2_0_chr17_43049121_r	0	-
chr17	43051062	43051117	ENST00000493795.5_exon_3_0_chr17_43051063_r	0	-
chr17	43057051	43057135	ENST00000493795.5_exon_4_0_chr17_43057052_r	0	-
chr17	43063332	43063373	ENST00000493795.5_exon_5_0_chr17_43063333_r	0	-
chr17	43063873	43063951	ENST00000493795.5_exon_6_0_chr17_43063874_r	0	-
chr17	43067607	43067695	ENST00000493795.5_exon_7_0_chr17_43067608_r	0	-
chr17	43070927	43071238	ENST00000493795.5_exon_8_0_chr17_43070928_r	0	-
chr17	43074330	43074521	ENST00000493795.5_exon_9_0_chr17_43074331_r	0	-
chr17	43076487	43076614	ENST00000493795.5_exon_10_0_chr17_43076488_r	0	-
chr17	43082403	43082575	ENST00000493795.5_exon_11_0_chr17_43082404_r	0	-
chr17	43090943	43091032	ENST00000493795.5_exon_12_0_chr17_43090944_r	0	-
chr17	43091434	43094860	ENST00000493795.5_exon_13_0_chr17_43091435_r	0	-
chr17	43095845	43095922	ENST00000493795.5_exon_14_0_chr17_43095846_r	0	-
chr17	43097243	43097289	ENST00000493795.5_exon_15_0_chr17_43097244_r	0	-
chr17	43099774	43099880	ENST00000493795.5_exon_16_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000493795.5_exon_17_0_chr17_43104122_r	0	-
chr17	43104867	43104956	ENST00000493795.5_exon_18_0_chr17_43104868_r	0	-
chr17	43106455	43106533	ENST00000493795.5_exon_19_0_chr17_43106456_r	0	-
chr17	43124016	43124115	ENST00000493795.5_exon_20_0_chr17_43124017_r	0	-
chr17	43125276	43125402	ENST00000493795.5_exon_21_0_chr17_43125277_r	0	-
chr17	43045677	43045802	ENST00000461221.5_exon_0_0_chr17_43045678_r	0	-
chr17	43047642	43047703	ENST00000461221.5_exon_1_0_chr17_43047643_r	0	-
chr17	43049120	43049194	ENST00000461221.5_exon_2_0_chr17_43049121_r	0	-
chr17	43051062	43051117	ENST00000461221.5_exon_3_0_chr17_43051063_r	0	-
chr17	43057051	43057135	ENST00000461221.5_exon_4_0_chr17_43057052_r	0	-
chr17	43063332	43063373	ENST00000461221.5_exon_5_0_chr17_43063333_r	0	-
chr17	43063873	43063951	ENST00000461221.5_exon_6_0_chr17_43063874_r	0	-
chr17	43067607	43067695	ENST00000461221.5_exon_7_0_chr17_43067608_r	0	-
chr17	43070927	43071238	ENST00000461221.5_exon_8_0_chr17_43070928_r	0	-
chr17	43074330	43074521	ENST00000461221.5_exon_9_0_chr17_43074331_r	0	-
chr17	43076487	43076614	ENST00000461221.5_exon_10_0_chr17_43076488_r	0	-
chr17	43082403	43082575	ENST00000461221.5_exon_11_0_chr17_43082404_r	0	-
chr17	43090943	43091032	ENST00000461221.5_exon_12_0_chr17_43090944_r	0	-
chr17	43091434	43094860	ENST00000461221.5_exon_13_0_chr17_43091435_r	0	-
chr17	43095845	43095922	ENST00000461221.5_exon_14_0_chr17_43095846_r	0	-
chr17	43097243	43097289	ENST00000461221.5_exon_15_0_chr17_43097244_r	0	-
chr17	43099774	43099877	ENST00000461221.5_exon_16_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000461221.5_exon_17_0_chr17_43104122_r	0	-
chr17	43104867	43104956	ENST00000461221.5_exon_18_0_chr17_43104868_r	0	-
chr17	43106477	43106533	ENST00000461221.5_exon_19_0_chr17_43106478_r	0	-
chr17	43115725	43115779	ENST00000461221.5_exon_20_0_chr17_43115726_r	0	-
chr17	43124016	43124115	ENST00000461221.5_exon_21_0_chr17_43124017_r	0	-
chr17	43125181	43125288	ENST00000461221.5_exon_22_0_chr17_43125182_r	0	-
"""

_EXONS2 = """
chr17	43045677	43045802	ENST00000491747.6_exon_0_0_chr17_43045678_r	0	-
chr17	43047642	43047703	ENST00000491747.6_exon_1_0_chr17_43047643_r	0	-
chr17	43049120	43049194	ENST00000491747.6_exon_2_0_chr17_43049121_r	0	-
chr17	43051062	43051117	ENST00000491747.6_exon_3_0_chr17_43051063_r	0	-
chr17	43057051	43057135	ENST00000491747.6_exon_4_0_chr17_43057052_r	0	-
chr17	43063332	43063373	ENST00000491747.6_exon_5_0_chr17_43063333_r	0	-
chr17	43063873	43063951	ENST00000491747.6_exon_6_0_chr17_43063874_r	0	-
chr17	43067607	43067695	ENST00000491747.6_exon_7_0_chr17_43067608_r	0	-
chr17	43070927	43071238	ENST00000491747.6_exon_8_0_chr17_43070928_r	0	-
chr17	43074330	43074521	ENST00000491747.6_exon_9_0_chr17_43074331_r	0	-
chr17	43076487	43076611	ENST00000491747.6_exon_10_0_chr17_43076488_r	0	-
chr17	43082403	43082575	ENST00000491747.6_exon_11_0_chr17_43082404_r	0	-
chr17	43090943	43091032	ENST00000491747.6_exon_12_0_chr17_43090944_r	0	-
chr17	43094743	43094860	ENST00000491747.6_exon_13_0_chr17_43094744_r	0	-
chr17	43095845	43095922	ENST00000491747.6_exon_14_0_chr17_43095846_r	0	-
chr17	43097243	43097289	ENST00000491747.6_exon_15_0_chr17_43097244_r	0	-
chr17	43099774	43099880	ENST00000491747.6_exon_16_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000491747.6_exon_17_0_chr17_43104122_r	0	-
chr17	43104867	43104956	ENST00000491747.6_exon_18_0_chr17_43104868_r	0	-
chr17	43106455	43106533	ENST00000491747.6_exon_19_0_chr17_43106456_r	0	-
chr17	43115725	43115779	ENST00000491747.6_exon_20_0_chr17_43115726_r	0	-
chr17	43124016	43124115	ENST00000491747.6_exon_21_0_chr17_43124017_r	0	-
chr17	43125276	43125356	ENST00000491747.6_exon_22_0_chr17_43125277_r	0	-
chr17	43063343	43063373	ENST00000484087.5_exon_0_0_chr17_43063344_r	0	-
chr17	43063873	43063951	ENST00000484087.5_exon_1_0_chr17_43063874_r	0	-
chr17	43067607	43067695	ENST00000484087.5_exon_2_0_chr17_43067608_r	0	-
chr17	43070927	43071238	ENST00000484087.5_exon_3_0_chr17_43070928_r	0	-
chr17	43074330	43074521	ENST00000484087.5_exon_4_0_chr17_43074331_r	0	-
chr17	43076487	43076614	ENST00000484087.5_exon_5_0_chr17_43076488_r	0	-
chr17	43082403	43082575	ENST00000484087.5_exon_6_0_chr17_43082404_r	0	-
chr17	43090943	43091032	ENST00000484087.5_exon_7_0_chr17_43090944_r	0	-
chr17	43094743	43094860	ENST00000484087.5_exon_8_0_chr17_43094744_r	0	-
chr17	43099774	43099877	ENST00000484087.5_exon_9_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000484087.5_exon_10_0_chr17_43104122_r	0	-
chr17	43104867	43104916	ENST00000484087.5_exon_11_0_chr17_43104868_r	0	-
chr17	43063343	43063373	ENST00000478531.5_exon_0_0_chr17_43063344_r	0	-
chr17	43063873	43063951	ENST00000478531.5_exon_1_0_chr17_43063874_r	0	-
chr17	43067607	43067695	ENST00000478531.5_exon_2_0_chr17_43067608_r	0	-
chr17	43070927	43071238	ENST00000478531.5_exon_3_0_chr17_43070928_r	0	-
chr17	43074330	43074521	ENST00000478531.5_exon_4_0_chr17_43074331_r	0	-
chr17	43076487	43076614	ENST00000478531.5_exon_5_0_chr17_43076488_r	0	-
chr17	43082403	43082575	ENST00000478531.5_exon_6_0_chr17_43082404_r	0	-
chr17	43090943	43091032	ENST00000478531.5_exon_7_0_chr17_43090944_r	0	-
chr17	43094743	43094860	ENST00000478531.5_exon_8_0_chr17_43094744_r	0	-
chr17	43095845	43095922	ENST00000478531.5_exon_9_0_chr17_43095846_r	0	-
chr17	43097243	43097289	ENST00000478531.5_exon_10_0_chr17_43097244_r	0	-
chr17	43099774	43099877	ENST00000478531.5_exon_11_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000478531.5_exon_12_0_chr17_43104122_r	0	-
chr17	43104867	43104956	ENST00000478531.5_exon_13_0_chr17_43104868_r	0	-
chr17	43106455	43106533	ENST00000478531.5_exon_14_0_chr17_43106456_r	0	-
chr17	43115725	43115779	ENST00000478531.5_exon_15_0_chr17_43115726_r	0	-
chr17	43124016	43124115	ENST00000478531.5_exon_16_0_chr17_43124017_r	0	-
chr17	43125276	43125359	ENST00000478531.5_exon_17_0_chr17_43125277_r	0	-
chr17	43063359	43063373	ENST00000493919.5_exon_0_0_chr17_43063360_r	0	-
chr17	43063873	43063951	ENST00000493919.5_exon_1_0_chr17_43063874_r	0	-
chr17	43067607	43067695	ENST00000493919.5_exon_2_0_chr17_43067608_r	0	-
chr17	43070927	43071238	ENST00000493919.5_exon_3_0_chr17_43070928_r	0	-
chr17	43074330	43074521	ENST00000493919.5_exon_4_0_chr17_43074331_r	0	-
chr17	43076487	43076614	ENST00000493919.5_exon_5_0_chr17_43076488_r	0	-
chr17	43082403	43082575	ENST00000493919.5_exon_6_0_chr17_43082404_r	0	-
chr17	43090943	43091032	ENST00000493919.5_exon_7_0_chr17_43090944_r	0	-
chr17	43094743	43094860	ENST00000493919.5_exon_8_0_chr17_43094744_r	0	-
chr17	43095845	43095922	ENST00000493919.5_exon_9_0_chr17_43095846_r	0	-
chr17	43097243	43097289	ENST00000493919.5_exon_10_0_chr17_43097244_r	0	-
chr17	43099774	43099880	ENST00000493919.5_exon_11_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000493919.5_exon_12_0_chr17_43104122_r	0	-
chr17	43104867	43104956	ENST00000493919.5_exon_13_0_chr17_43104868_r	0	-
chr17	43106455	43106533	ENST00000493919.5_exon_14_0_chr17_43106456_r	0	-
chr17	43124016	43124115	ENST00000493919.5_exon_15_0_chr17_43124017_r	0	-
chr17	43125276	43125402	ENST00000493919.5_exon_16_0_chr17_43125277_r	0	-
chr17	43076487	43076614	ENST00000487825.5_exon_0_0_chr17_43076488_r	0	-
chr17	43082403	43082575	ENST00000487825.5_exon_1_0_chr17_43082404_r	0	-
chr17	43090943	43091032	ENST00000487825.5_exon_2_0_chr17_43090944_r	0	-
chr17	43094743	43094860	ENST00000487825.5_exon_3_0_chr17_43094744_r	0	-
chr17	43099774	43099880	ENST00000487825.5_exon_4_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000487825.5_exon_5_0_chr17_43104122_r	0	-
chr17	43104867	43104916	ENST00000487825.5_exon_6_0_chr17_43104868_r	0	-
chr17	43076536	43076611	ENST00000461574.1_exon_0_0_chr17_43076537_r	0	-
chr17	43082403	43082575	ENST00000461574.1_exon_1_0_chr17_43082404_r	0	-
chr17	43090943	43091032	ENST00000461574.1_exon_2_0_chr17_43090944_r	0	-
chr17	43091434	43091824	ENST00000461574.1_exon_3_0_chr17_43091435_r	0	-
chr17	43091097	43094860	ENST00000354071.7_exon_0_0_chr17_43091098_r	0	-
chr17	43095845	43095922	ENST00000354071.7_exon_1_0_chr17_43095846_r	0	-
chr17	43097243	43097289	ENST00000354071.7_exon_2_0_chr17_43097244_r	0	-
chr17	43099774	43099880	ENST00000354071.7_exon_3_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000354071.7_exon_4_0_chr17_43104122_r	0	-
chr17	43104867	43104956	ENST00000354071.7_exon_5_0_chr17_43104868_r	0	-
chr17	43106455	43106533	ENST00000354071.7_exon_6_0_chr17_43106456_r	0	-
chr17	43115725	43115779	ENST00000354071.7_exon_7_0_chr17_43115726_r	0	-
chr17	43124016	43124115	ENST00000354071.7_exon_8_0_chr17_43124017_r	0	-
chr17	43125270	43125315	ENST00000354071.7_exon_9_0_chr17_43125271_r	0	-
chr17	43093012	43094860	ENST00000634433.1_exon_0_0_chr17_43093013_r	0	-
chr17	43099774	43099880	ENST00000634433.1_exon_1_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000634433.1_exon_2_0_chr17_43104122_r	0	-
chr17	43104867	43104956	ENST00000634433.1_exon_3_0_chr17_43104868_r	0	-
chr17	43106455	43106533	ENST00000634433.1_exon_4_0_chr17_43106456_r	0	-
chr17	43115725	43115779	ENST00000634433.1_exon_5_0_chr17_43115726_r	0	-
chr17	43124016	43124115	ENST00000634433.1_exon_6_0_chr17_43124017_r	0	-
chr17	43170125	43170245	ENST00000634433.1_exon_7_0_chr17_43170126_r	0	-
chr17	43093569	43094860	ENST00000412061.3_exon_0_0_chr17_43093570_r	0	-
chr17	43095845	43095866	ENST00000412061.3_exon_1_0_chr17_43095846_r	0	-
chr17	43093583	43094860	ENST00000470026.5_exon_0_0_chr17_43093584_r	0	-
chr17	43095845	43095922	ENST00000470026.5_exon_1_0_chr17_43095846_r	0	-
chr17	43097243	43097289	ENST00000470026.5_exon_2_0_chr17_43097244_r	0	-
chr17	43099774	43099880	ENST00000470026.5_exon_3_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000470026.5_exon_4_0_chr17_43104122_r	0	-
chr17	43104867	43104956	ENST00000470026.5_exon_5_0_chr17_43104868_r	0	-
chr17	43106455	43106533	ENST00000470026.5_exon_6_0_chr17_43106456_r	0	-
chr17	43115725	43115779	ENST00000470026.5_exon_7_0_chr17_43115726_r	0	-
chr17	43124016	43124115	ENST00000470026.5_exon_8_0_chr17_43124017_r	0	-
chr17	43125181	43125323	ENST00000470026.5_exon_9_0_chr17_43125182_r	0	-
chr17	43093584	43094860	ENST00000652672.1_exon_0_0_chr17_43093585_r	0	-
chr17	43095845	43095922	ENST00000652672.1_exon_1_0_chr17_43095846_r	0	-
chr17	43097243	43097289	ENST00000652672.1_exon_2_0_chr17_43097244_r	0	-
chr17	43099774	43099880	ENST00000652672.1_exon_3_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000652672.1_exon_4_0_chr17_43104122_r	0	-
chr17	43104867	43104956	ENST00000652672.1_exon_5_0_chr17_43104868_r	0	-
chr17	43106455	43106533	ENST00000652672.1_exon_6_0_chr17_43106456_r	0	-
chr17	43115725	43115779	ENST00000652672.1_exon_7_0_chr17_43115726_r	0	-
chr17	43121557	43121676	ENST00000652672.1_exon_8_0_chr17_43121558_r	0	-
chr17	43124016	43124115	ENST00000652672.1_exon_9_0_chr17_43124017_r	0	-
chr17	43125276	43125483	ENST00000652672.1_exon_10_0_chr17_43125277_r	0	-
chr17	43093585	43094860	ENST00000477152.5_exon_0_0_chr17_43093586_r	0	-
chr17	43095845	43095922	ENST00000477152.5_exon_1_0_chr17_43095846_r	0	-
chr17	43097243	43097289	ENST00000477152.5_exon_2_0_chr17_43097244_r	0	-
chr17	43099774	43099880	ENST00000477152.5_exon_3_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000477152.5_exon_4_0_chr17_43104122_r	0	-
chr17	43104867	43104956	ENST00000477152.5_exon_5_0_chr17_43104868_r	0	-
chr17	43115725	43115779	ENST00000477152.5_exon_6_0_chr17_43115726_r	0	-
chr17	43124016	43124115	ENST00000477152.5_exon_7_0_chr17_43124017_r	0	-
chr17	43125270	43125364	ENST00000477152.5_exon_8_0_chr17_43125271_r	0	-
chr17	43094111	43094860	ENST00000492859.5_exon_0_0_chr17_43094112_r	0	-
chr17	43095845	43095922	ENST00000492859.5_exon_1_0_chr17_43095846_r	0	-
chr17	43097243	43097289	ENST00000492859.5_exon_2_0_chr17_43097244_r	0	-
chr17	43099774	43099880	ENST00000492859.5_exon_3_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000492859.5_exon_4_0_chr17_43104122_r	0	-
chr17	43104867	43104956	ENST00000492859.5_exon_5_0_chr17_43104868_r	0	-
chr17	43106455	43106533	ENST00000492859.5_exon_6_0_chr17_43106456_r	0	-
chr17	43110464	43110580	ENST00000492859.5_exon_7_0_chr17_43110465_r	0	-
chr17	43115725	43115779	ENST00000492859.5_exon_8_0_chr17_43115726_r	0	-
chr17	43124016	43124115	ENST00000492859.5_exon_9_0_chr17_43124017_r	0	-
chr17	43125270	43125300	ENST00000492859.5_exon_10_0_chr17_43125271_r	0	-
chr17	43094111	43094860	ENST00000497488.1_exon_0_0_chr17_43094112_r	0	-
chr17	43125270	43125300	ENST00000497488.1_exon_1_0_chr17_43125271_r	0	-
chr17	43094111	43094860	ENST00000494123.5_exon_0_0_chr17_43094112_r	0	-
chr17	43095845	43095922	ENST00000494123.5_exon_1_0_chr17_43095846_r	0	-
chr17	43097243	43097289	ENST00000494123.5_exon_2_0_chr17_43097244_r	0	-
chr17	43099774	43099880	ENST00000494123.5_exon_3_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000494123.5_exon_4_0_chr17_43104122_r	0	-
chr17	43104867	43104956	ENST00000494123.5_exon_5_0_chr17_43104868_r	0	-
chr17	43106455	43106533	ENST00000494123.5_exon_6_0_chr17_43106456_r	0	-
chr17	43115725	43115779	ENST00000494123.5_exon_7_0_chr17_43115726_r	0	-
chr17	43124016	43124115	ENST00000494123.5_exon_8_0_chr17_43124017_r	0	-
chr17	43125276	43125450	ENST00000494123.5_exon_9_0_chr17_43125277_r	0	-
chr17	43094169	43094860	ENST00000473961.5_exon_0_0_chr17_43094170_r	0	-
chr17	43099774	43099877	ENST00000473961.5_exon_1_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000473961.5_exon_2_0_chr17_43104122_r	0	-
chr17	43104867	43104891	ENST00000473961.5_exon_3_0_chr17_43104868_r	0	-
chr17	43094481	43094860	ENST00000642945.1_exon_0_0_chr17_43094482_r	0	-
chr17	43095845	43095922	ENST00000642945.1_exon_1_0_chr17_43095846_r	0	-
chr17	43097243	43097289	ENST00000642945.1_exon_2_0_chr17_43097244_r	0	-
chr17	43099774	43099880	ENST00000642945.1_exon_3_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000642945.1_exon_4_0_chr17_43104122_r	0	-
chr17	43104867	43104956	ENST00000642945.1_exon_5_0_chr17_43104868_r	0	-
chr17	43106455	43106533	ENST00000642945.1_exon_6_0_chr17_43106456_r	0	-
chr17	43112486	43112606	ENST00000642945.1_exon_7_0_chr17_43112487_r	0	-
chr17	43115725	43115779	ENST00000642945.1_exon_8_0_chr17_43115726_r	0	-
chr17	43124016	43124115	ENST00000642945.1_exon_9_0_chr17_43124017_r	0	-
chr17	43125276	43125343	ENST00000642945.1_exon_10_0_chr17_43125277_r	0	-
chr17	43095845	43095922	ENST00000476777.5_exon_0_0_chr17_43095846_r	0	-
chr17	43097243	43097289	ENST00000476777.5_exon_1_0_chr17_43097244_r	0	-
chr17	43099774	43099877	ENST00000476777.5_exon_2_0_chr17_43099775_r	0	-
chr17	43104121	43104261	ENST00000476777.5_exon_3_0_chr17_43104122_r	0	-
chr17	43104867	43104956	ENST00000476777.5_exon_4_0_chr17_43104868_r	0	-
chr17	43106455	43106533	ENST00000476777.5_exon_5_0_chr17_43106456_r	0	-
chr17	43115725	43115779	ENST00000476777.5_exon_6_0_chr17_43115726_r	0	-
chr17	43124016	43124115	ENST00000476777.5_exon_7_0_chr17_43124017_r	0	-
chr17	43125270	43125353	ENST00000476777.5_exon_8_0_chr17_43125271_r	0	-
"""

try:
    main()
except Exception:
    print()
    traceback.print_exc(file=sys.stdout)
    sys.exit(1)
