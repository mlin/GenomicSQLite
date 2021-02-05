version development
# Tests GenomicSQLite loaders, queries, & misc ops on some full-size datasets

workflow genomicsqlite_big_tests {
    input {
        String git_revision = "main"
        File? libgenomicsqlite_so

        File reads = "https://s3.amazonaws.com/1000genomes/1000G_2504_high_coverage/data/ERR3239334/NA12878.final.cram" # !FileCoercion
        File variants = "gs://brain-genomics-public/research/cohort/1KGP/cohort_gatk/CCDG_13607_B01_GRM_WGS_2019-02-19_chr21.recalibrated_variants.vcf.gz" # !FileCoercion
    }

    call build {
        input:
        git_revision = git_revision
    }

    call test_sam {
        input:
        reads = reads,
        libgenomicsqlite_so = select_first([libgenomicsqlite_so, build.libgenomicsqlite_so]),
        genomicsqlite_py = build.genomicsqlite_py,
        sam_into_sqlite = build.sam_into_sqlite
    }

    call test_sam_web {
        input:
        reads_db = test_sam.reads_db,
        libgenomicsqlite_so = select_first([libgenomicsqlite_so, build.libgenomicsqlite_so]),
        genomicsqlite_py = build.genomicsqlite_py
    }

    call test_vcf {
        input:
        variants = variants,
        libgenomicsqlite_so = select_first([libgenomicsqlite_so, build.libgenomicsqlite_so]),
        genomicsqlite_py = build.genomicsqlite_py,
        vcf_into_sqlite = build.vcf_into_sqlite
    }
}

task build {
    input {
        String git_revision
    }

    command <<<
        set -euxo pipefail

        # build libgenomicsqlite.so and loader executables
        git clone --recursive https://github.com/mlin/GenomicSQLite.git
        cd GenomicSQLite
        git checkout ~{git_revision}
        cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS='-D_GLIBCXX_USE_CXX11_ABI=0' -B build .
        cmake --build build -j 8
        cp bindings/python/genomicsqlite/__init__.py genomicsqlite.py

        # test them
        env -C build "LD_LIBRARY_PATH=$(pwd)" ctest -V >&2

        # sufficient runtime deps: sqlite3 samtools tabix libzstd1
        ldd build/loaders/sam_into_sqlite >&2
    >>>

    output {
        File libgenomicsqlite_so = "GenomicSQLite/build/libgenomicsqlite.so"
        File genomicsqlite_py = "GenomicSQLite/genomicsqlite.py"
        File sam_into_sqlite = "GenomicSQLite/build/loaders/sam_into_sqlite"
        File vcf_into_sqlite = "GenomicSQLite/build/loaders/vcf_into_sqlite"
    }

    Array[String] apt_deps = [
        "zip", "pigz", "wget", "build-essential", "git-core", "sqlite3", "cmake", "libsqlite3-dev",
        "libzstd-dev", "python3-pip", "maven", "cargo", "libhts-dev", "samtools", "tabix", "libcurl4-openssl-dev"
    ]
    Array[String] pip_deps = ["pytest", "pytest-xdist"]

    runtime {
        cpu: 8
        inlineDockerfile: [
            "FROM ubuntu:20.04",
            "RUN apt-get -qq update && DEBIAN_FRONTEND=noninteractive apt-get install -qq -y ~{sep(' ', apt_deps)}",
            "RUN pip3 install ~{sep(' ', pip_deps)}"
        ]
    }
}

task test_sam {
    input {
        File reads
        String cram_ref_fa_gz = "ftp://ftp.ncbi.nlm.nih.gov/genomes/all/GCA/000/001/405/GCA_000001405.15_GRCh38/seqs_for_alignment_pipelines.ucsc_ids/GCA_000001405.15_GRCh38_no_alt_analysis_set.fna.gz"

        File genomicsqlite_py
        File libgenomicsqlite_so
        File sam_into_sqlite
    }

    String dbname = basename(reads) + ".gsql"

    command <<<
        set -euxo pipefail
        TMPDIR=${TMPDIR:-/tmp}

        cp ~{genomicsqlite_py} /usr/lib/python3.8/genomicsqlite.py
        cp ~{libgenomicsqlite_so} /usr/local/lib/libgenomicsqlite.so
        ldconfig
        >&2 sha256sum /usr/local/lib/libgenomicsqlite.so

        cp ~{sam_into_sqlite} /usr/local/bin/sam_into_sqlite
        chmod +x /usr/local/bin/sam_into_sqlite
        ldd -r /usr/local/bin/sam_into_sqlite

        reads_file='~{reads}'
        if [[ $reads_file == *.cram ]]; then
            # CRAM given; make BAM to take reference downloads out of the timings
            wget -nv -O - '~{cram_ref_fa_gz}' | pigz -dc > "${TMPDIR}/cram_ref.fa"
            samtools faidx "${TMPDIR}/cram_ref.fa"
            bam_file="${TMPDIR}/$(basename reads_file .cram).bam"
            time samtools view -T "${TMPDIR}/cram_ref.fa" -h -O BAM -@ 8 "$reads_file" > "$bam_file"
            reads_file=$bam_file
        fi
        >&2 ls -l "$reads_file"

        # load database
        time sam_into_sqlite --level -1 --inner-page-KiB 64 --outer-page-KiB 4 "$reads_file" '~{dbname}'
        >&2 ls -l '~{dbname}'

        # compaction
        time /usr/lib/python3.8/genomicsqlite.py '~{dbname}' --compact --level 8 --inner-page-KiB 64 --outer-page-KiB 2 -@ 4
        >&2 ls -l '~{dbname}'*

        # GRI query
        time python3 - <<"EOF"
        import sys
        import genomicsqlite
        dbconn = genomicsqlite.connect('~{dbname}.compact', read_only=True)
        chr = genomicsqlite.get_reference_sequences_by_name(dbconn)
        query = 'SELECT count(*) FROM ' + genomicsqlite.genomic_range_rowids_sql(dbconn, 'reads')
        print(query, file=sys.stderr)
        row = next(dbconn.execute(query, (chr['chr12'].rid,111803912,111804012)))
        print(f'result = {row[0]}', file=sys.stderr)
        print(row[0])
        EOF

        # page compression stats
        time sqlite3 '~{dbname}.compact' "SELECT meta1, count(*), avg(length(data)) FROM nested_vfs_zstd_pages GROUP BY meta1" >&2

        # add a QNAME-sorted seqs table. TODO: write it into a separate attached db
        chmod +x /usr/lib/python3.8/genomicsqlite.py
        time /usr/lib/python3.8/genomicsqlite.py '~{dbname}.compact' "PRAGMA journal_mode=off; PRAGMA synchronous=off; CREATE TABLE reads_seqs_by_qname AS SELECT * from reads_seqs NOT INDEXED ORDER BY qname"
    >>>

    output {
        File reads_db = dbname + ".compact"
        Int reads_db_size = round(size(dbname))
        Int reads_original_size = round(size(reads))
    }

    Array[String] apt_deps = ["sqlite3", "samtools", "tabix", "libzstd1", "pigz", "wget"]

    runtime {
        cpu: 8
        inlineDockerfile: [
            "FROM ubuntu:20.04",
            "RUN apt-get -qq update && DEBIAN_FRONTEND=noninteractive apt-get install -qq -y ~{sep(' ', apt_deps)}"
        ]
    }
}

task test_sam_web {
    input {
        File reads_db
        File genomicsqlite_py
        File libgenomicsqlite_so
    }

    command <<<
        set -euxo pipefail
        TMPDIR=${TMPDIR:-/tmp}

        cp ~{genomicsqlite_py} /usr/local/bin/genomicsqlite
        chmod +x /usr/local/bin/genomicsqlite
        cp ~{libgenomicsqlite_so} /usr/local/lib/libgenomicsqlite.so
        ldconfig

        # make self-signed cert
        openssl req -new -newkey rsa:4096 -days 365 -nodes -x509 \
            -subj "/C=US/ST=Denial/L=Springfield/O=Dis/CN=www.example.com" \
            -keyout /tmp/www.example.com.key  -out /tmp/www.example.com.crt

        # write nginx.config as heredoc
        # references: https://docs.nginx.com/nginx/admin-guide/web-server/serving-static-content/
        #             http://nginx.org/en/docs/http/configuring_https_servers.html
        READS_DB_DIR="$(dirname '~{reads_db}')"
        cat << EOF > nginx.config
        worker_processes     8;
        error_log            stderr warn;
        events {
        }
        http {
            access_log                stderr;
            proxy_max_temp_file_size  0;
            sendfile                  on;
            sendfile_max_chunk        1m;

            server {
                root                 $READS_DB_DIR;
                listen               9999 ssl;
                ssl_certificate      /tmp/www.example.com.crt;
                ssl_certificate_key  /tmp/www.example.com.key;

                location / {
                }
            }
        }
        EOF

        # start nginx as background process
        nginx -Tc "$(pwd)/nginx.config"
        nginx -c "$(pwd)/nginx.config"

        export SQLITE_WEB_INSECURE=1
        export SQLITE_WEB_LOG=4
        URL="$(basename '~{reads_db}')"
        URL="https://localhost:9999/${URL}"

        # GRI query
        >&2 genomicsqlite "$URL" -readonly \
            "SELECT count(1)
             FROM (SELECT _gri_rid AS rid FROM _gri_refseq WHERE gri_refseq_name='chr21') AS query, reads
             WHERE reads._rowid_ IN genomic_range_rowids('reads', query.rid, 20000000, 20100000)"

        # big aggregation
        >&2 genomicsqlite "$URL" -readonly \
            'SELECT gri_refseq_name, count(1) AS read_count
                FROM reads LEFT JOIN _gri_refseq USING(_gri_rid)
                GROUP BY gri_refseq_name
                ORDER BY read_count DESC'

        # stop nginx
        killall nginx || true
    >>>

    Array[String] apt_deps = ["sqlite3", "nginx", "openssl", "python3-minimal", "libcurl4", "psmisc"]

    runtime {
        cpu: 8
        inlineDockerfile: [
            "FROM ubuntu:20.04",
            "RUN apt-get -qq update && DEBIAN_FRONTEND=noninteractive apt-get install -qq -y ~{sep(' ', apt_deps)}"
        ]
    }
}

task test_vcf {
    input {
        File variants

        File genomicsqlite_py
        File libgenomicsqlite_so
        File vcf_into_sqlite
    }

    String dbname = basename(variants) + ".gsql"

    command <<<
        set -euxo pipefail

        cp ~{genomicsqlite_py} /usr/lib/python3.8/genomicsqlite.py
        cp ~{libgenomicsqlite_so} /usr/local/lib/libgenomicsqlite.so
        ldconfig
        >&2 sha256sum /usr/local/lib/libgenomicsqlite.so

        cp ~{vcf_into_sqlite} /usr/local/bin/vcf_into_sqlite
        chmod +x /usr/local/bin/vcf_into_sqlite
        ldd -r /usr/local/bin/vcf_into_sqlite

        # load database
        time vcf_into_sqlite --inner-page-KiB 64 --outer-page-KiB 2 --genotypes-without-rowid "~{variants}" "~{dbname}"

        # GRI query
        time python3 - <<"EOF"
        import sys
        import genomicsqlite
        dbconn = genomicsqlite.connect('~{dbname}', read_only=True)
        chr = genomicsqlite.get_reference_sequences_by_name(dbconn)
        query = 'SELECT count(*) FROM ' + genomicsqlite.genomic_range_rowids_sql(dbconn, 'variants')
        print(query, file=sys.stderr)
        row = next(dbconn.execute(query, (chr['chr21'].rid, 34787801, 35049344))) #(chr['chr12'].rid,111803912,111804012)))
        print(f'result = {row[0]}', file=sys.stderr)
        print(row[0])
        EOF

        # page compression stats
        time sqlite3 "~{dbname}" "SELECT meta1, count(*), avg(length(data)) FROM nested_vfs_zstd_pages GROUP BY meta1" >&2
    >>>

    output {
        File variants_db = dbname
        Int variants_db_size = round(size(dbname))
        Int variants_original_size = round(size(variants))
    }

    Array[String] apt_deps = ["sqlite3", "samtools", "tabix", "libzstd1", "pigz", "wget"]

    runtime {
        cpu: 8
        inlineDockerfile: [
            "FROM ubuntu:20.04",
            "RUN apt-get -qq update && DEBIAN_FRONTEND=noninteractive apt-get install -qq -y ~{sep(' ', apt_deps)}"
        ]
    }
}
