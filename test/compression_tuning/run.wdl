version 1.0

struct Result {
    Int inner_page_KiB
    Int outer_page_KiB
    Int db_size
    Int efficiency
}

workflow genomicsqlite_compression_tuning {
    input {}
    Array[Int] page_sizes = [4, 8, 16, 32, 64]
    scatter (inner_page_KiB in page_sizes) {
        scatter(outer_page_KiB in page_sizes) {
            call trial {
                input:
                inner_page_KiB = inner_page_KiB,
                outer_page_KiB = outer_page_KiB
            }
            Result res = object {
                inner_page_KiB: inner_page_KiB,
                outer_page_KiB: outer_page_KiB,
                db_size: trial.db_size,
                efficiency: trial.efficiency
            }
        }
    }

    output {
        Array[Result] results = flatten(res)
    }
}

task trial {
    input {
        File bam
        File sam_into_sqlite
        File libgenomicsqlite_so
        Int inner_page_KiB
        Int outer_page_KiB
        String docker
    }

    command <<<
        set -euxo pipefail
        cp ~{libgenomicsqlite_so} libgenomicsqlite.so
        cp ~{sam_into_sqlite} sam_into_sqlite
        chmod +x sam_into_sqlite

        LD_LIBRARY_PATH=$(pwd) ./sam_into_sqlite --inner-page-KiB ~{inner_page_KiB} --outer-page-KiB ~{outer_page_KiB} ~{bam} reads.db
        zstd -6 -q reads.db -o reads.db.zst
    >>>

    runtime {
        docker: docker
        cpu: 4
    }

    output {
        Int db_size = round(size("reads.db"))
        Int efficiency = round(100.0*size("reads.db.zst")/size("reads.db"))
    }
}
