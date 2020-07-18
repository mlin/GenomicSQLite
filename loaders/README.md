# GenomicSQLite example loaders

These programs exemplify loading common genomics formats into SQLite databases, with the Genomics Extension providing compression and genomic range indexing. They're used in the extension's test suite, and may perhaps become useful for new applications. Improvement/addition pull requests welcome!

* `vcf_into_sqlite:`: loads VCF/gVCF/pVCF into a highly detailed schema representing all fields in SQL columns.
* `vcf_lines_into_sqlite`: more simply loads VCF with each text line stored alongside bare-essential genomic range columns for indexing.
* `sam_into_sqlite`: loads SAM/BAM/CRAM with a main table for the alignment details and cross-referenced tables for QNAME, SEQ, QUAL, & tags.

Compared to `vcf_into_sqlite`, `vcf_lines_into_sqlite` is much simpler and faster, and produces a smaller database; albeit one less useful for detailed SQL querying. They illustrate opposite ends of a tradeoff in schema design, while the SAM loader occupies a middle ground.
