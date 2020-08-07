all:
	docker build -t genomicsqlite_compression_tuning .
	miniwdl run run.wdl \
		trial.docker=genomicsqlite_compression_tuning \
		trial.bam=../data/NA12878.chr21:20000000-22500000.bam \
		trial.sam_into_sqlite=../../build/loaders/sam_into_sqlite \
		trial.libgenomicsqlite_so=../../build/libgenomicsqlite.so \
		--verbose --dir /tmp

.PHONY: all
