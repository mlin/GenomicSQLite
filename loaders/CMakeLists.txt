add_executable(vcf_into_sqlite vcf_into_sqlite.cc common.hpp strlcpy.h)
target_link_libraries(vcf_into_sqlite PRIVATE genomicsqlite SQLiteCpp hts)
