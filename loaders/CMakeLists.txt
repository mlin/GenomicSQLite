add_executable(vcf_into_sqlite vcf_into_sqlite.cc common.hpp strlcpy.h)
target_link_libraries(vcf_into_sqlite PRIVATE pthread libsqlite3.so.0 genomicsqlite SQLiteCpp hts)

add_executable(vcf_lines_into_sqlite vcf_lines_into_sqlite.cc common.hpp strlcpy.h)
target_link_libraries(vcf_lines_into_sqlite PRIVATE pthread libsqlite3.so.0 genomicsqlite SQLiteCpp)

add_executable(sam_into_sqlite sam_into_sqlite.cc common.hpp strlcpy.h)
target_link_libraries(sam_into_sqlite PRIVATE pthread libsqlite3.so.0 genomicsqlite SQLiteCpp hts)
