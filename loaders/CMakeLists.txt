add_executable(vcf_into_sqlite vcf_into_sqlite.cc common.hpp strlcpy.h)
target_link_libraries(vcf_into_sqlite PRIVATE genomicsqlite hts sqlite3 pthread dl)

add_executable(vcf_lines_into_sqlite vcf_lines_into_sqlite.cc common.hpp strlcpy.h)
target_link_libraries(vcf_lines_into_sqlite PRIVATE genomicsqlite sqlite3 pthread dl)

add_executable(sam_into_sqlite sam_into_sqlite.cc common.hpp strlcpy.h)
target_link_libraries(sam_into_sqlite PRIVATE genomicsqlite SQLiteCpp hts)
