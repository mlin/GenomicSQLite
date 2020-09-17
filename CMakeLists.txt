cmake_minimum_required(VERSION 3.11...3.13)
if(${CMAKE_VERSION} VERSION_LESS 3.12)
    cmake_policy(VERSION ${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION})
endif()

include(FetchContent)

if(${CMAKE_VERSION} VERSION_LESS 3.14)
    macro(FetchContent_MakeAvailable NAME)
        FetchContent_GetProperties(${NAME})
        if(NOT ${NAME}_POPULATED)
            FetchContent_Populate(${NAME})
            add_subdirectory(${${NAME}_SOURCE_DIR} ${${NAME}_BINARY_DIR})
        endif()
    endmacro()
endif()

FetchContent_Declare(
    sqlite_zstd_vfs
    GIT_REPOSITORY  https://github.com/mlin/sqlite_zstd_vfs.git
    GIT_TAG         dadf93e
)
FetchContent_MakeAvailable(sqlite_zstd_vfs)
include_directories(${sqlite_zstd_vfs_SOURCE_DIR}/src)

project(genomicsqlite VERSION 1.0
        DESCRIPTION "Genomics Extension for SQLite"
        LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

execute_process(COMMAND git describe --tags --always --dirty
                OUTPUT_VARIABLE GIT_REVISION OUTPUT_STRIP_TRAILING_WHITESPACE)
                set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DGIT_REVISION=\"\\\"${GIT_REVISION}\\\"\"")

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
add_library(genomicsqlite SHARED src/genomicsqlite.cc include/genomicsqlite.h src/json1.c src/hardcoded_refseq.hpp)
if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    find_library(LIBZSTD_A NAMES libzstd.a REQUIRED)
    target_link_libraries(genomicsqlite PRIVATE SQLiteCpp ${LIBZSTD_A})
elseif(ZSTD_WHOLE_ARCHIVE)
    # Linux: statically linking zstd
    target_link_libraries(genomicsqlite PRIVATE libsqlite3.so.0 SQLiteCpp -Wl,--whole-archive libzstd.a -Wl,--no-whole-archive)
else()
    # Linking "libsqlite3.so.0" explicitly, to not require "libsqlite3.so" symlink which may not be
    # present on CentOS.
    target_link_libraries(genomicsqlite PRIVATE libsqlite3.so.0 SQLiteCpp zstd)
endif()

if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)
    add_subdirectory(loaders)
    add_subdirectory(test)
    include(CTest)
    enable_testing()
    set(GENOMICSQLITE_TEST_ENV env PYTHONPATH=${CMAKE_CURRENT_SOURCE_DIR}/bindings/python:${PYTHONPATH} LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}:${LD_LIBRARY_PATH} GENOMICSQLITE_SYSTEM_LIBRARY=1)
    add_test(NAME smoke_test COMMAND ${GENOMICSQLITE_TEST_ENV} python3 ${CMAKE_CURRENT_SOURCE_DIR}/test/genomicsqlite_smoke_test.py)
    add_test(NAME capi_smoke_test COMMAND ${GENOMICSQLITE_TEST_ENV} ${CMAKE_BINARY_DIR}/test/capi_smoke_test)
    add_test(NAME JdbcTests COMMAND ${GENOMICSQLITE_TEST_ENV} mvn test -f ${CMAKE_CURRENT_SOURCE_DIR}/bindings/jdbc/genomicsqlite-jdbc/pom.xml)
    add_test(NAME pytest COMMAND ${GENOMICSQLITE_TEST_ENV} python3 -m pytest -xv -n 4 --tb=short ${CMAKE_CURRENT_SOURCE_DIR}/test)
endif()
