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
    GIT_TAG         7b251ca
)
FetchContent_MakeAvailable(sqlite_zstd_vfs)
FetchContent_MakeAvailable(sqlitecpp)
FetchContent_MakeAvailable(sqlite_web_vfs)
FetchContent_MakeAvailable(concurrentqueue)
include_directories(${concurrentqueue_SOURCE_DIR})
include_directories(${sqlite_web_vfs_SOURCE_DIR}/src)
include_directories(${sqlitecpp_SOURCE_DIR}/include)
include_directories(${sqlite_zstd_vfs_SOURCE_DIR}/src)

project(genomicsqlite VERSION 1.0
        DESCRIPTION "Genomics Extension for SQLite"
        LANGUAGES C CXX)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    # Don't dlopen() libcurl until first attempt to use web_vfs -- avoids loading its large tree of
    # shared library dependencies in most cases. Enabled only for Linux because it seemed to cause
    # libcurl routines to intermittently segfault in the macOS Python tests, which we haven't had
    # time to track down yet.
    add_definitions(-DHTTP_LAZYCURL)
    set(LINK_LIBCURL "")
else()
    set(LINK_LIBCURL curl)
endif()

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

execute_process(COMMAND git describe --tags --always --dirty
                OUTPUT_VARIABLE GIT_REVISION OUTPUT_STRIP_TRAILING_WHITESPACE)
                set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DGIT_REVISION=\"\\\"${GIT_REVISION}\\\"\"")

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
add_library(genomicsqlite SHARED src/genomicsqlite.cc include/genomicsqlite.h src/json1.c src/uint.c src/hardcoded_refseq.hpp)
if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    find_library(LIBZSTD_A NAMES libzstd.a REQUIRED)
    target_link_libraries(genomicsqlite PRIVATE SQLiteCpp ${LINK_LIBCURL} ${LIBZSTD_A})
elseif(ZSTD_WHOLE_ARCHIVE)
    # Linux: statically linking zstd
    target_link_libraries(genomicsqlite PRIVATE SQLiteCpp ${LINK_LIBCURL} -Wl,--whole-archive libzstd.a -Wl,--no-whole-archive)
else()
    target_link_libraries(genomicsqlite PRIVATE SQLiteCpp zstd ${LINK_LIBCURL})
endif()

if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)
    add_subdirectory(loaders)
    add_subdirectory(test)
    include(CTest)
    enable_testing()
    set(GENOMICSQLITE_TEST_ENV env PYTHONPATH=${CMAKE_CURRENT_SOURCE_DIR}/bindings/python:${PYTHONPATH} LIBGENOMICSQLITE=${CMAKE_BINARY_DIR}/libgenomicsqlite SQLITE_WEB_LOG=99)
    add_test(NAME smoke_test COMMAND ${GENOMICSQLITE_TEST_ENV} python3 ${CMAKE_CURRENT_SOURCE_DIR}/test/genomicsqlite_smoke_test.py)
    add_test(NAME capi_smoke_test COMMAND ${GENOMICSQLITE_TEST_ENV} ${CMAKE_BINARY_DIR}/test/capi_smoke_test)
    add_test(NAME JdbcTests COMMAND ${GENOMICSQLITE_TEST_ENV} mvn test -B -f ${CMAKE_CURRENT_SOURCE_DIR}/bindings/jdbc/genomicsqlite-jdbc/pom.xml)
    add_test(NAME cargo_test COMMAND ${GENOMICSQLITE_TEST_ENV} ${CMAKE_CURRENT_SOURCE_DIR}/bindings/rust/cargo test --no-default-features)
    add_test(NAME pytest COMMAND ${GENOMICSQLITE_TEST_ENV} python3 -m pytest -xv -n 4 --tb=short ${CMAKE_CURRENT_SOURCE_DIR}/test)
endif()
