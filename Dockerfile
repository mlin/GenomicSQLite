# Builds libgenomicsqlite.so in AlmaLinux 8 (glibc 2.28, the manylinux_2_28 baseline) to
# maximize compatibility. Bakes in Zstandard, and also builds a compatible libsqlite3.so.0
# for users unable/unwilling to upgrade their host's.
FROM almalinux:8 as builder

ARG CMAKE_VERSION=3.17.3
ARG SQLITE_VERSION=3540100
ARG ZSTD_VERSION=1.5.5
ARG CPU_ARCH=ivybridge
ENV CFLAGS="-march=${CPU_ARCH} -O3"
ENV CXXFLAGS=${CFLAGS}
# https://www.sqlite.org/compile.html
ENV SQLITE_CFLAGS="\
        -DSQLITE_ENABLE_LOAD_EXTENSION \
        -DSQLITE_USE_URI \
        -DSQLITE_LIKE_DOESNT_MATCH_BLOBS \
        -DSQLITE_DEFAULT_MEMSTATUS=0 \
        -DSQLITE_MAX_EXPR_DEPTH=0 \
        -DSQLITE_ENABLE_NULL_TRIM \
        -DSQLITE_USE_ALLOCA \
        -DSQLITE_HAVE_ISNAN \
        -DSQLITE_ENABLE_UPDATE_DELETE_LIMIT \
        -DSQLITE_ENABLE_COLUMN_METADATA \
        -DSQLITE_ENABLE_DBSTAT_VTAB \
        -DSQLITE_ENABLE_FTS5 \
        -DSQLITE_ENABLE_RTREE \
        -DSQLITE_ENABLE_PREUPDATE_HOOK \
        -DSQLITE_ENABLE_SESSION \
"

RUN dnf install -y -q wget unzip rsync git libcurl-devel gcc gcc-c++ make

RUN mkdir -p /work/GenomicSQLite

# SQLite
WORKDIR /work
RUN wget -nv http://www.sqlite.org/2026/sqlite-amalgamation-${SQLITE_VERSION}.zip \
        && unzip -o sqlite-amalgamation-${SQLITE_VERSION}.zip
WORKDIR /work/sqlite-amalgamation-${SQLITE_VERSION}
RUN bash -c "gcc -shared -o libsqlite3.so.0 -fPIC -shared -Wl,-soname,libsqlite3.so.0 -g ${CFLAGS} ${SQLITE_CFLAGS} sqlite3.c & pid=$? \
             gcc -o sqlite3 -g ${CFLAGS} ${SQLITE_CFLAGS} sqlite3.c shell.c -lpthread -ldl -lm && wait $pid"
RUN cp libsqlite3.so.0 /usr/local/lib && cp *.h /usr/local/include && cp sqlite3 /usr/local/bin
RUN ln -s /usr/local/lib/libsqlite3.so.0 /usr/local/lib/libsqlite3.so

# Zstandard -- hacked with -fPIC for use with ZSTD_WHOLE_ARCHIVE
WORKDIR /work
RUN wget -nv -O - https://github.com/facebook/zstd/releases/download/v${ZSTD_VERSION}/zstd-${ZSTD_VERSION}.tar.gz | tar zx
WORKDIR /work/zstd-${ZSTD_VERSION}
RUN CFLAGS="${CFLAGS} -fPIC" make install -j $(nproc)

# CMake
WORKDIR /work
RUN wget -nv https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-Linux-x86_64.sh
RUN yes | sh cmake-${CMAKE_VERSION}-Linux-x86_64.sh
RUN rsync -a cmake-${CMAKE_VERSION}-Linux-x86_64/ /usr/local/

RUN ldconfig

# libgenomicsqlite.so
ADD . /work/GenomicSQLite
WORKDIR /work/GenomicSQLite
RUN rm -rf build
# _GLIBCXX_USE_CXX11_ABI=0: the header's C++ API passes std::string across the
# library boundary, so the .so's C++ ABI is part of its public contract. Keep the
# pre-C++11 libstdc++ ABI -- the artifact's historical ABI (the old CentOS
# devtoolset toolchain defaulted to it) and the ABI the CI workflow compiles the
# loader/test executables with. Stock AlmaLinux GCC defaults to ABI=1, which
# silently changes the exported symbols and breaks those executables.
RUN cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="${CXXFLAGS} -D_GLIBCXX_USE_CXX11_ABI=0" -DZSTD_WHOLE_ARCHIVE=true -B build . && cmake --build build --target genomicsqlite -j $(nproc)
RUN sqlite3 -cmd '.load build/libgenomicsqlite.so' :memory: 'select sqlite_version(); select genomicsqlite_version()'




###################################################################################################
# Run-up in fresh almalinux:8, to confirm no dependencies crept into the .so's
# NOTE: this stage is a test stage -- nothing in the final stage depends on it,
# so CI builds it explicitly with `docker build --target alma-smoke-test`
# (BuildKit would otherwise skip it silently).

FROM almalinux:8 AS alma-smoke-test

COPY --from=builder /usr/local/bin/sqlite3 /usr/local/bin/
COPY --from=builder /usr/local/include/sqlite3.h /usr/local/include/

# NOTE: /usr/local/lib is NOT a default ld path in AlmaLinux -- https://unix.stackexchange.com/q/356624
RUN mkdir -p /work/lib
WORKDIR /work
COPY --from=builder /usr/local/lib/libsqlite3.so.0 ./lib/
COPY --from=builder /work/GenomicSQLite/build/libgenomicsqlite.so ./
# the following approximates how bindings will usually load the extension at runtime:
#     - libsqlite3.so.0 in the ld search path (if not already resident in the process)
#     - given absolute path to libgenomicsqlite3.so
RUN LD_LIBRARY_PATH=$(pwd)/lib sqlite3 -cmd ".load $(pwd)/libgenomicsqlite.so" :memory: 'select sqlite_version(); select genomicsqlite_version()'
        # if troubleshooting that: set LD_DEBUG=libs for extremely detailed dlopen() logs

# now try capi_smoke_test, with some hoops to ensure it's not dependent on implicit RPATHs
RUN yum install -y -q gcc && gcc -v
RUN mv libgenomicsqlite.so lib/ && ln -s libsqlite3.so.0 lib/libsqlite3.so
ADD ./test/capi_smoke_test.c ./include/genomicsqlite.h ./
RUN gcc -o genomicsqlite_capi_smoke_test ${CFLAGS} -L$(pwd)/lib -Wl,-rpath,\$ORIGIN -Wl,-z,origin \
        capi_smoke_test.c -lgenomicsqlite -lsqlite3 \
        && readelf -d genomicsqlite_capi_smoke_test
RUN mv lib/{libgenomicsqlite.so,libsqlite3.so.0} . && rm -rf lib
RUN ./genomicsqlite_capi_smoke_test
        # ^ we didn't need LD_LIBRARY_PATH even though the .so's aren't at their linktime locations

# test rust bindings too (also a test stage: CI builds --target rust-smoke-test)
FROM almalinux:8 AS rust-smoke-test

RUN yum install -y -q gcc git
ADD https://sh.rustup.rs /usr/local/bin/rustup-init.sh
RUN chmod +x /usr/local/bin/rustup-init.sh && rustup-init.sh -y
ENV PATH=${PATH}:/root/.cargo/bin

RUN mkdir -p /work/lib
WORKDIR /work
COPY --from=builder /usr/local/lib/libsqlite3.so.0 ./lib/
RUN ln -s libsqlite3.so.0 lib/libsqlite3.so
ADD ./.git ./.git
ADD ./bindings/rust ./rust
COPY --from=builder /work/GenomicSQLite/build/libgenomicsqlite.so ./rust/libgenomicsqlite-linux-x86_64.so
# git in the cargo wrapper must accept the context's .git even if the build
# host checked it out under a different uid than root
RUN git config --global --add safe.directory /work
RUN SQLITE_WEB_LOG=99 LD_LIBRARY_PATH=$(pwd)/lib LIBRARY_PATH=$(pwd)/lib rust/cargo test --release

###################################################################################################
# Run-up in ubuntu 20.04 (final stage: the default docker build target, and the
# image the CI workflow copies the portable libraries out of)

FROM ubuntu:20.04 AS ubuntu-smoke-test

COPY --from=builder /usr/local/bin/sqlite3 /usr/local/bin/
COPY --from=builder /usr/local/include/sqlite3.h /usr/local/include/
COPY --from=builder /usr/local/lib/libsqlite3.so.0 /work/GenomicSQLite/build/libgenomicsqlite.so /usr/local/lib/
RUN ln -s libsqlite3.so.0 /usr/local/lib/libsqlite3.so

RUN sqlite3 -cmd '.load /usr/local/lib/libgenomicsqlite.so' :memory: 'select sqlite_version(); select genomicsqlite_version()'

RUN mkdir /work
WORKDIR /work

RUN apt-get -qq update && apt-get install -qq -y build-essential && gcc -v
ADD ./test/capi_smoke_test.c ./include/genomicsqlite.h /work/
RUN gcc -o genomicsqlite_capi_smoke_test ${CFLAGS} capi_smoke_test.c -lgenomicsqlite -lsqlite3
RUN ./genomicsqlite_capi_smoke_test

# display dependencies & digests in docker build log
RUN ldd -v -r /usr/local/lib/libgenomicsqlite.so
RUN objdump -t /usr/local/lib/libgenomicsqlite.so | grep -o 'GLIBC_.*' | sort -Vr | head -n1
RUN objdump -t /usr/local/lib/libgenomicsqlite.so | grep -o 'GLIBCXX_.*' | sort -Vr | head -n1
RUN objdump -t /usr/local/lib/libsqlite3.so.0 | grep -o 'GLIBC_.*' | sort -Vr | head -n1
RUN sha256sum /usr/local/lib/libgenomicsqlite.so /usr/local/lib/libsqlite3.so.0
