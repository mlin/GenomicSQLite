# Builds libgenomicsqlite.so in old ubuntu (16.04) to maximize portability. Also bakes in Zstandard.
# This recipe works on 14.04 too, but there are C++ symbol differences before & after g++ 5.1:
#   https://gcc.gnu.org/onlinedocs/gcc-5.2.0/libstdc++/manual/manual/using_dual_abi.html
FROM ubuntu:16.04

ARG CMAKE_VERSION=3.17.3
ARG SQLITE_VERSION=3310000
ARG ZSTD_VERSION=1.4.7
ARG CPU_ARCH=haswell

ENV CFLAGS="-march=${CPU_ARCH} -O3"
ENV CXXFLAGS=${CFLAGS}

# apt
RUN apt-get -qq update && DEBIAN_FRONTEND=noninteractive apt-get -qq install -y \
        build-essential wget zip rsync git-core

RUN mkdir -p /work/GenomicSQLite

# SQLite
WORKDIR /work
RUN wget -nv https://www.sqlite.org/2020/sqlite-amalgamation-${SQLITE_VERSION}.zip \
        && unzip -o sqlite-amalgamation-${SQLITE_VERSION}.zip
WORKDIR /work/sqlite-amalgamation-${SQLITE_VERSION}
RUN gcc -shared -o libsqlite3.so.0 -fPIC -shared -Wl,-soname,libsqlite3.so.0 -g ${CFLAGS} sqlite3.c
RUN gcc -o sqlite3 -g ${CFLAGS} sqlite3.c shell.c -lpthread -ldl
RUN cp libsqlite3.so.0 /usr/local/lib && cp *.h /usr/local/include && cp sqlite3 /usr/local/bin
RUN ln -s /usr/local/lib/libsqlite3.so.0 /usr/local/lib/libsqlite3.so

# Zstandard -- hacked with -fPIC for use with ZSTD_WHOLE_ARCHIVE
WORKDIR /work
RUN wget -nv -O - https://github.com/facebook/zstd/archive/v${ZSTD_VERSION}/v${ZSTD_VERSION}.tar.gz | tar zx
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
RUN rm -rf build && cmake -DCMAKE_BUILD_TYPE=Release -DZSTD_WHOLE_ARCHIVE=true -B build . && cmake --build build --target genomicsqlite -j $(nproc)
RUN ldd -v -r build/libgenomicsqlite.so
RUN sqlite3 -cmd '.load build/libgenomicsqlite.so' :memory: 'select genomicsqlite_version()'
