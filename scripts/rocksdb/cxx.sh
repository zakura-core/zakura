#!/bin/sh
# C++ compiler wrapper for the bundled RocksDB build in `librocksdb-sys`.
#
# RocksDB sources listed in hot-sources.txt keep the release `-O3`; every other
# RocksDB source is compiled with `-Os`. The profiled hot paths stay fast, and the
# rarely run code (option parsing, start-up tables, unused table formats) takes
# about half the space. Any other compile passes through unchanged.
#
# `.cargo/config.toml` selects this wrapper through `CXX_<target>`. Set
# ZAKURA_REAL_CXX to use a compiler other than `c++`.
set -eu

real_cxx=${ZAKURA_REAL_CXX:-c++}
hot_list="$(dirname "$0")/hot-sources.txt"

# The RocksDB-relative path of the source being compiled, if any.
source=""
for arg do
    case $arg in
        /*) path=$arg ;;
        *) path="$PWD/$arg" ;;
    esac
    case $path in
        */librocksdb-sys-*/rocksdb/*.cc) source=${path#*/librocksdb-sys-*/rocksdb/} ;;
    esac
done

if [ -n "$source" ] && ! grep -qxF "$source" "$hot_list"; then
    for arg do
        shift
        [ "$arg" = "-O3" ] && arg="-Os"
        set -- "$@" "$arg"
    done
fi

exec "$real_cxx" "$@"
