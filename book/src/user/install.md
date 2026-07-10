# Building and Installing Zakura

The easiest way to install and run Zakura is to follow the [Getting
Started](../README.md#getting-started) section.

## Building Zakura

If you want to build Zakura, install the build dependencies as described in the
[Manual Install](../README.md#manual-install) section, and
get the source code from GitHub:

```bash
git clone https://github.com/zakura-core/zakura.git
cd zakura
```

You can then build and run `zakurad` by:

```bash
cargo build --release --bin zakurad
target/release/zakurad start
```

If you rebuild Zakura often, you can speed the build process up by dynamically
linking RocksDB, which is a C++ dependency, instead of rebuilding it and linking
it statically. If you want to utilize dynamic linking, first install RocksDB
version >= 8.9.1 as a system library. On Arch Linux, you can do that by:

```bash
pacman -S rocksdb
```

On Ubuntu version >= 24.04, that would be

```bash
apt install -y librocksdb-dev
```

Once you have the library installed, set

```bash
export ROCKSDB_LIB_DIR="/usr/lib/"
```

and enjoy faster builds. Dynamic linking will also decrease the size of the
resulting `zakurad` binary in release mode by ~ 6 MB.

### Building on ARM

If you're using an ARM machine, install the [Rust compiler for
ARM](https://rust-lang.github.io/rustup/installation/other.html). If you build
Zakura using the `x86_64` tools, it might run really slowly.

### Build Troubleshooting

If you are having trouble with:

- **clang:** Install both `libclang` and `clang` - they are usually different
  packages.
- **libclang:** Check out the [clang-sys
  documentation](https://github.com/KyleMayes/clang-sys#dependencies).
- **g++ or MSVC++:** Try using `clang` or `Xcode` instead.
- **rustc:** Use the latest stable `rustc` and `cargo` versions.
- **dependencies**: Use `cargo install` without `--locked` to build with the
  latest versions of each dependency.

#### Optional Tor feature

The `zakura-network/tor` feature has an optional dependency named `libsqlite3`.
If you don't have it installed, you might see errors like `note: /usr/bin/ld:
cannot find -lsqlite3`. Follow [the arti
instructions](https://gitlab.torproject.org/tpo/core/arti/-/blob/main/CONTRIBUTING.md#setting-up-your-development-environment)
to install `libsqlite3`, or use one of these commands instead:

```sh
cargo build
cargo build -p zakura --all-features
```
