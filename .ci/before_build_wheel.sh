#!/bin/sh
set -xeu

# On 32-bit Linux platforms, we need libatomic1 to use rustup
if command -v yum > /dev/null 2>&1; then
   yum install -y libatomic
   # mdbx-sys (a libmdbx dependency) needs libclang at build time for bindgen.
   yum install -y clang-devel
   # zstd-sys (pulled in by mtxdb with its pkg-config feature) needs libzstd.
   yum install -y libzstd-devel
fi

# The musllinux (Alpine) equivalents of the above.
if command -v apk > /dev/null 2>&1; then
   apk add --no-cache clang19-dev zstd-dev
fi

# Install a Rust toolchain
curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain stable -y --profile minimal
