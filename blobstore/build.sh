#!/bin/bash
# shellcheck disable=SC1091

script_path=$(readlink -f "${BASH_SOURCE[0]}")
script_dir=$(dirname "$script_path")
root_dir=$(dirname "$script_dir")
source "$root_dir/shell/utilities.sh"
source "$root_dir/build/cgo_env.sh"

build_linux_x86_64() {
  threads="$threads" make -f ../Makefile blobstore
}

# build arm64 with amd64 docker ubuntu:focal,
# apt-get install -y gcc-9-aarch64-linux-gnu gcc-aarch64-linux-gnu  g++-9-aarch64-linux-gnu g++-aarch64-linux-gnu
# Support Ubuntu focal, not support CentOS7
build_linux_arm64_gcc9() {
  echo "build linux arm64 gcc9"
  export PORTABLE=1
  export ARCH=arm64
  # export CC=aarch64-linux-gnu-gcc
  export EXTRA_CFLAGS="-Wno-deprecated-copy -fno-strict-aliasing -Wclass-memaccess -Wno-error=class-memaccess -Wpessimizing-move -Wno-error=pessimizing-move"
  export EXTRA_CXXFLAGS=$EXTRA_CFLAGS

  CGO_ENABLED=1 GOOS=linux GOARCH=arm64 threads="$threads" make -f ../Makefile blobstore
}

# build arm64 with amd64 docker buntu:xenial,
# apt-get install -y gcc-4.9-aarch64-linux-gnu gcc-aarch64-linux-gnu g++-4.9-aarch64-linux-gnu g++-aarch64-linux-gnu
# support CentOS7
#
build_linux_arm64_gcc4() {
  echo "build linux arm64 gcc4.9"
  export PORTABLE=1
  export ARCH=arm64
  # export CC=aarch64-linux-gnu-gcc
  export EXTRA_CFLAGS=" -fno-strict-aliasing  "
  export EXTRA_CXXFLAGS=$EXTRA_CFLAGS

  CGO_ENABLED=1 GOOS=linux GOARCH=arm64 threads="$threads" make -f ../Makefile blobstore
}

cpu_arch=$(get_cpu_arch)
gcc_version=$(get_gcc_version)
threads=$(get_cpu_cores)
threads=$((threads <= 0 ? 4 : (threads > 10 ? 10 : threads)))

function usage_help() {
  echo "bash $0 [-t] [-h]"
  echo "usage: bash $0                  Compile binary with one thread"
  echo "   or: bash $0 -t <count>       Compile binary with specified count thread"
  echo "Arguments:"
  echo "   -h, --help                   Print Help and exit"
  echo "   -t, --thread                 Specified compile threads count, default is 1"
}

if ! GETOPT_ARGS=$(getopt -q -o t:h --long thread:,help -- "$@"); then
  echo_error "Error: Invalid option."
fi
eval set -- "$GETOPT_ARGS"

while [ -n "$1" ]; do
  case "$1" in
  -t | --thread)
    [ -z "$2" ] && echo_error "Error: -t/--thread requires a value."
    threads=$2
    shift 2
    ;;
  -h | --help)
    usage_help
    exit 1
    ;;
  --)
    shift
    break
    ;;
  *)
    echo "unimplemented option"
    exit 1
    ;;
  esac
done

if [ "$cpu_arch" == "x86" ]; then
  build_linux_x86_64
elif [ "$cpu_arch" == "arm" ]; then
  if [ "$gcc_version" -ge 9 ]; then
    build_linux_arm64_gcc9
  else
    build_linux_arm64_gcc4
  fi
else
  echo "unknown cpu architecture"
  exit 1
fi
