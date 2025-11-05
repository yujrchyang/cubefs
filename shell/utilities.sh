#!/bin/bash

function get_cpu_arch() {
  arch=$(uname -m)
  case $arch in
  x86_64 | i386 | i686)
    echo "x86"
    ;;
  armv* | aarch64)
    echo "arm"
    ;;
  *)
    echo "unknown"
    ;;
  esac
}

function get_cpu_cores() {
  cores=$(grep -c processor < /proc/cpuinfo)
  echo "$cores"
}

function get_gcc_version() {
  gcc_version=$(gcc -dumpversion)
  echo "$gcc_version"
}

function get_os_name() {
  os_name=$(uname -s | tr '[:upper:]' '[:lower:]')
  echo "$os_name"
}

function echo_error() {
  local msg="$1"
  echo "$msg" >&2
  exit 1
}
