#!/usr/bin/env bash

set -euo pipefail

clean() {
  cargo clean
}

format() {
  cargo fmt
}

lint() {
  cargo clippy --all-features
}

compile() {
  cargo build --all-features
}

test() {
  echo "### features: default"
  cargo test -- --show-output

  echo "### features: tokio"
  cargo test --features tokio -- --show-output

  echo "### features: full"
  cargo test --all-features -- --show-output
}

unit() {
  cargo test --all-features -- --show-output $1
}

cover() {
  cargo llvm-cov clean
  cargo llvm-cov --all-features --html --quiet
  cargo llvm-cov report
}

bench() {
  cargo +nightly bench --quiet -- $1
}

doc() {
  cargo +nightly rustdoc --all-features -- --cfg docsrs
}

msrv() {
  cargo msrv find --all-features --ignore-lockfile --no-check-feedback
}

if [[ "$#" == "0" ]]; then
  #clean
  format
  compile
  test
  lint
  doc
  cover

elif [[ "$1" == "unit" ]]; then
  unit $2

else
  for a in "$@"; do
    case "$a" in
    clean)
      clean
      ;;
    format)
      format
      ;;
    compile)
      compile
      ;;
    test)
      test
      ;;
    lint)
      lint
      ;;
    doc)
      doc
      ;;
    cover)
      cover
      ;;
    bench)
      bench
      ;;
    msrv)
      msrv
      ;;
    *)
      echo "Bad task: $a"
      exit 1
      ;;
    esac
  done
fi
