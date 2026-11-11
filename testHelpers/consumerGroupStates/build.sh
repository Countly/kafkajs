#!/usr/bin/env bash
set -euo pipefail
version="${1:?supply a pinned Kafka version}"
work_dir="${2:?supply a disposable build directory}"
case "$version" in 4.4.0|4.3.1|4.2.2|4.1.2) ;; *) echo 'unsupported fixture version' >&2; exit 2 ;; esac
source_dir="$(cd "$(dirname "$0")" && pwd)"
mkdir -p "$work_dir"
work_dir="$(cd "$work_dir" && pwd)"
timeout 600 curl -fsSL "https://archive.apache.org/dist/kafka/$version/kafka-$version-src.tgz" -o "$work_dir/source.tgz"
mkdir -p "$work_dir/source"
timeout 600 tar -xzf "$work_dir/source.tgz" --strip-components=1 -C "$work_dir/source"
timeout 600 python3 "$source_dir/prepare.py" "$version" "$work_dir/source"
(
  cd "$work_dir/source"
  timeout 600 ./gradlew --no-daemon :group-coordinator:jar -x test
)
artifact="$work_dir/source/group-coordinator/build/libs/kafka-group-coordinator-$version.jar"
test -f "$artifact"
mkdir -p "$source_dir/build/$version"
cp "$artifact" "$source_dir/build/$version/kafka-group-coordinator-$version.jar"
sha256sum "$source_dir/build/$version/kafka-group-coordinator-$version.jar"
