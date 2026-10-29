#!/usr/bin/env bash
set -euo pipefail
work_dir="${1:?supply a disposable class output directory}"
source_dir="$(cd "$(dirname "$0")" && pwd)"
mkdir -p "$work_dir"
timeout 600 javac --release 17 -d "$work_dir" "$source_dir/AssignmentGate.java" "$source_dir/GateProbe.java"
timeout 600 java -cp "$work_dir" org.apache.kafka.coordinator.group.GateProbe
