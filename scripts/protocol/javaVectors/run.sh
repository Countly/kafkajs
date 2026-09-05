#!/bin/bash -e
#
# Generates golden protocol vectors by running Apache Kafka's own generated codecs.
# The kafka-clients jar is downloaded into a temporary directory and is never committed.
#
# Usage: scripts/protocol/javaVectors/run.sh [output-dir]
# Requires: a JDK on PATH (javac + java), network access to Maven Central on first run.

KAFKA_CLIENTS_VERSION="${KAFKA_CLIENTS_VERSION:-4.3.1}"
JAR_URL="https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"
WORK_DIR="${JAVA_VECTORS_WORK_DIR:-$(mktemp -d -t kafkajs-java-vectors-XXXXXX)}"
# A caller-provided work dir is a cache and is kept; a temp one is removed on exit.
if [ -z "${JAVA_VECTORS_WORK_DIR:-}" ]; then trap 'rm -rf "${WORK_DIR}"' EXIT; fi
OUT_DIR="${1:-$(cd "$(dirname "$0")/../../.." && pwd)}"
SRC_DIR="$(cd "$(dirname "$0")" && pwd)"

JAR="${WORK_DIR}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"
if [ ! -f "${JAR}" ]; then
  echo "downloading ${JAR_URL}"
  curl -fsSL "${JAR_URL}" -o "${JAR}"
fi

javac -cp "${JAR}" -d "${WORK_DIR}" "${SRC_DIR}/GenerateVectors.java"
java -cp "${JAR}:${WORK_DIR}" GenerateVectors "${OUT_DIR}"
echo "work dir: ${WORK_DIR} (the jar is never committed)"
