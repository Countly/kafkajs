#!/bin/bash -e
#
# Generates golden vectors by running Apache Kafka's own code: `assignors` (the default) runs the
# partition assignors, `messages` runs the generated message codecs for the consumer-protocol
# and group-visibility APIs.
# The kafka-clients jar is downloaded into a temporary directory and is never committed.
#
# Usage: scripts/protocol/javaVectors/runAssignors.sh [assignors|messages] [output-dir]
# Requires: a JDK on PATH (javac + java), network access to Maven Central on first run.

KAFKA_CLIENTS_VERSION="${KAFKA_CLIENTS_VERSION:-4.3.1}"
SLF4J_VERSION="${SLF4J_VERSION:-1.7.36}"
WORK_DIR="${JAVA_VECTORS_WORK_DIR:-$(mktemp -d -t kafkajs-assignor-vectors-XXXXXX)}"
# A caller-provided work dir is a cache and is kept; a temp one is removed on exit.
if [ -z "${JAVA_VECTORS_WORK_DIR:-}" ]; then trap 'rm -rf "${WORK_DIR}"' EXIT; fi
SRC_DIR="$(cd "$(dirname "$0")" && pwd)"

# Subcommand: `assignors` (the default, and what this script did before) or `messages`.
MODE="assignors"
if [ "${1:-}" = "assignors" ] || [ "${1:-}" = "messages" ]; then
  MODE="$1"
  shift
fi
OUT_DIR="${1:-$(cd "$(dirname "$0")/../../.." && pwd)}"

CLIENTS_JAR="${WORK_DIR}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"
SLF4J_JAR="${WORK_DIR}/slf4j-api-${SLF4J_VERSION}.jar"

[ -f "${CLIENTS_JAR}" ] || curl -fsSL \
  "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
  -o "${CLIENTS_JAR}"
# slf4j-api is NOT optional here, unlike run.sh: RangeAssignor, StickyAssignor and
# CooperativeStickyAssignor all extend AbstractPartitionAssignor, which holds a static
# LoggerFactory.getLogger(...), so loading any of them without it throws NoClassDefFoundError.
# The generated *Data codecs run.sh uses never log, which is why that script gets away with one jar.
[ -f "${SLF4J_JAR}" ] || curl -fsSL \
  "https://repo1.maven.org/maven2/org/slf4j/slf4j-api/${SLF4J_VERSION}/slf4j-api-${SLF4J_VERSION}.jar" \
  -o "${SLF4J_JAR}"

CP="${CLIENTS_JAR}:${SLF4J_JAR}"
if [ "${MODE}" = "messages" ]; then
  javac -cp "${CP}" -d "${WORK_DIR}" "${SRC_DIR}/GenerateMessageVectors.java"
  java -cp "${CP}:${WORK_DIR}" GenerateMessageVectors "${OUT_DIR}"
else
  javac -cp "${CP}" -d "${WORK_DIR}" "${SRC_DIR}/GenerateAssignorVectors.java"
  java -cp "${CP}:${WORK_DIR}" GenerateAssignorVectors "${OUT_DIR}"
fi
echo "work dir: ${WORK_DIR} (the jars are never committed)"
