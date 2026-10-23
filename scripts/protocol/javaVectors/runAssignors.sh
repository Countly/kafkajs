#!/bin/bash -e
#
# Generates golden vectors by running Apache Kafka's own code: `assignors` (the default) runs the
# partition assignors, `messages` runs the generated message codecs for the consumer-protocol,
# group-visibility, transaction, admin (config, topic, record, group, ACL), topic-partition and
# classic group APIs.
# The kafka-clients jar is downloaded into a temporary directory and is never committed.
#
# Usage: scripts/protocol/javaVectors/runAssignors.sh [assignors|messages|snappy|produce] [output-dir] [--read-back]
# Requires: a JDK on PATH (javac + java), 17 or newer for `messages` (it uses records), and network
# access to Maven Central on first run, when the jars are downloaded.
# Snappy writes fixtures to output-dir; --read-back checks encoder artifacts in that directory.

KAFKA_CLIENTS_VERSION="${KAFKA_CLIENTS_VERSION:-4.3.1}"
SLF4J_VERSION="${SLF4J_VERSION:-1.7.36}"
WORK_DIR="${JAVA_VECTORS_WORK_DIR:-$(mktemp -d -t kafkajs-assignor-vectors-XXXXXX)}"
# A caller-provided work dir is a cache and is kept; a temp one is removed on exit.
if [ -z "${JAVA_VECTORS_WORK_DIR:-}" ]; then trap 'rm -rf "${WORK_DIR}"' EXIT; fi
# A caller-provided work dir need not exist yet; without this the first download fails with
# curl's opaque "(23) Failure writing output to destination".
mkdir -p "${WORK_DIR}"
SRC_DIR="$(cd "$(dirname "$0")" && pwd)"

# Subcommand: `assignors` (the default, and what this script did before) or `messages`.
MODE="assignors"
if [ "${1:-}" = "assignors" ] || [ "${1:-}" = "messages" ] || [ "${1:-}" = "snappy" ] || [ "${1:-}" = "produce" ]; then
  MODE="$1"
  shift
fi
OUT_DIR="${1:-$(cd "$(dirname "$0")/../../.." && pwd)}"

# Snappy fixtures need only the audited Snappy jar, not kafka-clients or slf4j.
if [ "${MODE}" = "snappy" ]; then
  if [ -z "${1:-}" ]; then OUT_DIR="${OUT_DIR}/src/protocol/message/compression/fixtures"; fi
  SNAPPY_VERSION="1.1.10.7"
  SNAPPY_JAR="${WORK_DIR}/snappy-java-${SNAPPY_VERSION}.jar"
  SNAPPY_URL="https://repo.maven.apache.org/maven2/org/xerial/snappy/snappy-java/${SNAPPY_VERSION}/snappy-java-${SNAPPY_VERSION}.jar"
  [ -f "${SNAPPY_JAR}" ] || curl -fsSL --max-time 90 "${SNAPPY_URL}" -o "${SNAPPY_JAR}"
  SNAPPY_SHA1="$(curl -fsSL --max-time 90 "${SNAPPY_URL}.sha1")"
  [ "${SNAPPY_SHA1}" = "3049f95640f4625a945cfab85715f603fa4c8f80" ]
  [ "$(sha1sum "${SNAPPY_JAR}" | cut -d ' ' -f 1)" = "${SNAPPY_SHA1}" ]
  javac -cp "${SNAPPY_JAR}" -d "${WORK_DIR}" "${SRC_DIR}/GenerateSnappyVectors.java"
  if [ "${2:-}" = "--read-back" ]; then
    java -cp "${SNAPPY_JAR}:${WORK_DIR}" GenerateSnappyVectors --read-back "${OUT_DIR}"
  else
    java -cp "${SNAPPY_JAR}:${WORK_DIR}" GenerateSnappyVectors "${OUT_DIR}"
  fi
  exit 0
fi

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
if [ "${MODE}" = "messages" ] || [ "${MODE}" = "produce" ]; then
  javac -cp "${CP}" -d "${WORK_DIR}" "${SRC_DIR}/GenerateMessageVectors.java"
  if [ "${MODE}" = "produce" ]; then
    for JAR in "${CLIENTS_JAR}" "${SLF4J_JAR}"; do
      if [ "${JAR}" = "${CLIENTS_JAR}" ]; then
        JAR_URL="https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"
      else
        JAR_URL="https://repo.maven.apache.org/maven2/org/slf4j/slf4j-api/${SLF4J_VERSION}/slf4j-api-${SLF4J_VERSION}.jar"
      fi
      JAR_SHA1="$(curl -fsSL --max-time 90 "${JAR_URL}.sha1")"
      [ "$(sha1sum "${JAR}" | cut -d ' ' -f 1)" = "${JAR_SHA1}" ]
    done
    java -cp "${CP}:${WORK_DIR}" GenerateMessageVectors produce "${OUT_DIR}"
  else
    java -cp "${CP}:${WORK_DIR}" GenerateMessageVectors "${OUT_DIR}"
  fi
else
  javac -cp "${CP}" -d "${WORK_DIR}" "${SRC_DIR}/GenerateAssignorVectors.java"
  java -cp "${CP}:${WORK_DIR}" GenerateAssignorVectors "${OUT_DIR}"
fi
echo "work dir: ${WORK_DIR} (the jars are never committed)"
