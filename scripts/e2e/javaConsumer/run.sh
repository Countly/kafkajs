#!/bin/bash -e
#
# Runs one Java group member against the local test cluster, for the mixed Java/JS interop
# scenario. The jars are downloaded into a temporary directory and are never committed.
#
# Usage: scripts/e2e/javaConsumer/run.sh [--group-protocol classic|consumer] [--report-group-type] \
#          <bootstrapServers> <groupId> <topic> <assignor> <runForMs>
#        scripts/e2e/javaConsumer/run.sh --probe
#        scripts/e2e/javaConsumer/run.sh --describe-group <bootstrapServers> <groupId>
# Requires: a JDK on PATH (javac + java), network access to Maven Central on first run.

KAFKA_CLIENTS_VERSION="${KAFKA_CLIENTS_VERSION:-4.3.1}"
SLF4J_VERSION="${SLF4J_VERSION:-1.7.36}"
WORK_DIR="${JAVA_E2E_WORK_DIR:-$(mktemp -d -t kafkajs-java-consumer-XXXXXX)}"
# A caller-provided work dir is a cache and is kept; a temp one is removed on exit.
if [ -z "${JAVA_E2E_WORK_DIR:-}" ]; then trap 'rm -rf "${WORK_DIR}"' EXIT; fi
# A caller-provided work dir need not exist yet; without this the first download fails with
# curl's opaque "(56) Failure writing output to destination" rather than a usable message.
mkdir -p "${WORK_DIR}"
SRC_DIR="$(cd "$(dirname "$0")" && pwd)"

CLIENTS_JAR="${WORK_DIR}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"
SLF4J_JAR="${WORK_DIR}/slf4j-api-${SLF4J_VERSION}.jar"

[ -f "${CLIENTS_JAR}" ] || curl -fsSL \
  "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
  -o "${CLIENTS_JAR}"
# slf4j-api is not optional: KafkaConsumer and the built-in assignors hold static
# LoggerFactory.getLogger(...) fields, so loading them without it throws NoClassDefFoundError -
# the very condition the live scenario's skip path exists for.
[ -f "${SLF4J_JAR}" ] || curl -fsSL \
  "https://repo1.maven.org/maven2/org/slf4j/slf4j-api/${SLF4J_VERSION}/slf4j-api-${SLF4J_VERSION}.jar" \
  -o "${SLF4J_JAR}"

CP="${CLIENTS_JAR}:${SLF4J_JAR}"
javac -cp "${CP}" -d "${WORK_DIR}" "${SRC_DIR}/JavaGroupMember.java"

# --probe: download, compile and force-load only. The live spec runs this once at collection time
# to decide whether the scenario can run at all - a missing JDK, an unreachable Maven Central or a
# NoClassDefFoundError all fail here, before jest has built the suite, where a skip is still
# expressible. The probe loads the consumer and the three assignors explicitly, because the JVM
# would otherwise resolve them lazily and a classpath hole would only surface mid-run.
exec java -cp "${CP}:${WORK_DIR}" JavaGroupMember "$@"
