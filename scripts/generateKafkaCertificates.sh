#!/usr/bin/env bash
# Regenerates the test-cluster TLS material (private CA + one server keystore shared by kafka1..3).
# Non-interactive. Output: testHelpers/certs/*. Password everywhere: testtest.
set -euo pipefail

DIR="$(cd "$(dirname "$0")/.." && pwd)/testHelpers/certs"
PASS=testtest
DAYS=3650
SAN="DNS:localhost,DNS:kafka1,DNS:kafka2,DNS:kafka3,IP:127.0.0.1"
WORK="$(mktemp -d)"

rm -f "$DIR"/ca-key "$DIR"/ca-cert.srl "$DIR"/cert-file "$DIR"/cert-signed \
      "$DIR"/kafka.server.keystore.jks "$DIR"/kafka.server.truststore.jks

# 1. private CA
openssl req -new -x509 -sha256 -newkey rsa:2048 -nodes -days "$DAYS" \
  -subj "/C=SE/ST=Unknown/L=Stockholm/O=kafkajs/OU=kafkajs/CN=kafkajs-test-ca" \
  -keyout "$DIR/ca-key" -out "$WORK/ca-cert.pem"

# 2. server key + CSR (keystore alias localhost)
keytool -genkeypair -keystore "$WORK/kafka.server.keystore.jks" -storetype JKS -alias localhost \
  -keyalg RSA -keysize 2048 -sigalg SHA256withRSA -validity "$DAYS" \
  -dname "CN=localhost, OU=kafkajs, O=kafkajs, L=Stockholm, ST=Unknown, C=SE" \
  -ext "SAN=$SAN" -storepass "$PASS" -keypass "$PASS"
keytool -certreq -keystore "$WORK/kafka.server.keystore.jks" -alias localhost \
  -file "$DIR/cert-file" -storepass "$PASS" -keypass "$PASS"

# 3. CA signs it (SAN must be re-added at signing time)
printf "subjectAltName=%s\nextendedKeyUsage=serverAuth,clientAuth\n" "$SAN" > "$WORK/ext.cnf"
openssl x509 -req -sha256 -CA "$WORK/ca-cert.pem" -CAkey "$DIR/ca-key" -CAcreateserial \
  -in "$DIR/cert-file" -out "$WORK/server-signed.pem" -days "$DAYS" -extfile "$WORK/ext.cnf"
mv "$WORK/ca-cert.srl" "$DIR/ca-cert.srl" 2>/dev/null || true

# 4. keystore = CA + signed leaf; truststore = CA
keytool -importcert -noprompt -keystore "$WORK/kafka.server.keystore.jks" -alias CARoot \
  -file "$WORK/ca-cert.pem" -storepass "$PASS"
keytool -importcert -noprompt -keystore "$WORK/kafka.server.keystore.jks" -alias localhost \
  -file "$WORK/server-signed.pem" -storepass "$PASS" -keypass "$PASS"
keytool -importcert -noprompt -keystore "$WORK/kafka.server.truststore.jks" -storetype JKS \
  -alias CARoot -file "$WORK/ca-cert.pem" -storepass "$PASS"

mv "$WORK/kafka.server.keystore.jks" "$WORK/kafka.server.truststore.jks" "$DIR/"
# Clients trust the CA: testHelpers/index.js reads cert-signed as the `ca` option.
cp "$WORK/ca-cert.pem" "$DIR/cert-signed"

printf "%s\n" "$PASS" > "$DIR/keystore_creds"
printf "%s\n" "$PASS" > "$DIR/sslkey_creds"
printf "%s\n" "$PASS" > "$DIR/truststore_creds"
rm -rf "$WORK"
echo "certificates regenerated in $DIR"
