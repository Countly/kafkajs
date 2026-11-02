#!/bin/bash
set -euo pipefail
umask 077
export KRB5_CONFIG=/etc/krb5.conf
if [ -e /state/principal ] || [ -e /state/ready ]; then
  echo 'Refusing reused KDC state' >&2
  exit 1
fi
timeout -k 5s 20s kdb5_util create -r KAFKAJS.TEST -s -P fixture-only-master-key
timeout -k 5s 20s kadmin.local -r KAFKAJS.TEST -q 'addprinc -randkey kafka/localhost@KAFKAJS.TEST'
timeout -k 5s 20s kadmin.local -r KAFKAJS.TEST -q 'addprinc -randkey client@KAFKAJS.TEST'
timeout -k 5s 20s kadmin.local -r KAFKAJS.TEST -q 'ktadd -k /state/kafka.keytab kafka/localhost@KAFKAJS.TEST'
timeout -k 5s 20s kadmin.local -r KAFKAJS.TEST -q 'ktadd -k /state/client.keytab client@KAFKAJS.TEST'
chmod 0644 /state/kafka.keytab
: > /state/ready
exec krb5kdc -n -r KAFKAJS.TEST
