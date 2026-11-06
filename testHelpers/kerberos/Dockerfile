ARG KDC_BASE_IMAGE
FROM ${KDC_BASE_IMAGE}
# A base that already carries the MIT Kerberos tools needs nothing; a stock Ubuntu gets them from its archive.
RUN command -v krb5kdc >/dev/null 2>&1 || (apt-get update \
  && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends krb5-kdc krb5-admin-server krb5-user \
  && rm -rf /var/lib/apt/lists/*)
RUN command -v krb5kdc && command -v kdb5_util && command -v kadmin.local && command -v kinit && command -v klist
EXPOSE 88/tcp 88/udp
