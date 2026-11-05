ARG KDC_BASE_IMAGE
FROM ${KDC_BASE_IMAGE}
RUN command -v krb5kdc && command -v kdb5_util && command -v kadmin.local && command -v kinit && command -v klist
EXPOSE 88/tcp 88/udp
