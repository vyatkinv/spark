FROM debian:bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends krb5-kdc krb5-admin-server \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /keytabs

COPY krb5.conf /etc/krb5.conf
COPY kdc.conf /etc/krb5kdc/kdc.conf
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 88

ENTRYPOINT ["/entrypoint.sh"]
