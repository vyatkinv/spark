#!/bin/bash
set -e

REALM="TEST.REALM"

kdb5_util create -s -r "$REALM" -P masterpassword <<< "masterpassword"

kadmin.local -q "addprinc -randkey spark/localhost@$REALM"
kadmin.local -q "addprinc -randkey hdfs/localhost@$REALM"
kadmin.local -q "addprinc -randkey yarn/localhost@$REALM"

kadmin.local -q "ktadd -k /keytabs/spark.keytab spark/localhost@$REALM"
kadmin.local -q "ktadd -k /keytabs/all.keytab spark/localhost@$REALM hdfs/localhost@$REALM yarn/localhost@$REALM"

chmod 644 /keytabs/*.keytab

echo "KDC ready with realm $REALM"

krb5kdc -n
