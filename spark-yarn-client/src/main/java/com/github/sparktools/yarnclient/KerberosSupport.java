package com.github.sparktools.yarnclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Utility for Kerberos authentication via Hadoop UGI.
 *
 * <p>All HDFS / YARN operations performed after {@link #login} inherit the
 * authenticated subject automatically through the UGI thread-local context.
 */
public final class KerberosSupport {

    private static final Logger log = LoggerFactory.getLogger(KerberosSupport.class);

    private KerberosSupport() {}

    /**
     * Performs a keytab-based Kerberos login and returns the resulting UGI.
     * Also sets {@code hadoop.security.authentication=kerberos} on the supplied
     * configuration so that subsequent FileSystem / YarnClient calls use SASL.
     */
    public static UserGroupInformation login(
            Configuration conf, String principal, String keytabPath) throws IOException {
        conf.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(conf);
        log.info("Kerberos login: principal={}, keytab={}", principal, keytabPath);
        UserGroupInformation.loginUserFromKeytab(principal, keytabPath);
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        log.info("Logged in as: {}", ugi.getUserName());
        return ugi;
    }

    /**
     * Renews the TGT if it is close to expiry.
     * Call periodically from long-running jobs (e.g. every hour).
     */
    public static void reloginIfNeeded() throws IOException {
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        if (ugi.isFromKeytab()) {
            ugi.checkTGTAndReloginFromKeytab();
        }
    }

    public static boolean isKerberosAuthentication(Configuration conf) {
        return "kerberos".equalsIgnoreCase(
                conf.get("hadoop.security.authentication", "simple"));
    }
}
