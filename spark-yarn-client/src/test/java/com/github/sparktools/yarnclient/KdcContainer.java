package com.github.sparktools.yarnclient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;

/**
 * Testcontainers wrapper for a MIT Kerberos KDC.
 *
 * <p>Creates a {@code TEST.REALM} with principals for {@code spark/localhost},
 * {@code hdfs/localhost}, and {@code yarn/localhost}. Keytabs are available
 * inside the container at {@code /keytabs/}.
 *
 * <p>After starting, call {@link #copyKeytabToHost()} to extract the keytab
 * and {@link #writeKrb5Conf()} to generate a {@code krb5.conf} pointing to
 * the container's KDC port.
 */
class KdcContainer extends GenericContainer<KdcContainer> {

    private static final Logger log = LoggerFactory.getLogger(KdcContainer.class);

    static final int KDC_PORT = 88;
    static final String REALM = "TEST.REALM";

    KdcContainer() {
        super(new ImageFromDockerfile("spark-yarn-client-kdc-test", false)
                .withDockerfile(resolveDockerfile()));
        withExposedPorts(KDC_PORT);
        waitingFor(Wait.forLogMessage(".*KDC ready.*\\n", 1)
                .withStartupTimeout(Duration.ofMinutes(2)));
    }

    /**
     * Returns the KDC address as seen from the host (for krb5.conf).
     */
    String getKdcAddress() {
        return getHost() + ":" + getMappedPort(KDC_PORT);
    }

    /**
     * Copies the spark keytab from the container to a temporary file on the host.
     */
    Path copyKeytabToHost() throws IOException {
        Path keytabDir = Files.createTempDirectory("kdc-keytabs-");
        Path keytabFile = keytabDir.resolve("spark.keytab");
        copyFileFromContainer("/keytabs/spark.keytab", keytabFile.toString());
        return keytabFile;
    }

    /**
     * Writes a krb5.conf pointing to this container's KDC and returns the path.
     * Sets {@code java.security.krb5.conf} system property.
     */
    Path writeKrb5Conf() throws IOException {
        String kdcHost = getHost();
        int kdcPort = getMappedPort(KDC_PORT);

        String conf = "[libdefaults]\n"
                + "    default_realm = " + REALM + "\n"
                + "    dns_lookup_realm = false\n"
                + "    dns_lookup_kdc = false\n"
                + "    udp_preference_limit = 1\n"
                + "\n"
                + "[realms]\n"
                + "    " + REALM + " = {\n"
                + "        kdc = " + kdcHost + ":" + kdcPort + "\n"
                + "    }\n"
                + "\n"
                + "[domain_realm]\n"
                + "    .localhost = " + REALM + "\n"
                + "    localhost = " + REALM + "\n";

        Path krb5Path = Files.createTempFile("krb5-", ".conf");
        Files.writeString(krb5Path, conf);
        System.setProperty("java.security.krb5.conf", krb5Path.toString());
        log.info("Wrote krb5.conf to {} (kdc={}:{})", krb5Path, kdcHost, kdcPort);
        return krb5Path;
    }

    String getPrincipal() {
        return "spark/localhost@" + REALM;
    }

    private static Path resolveDockerfile() {
        Path[] candidates = {
            Paths.get("src/test/docker/kdc/Dockerfile"),
            Paths.get("spark-yarn-client/src/test/docker/kdc/Dockerfile"),
        };
        for (Path p : candidates) {
            if (Files.exists(p)) return p;
        }
        throw new IllegalStateException(
                "Cannot find KDC Dockerfile. Run from the spark-yarn-client module directory.");
    }
}
