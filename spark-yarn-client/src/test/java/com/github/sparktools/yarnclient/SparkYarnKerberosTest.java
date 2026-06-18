package com.github.sparktools.yarnclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.*;
import org.testcontainers.DockerClientFactory;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Tests for the Kerberos authentication path using a Dockerized MIT KDC
 * managed by {@link KdcContainer}.
 *
 * <p>Verifies:
 * <ol>
 *   <li>{@link SparkYarnConfig#isKerberosEnabled()} logic (no Docker needed).</li>
 *   <li>{@link KerberosSupport#login} with real KDC credentials from Docker.</li>
 *   <li>Kerberos principal and keytab are forwarded to the Spark conf archive.</li>
 *   <li>{@link KerberosSupport#isKerberosAuthentication} reflects the post-login state.</li>
 * </ol>
 *
 * <p>Requires Docker for Kerberos login tests. Config-only tests run without Docker.
 *
 * <p>Run with: {@code mvn test -pl spark-yarn-client -Dtest=SparkYarnKerberosTest}
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SparkYarnKerberosTest {

    private static KdcContainer kdc;
    private static java.nio.file.Path keytabFile;
    private static String testPrincipal;

    @BeforeAll
    static void startKdc() throws Exception {
        // Config-only tests (1-4) run without Docker; KDC tests (5+) need Docker
        boolean dockerAvailable = false;
        try {
            dockerAvailable = DockerClientFactory.instance().isDockerAvailable();
        } catch (Exception e) {
            // Docker not available — KDC tests will be skipped
        }

        if (dockerAvailable) {
            kdc = new KdcContainer();
            kdc.start();

            keytabFile = kdc.copyKeytabToHost();
            testPrincipal = kdc.getPrincipal();
            kdc.writeKrb5Conf();
        } else {
            System.err.println("Docker not available for Testcontainers — KDC tests will be skipped");
        }
    }

    @AfterAll
    static void stopKdc() {
        if (kdc != null) kdc.stop();
        try { UserGroupInformation.reset(); } catch (Exception ignored) {}
    }

    // ── SparkYarnConfig.isKerberosEnabled() (no Docker needed) ───────────────

    @Test
    @Order(1)
    void isKerberosEnabled_trueWhenBothPrincipalAndKeytabSet() {
        assertTrue(SparkYarnConfig.builder()
                .hdfsUri("hdfs://host:8020")
                .kerberos("user@REALM.COM", "/etc/spark.keytab")
                .build().isKerberosEnabled());
    }

    @Test
    @Order(2)
    void isKerberosEnabled_falseWhenNeitherSet() {
        assertFalse(SparkYarnConfig.builder()
                .hdfsUri("hdfs://host:8020")
                .build().isKerberosEnabled());
    }

    @Test
    @Order(3)
    void isKerberosEnabled_falseWhenKeytabBlank() {
        assertFalse(SparkYarnConfig.builder()
                .hdfsUri("hdfs://host:8020")
                .kerberos("user@REALM.COM", "")
                .build().isKerberosEnabled());
    }

    @Test
    @Order(4)
    void isKerberosEnabled_falseWhenPrincipalBlank() {
        assertFalse(SparkYarnConfig.builder()
                .hdfsUri("hdfs://host:8020")
                .kerberos("", "/etc/spark.keytab")
                .build().isKerberosEnabled());
    }

    // ── KerberosSupport.login() with Dockerized KDC ──────────────────────────

    @Test
    @Order(5)
    void kerberosLogin_succeedsWithDockerKdcCredentials() throws Exception {
        assumeTrue(kdc != null, "Skipped: Docker is not available");

        Configuration conf = new Configuration();
        UserGroupInformation ugi = KerberosSupport.login(
                conf, testPrincipal, keytabFile.toAbsolutePath().toString());

        assertNotNull(ugi, "login must return a non-null UGI");
        assertTrue(ugi.isFromKeytab(), "UGI must be backed by a keytab");
        assertEquals(testPrincipal, ugi.getUserName(),
                "Logged-in principal must match the requested one");
    }

    @Test
    @Order(6)
    void kerberosLogin_setsHadoopSecurityAuthenticationOnConf() throws Exception {
        assumeTrue(kdc != null, "Skipped: Docker is not available");

        Configuration conf = new Configuration();
        KerberosSupport.login(conf, testPrincipal, keytabFile.toAbsolutePath().toString());

        assertEquals("kerberos", conf.get("hadoop.security.authentication"),
                "KerberosSupport.login must set hadoop.security.authentication=kerberos");
        assertTrue(KerberosSupport.isKerberosAuthentication(conf),
                "isKerberosAuthentication must return true after login");
    }

    // ── Kerberos properties forwarded to Spark conf (no Docker needed) ───────

    @Test
    @Order(7)
    void kerberosProperties_includedInSparkConf() {
        String principal = "spark/localhost@MY.REALM";
        String keytab = "/etc/keytabs/spark.keytab";

        SparkYarnConfig config = SparkYarnConfig.builder()
                .hdfsUri("hdfs://host:8020")
                .kerberos(principal, keytab)
                .build();

        SparkJobConfig job = SparkJobConfig.builder()
                .appName("KerberosPropertyTest")
                .mainClass("com.example.App")
                .localJarPath("/fake/app.jar")
                .deployMode("cluster")
                .build();

        SparkYarnSubmitter submitter = new SparkYarnSubmitter(config, null, null);
        Properties props = submitter.buildSparkProperties(
                job,
                "hdfs://host:8020/spark-apps/jars/app.jar",
                new Path("hdfs://host:8020/.sparkStaging/application_123_0001"));

        assertEquals(principal, props.getProperty("spark.kerberos.principal"),
                "Principal must be forwarded to spark.kerberos.principal");
        assertEquals(keytab, props.getProperty("spark.kerberos.keytab"),
                "Keytab path must be forwarded to spark.kerberos.keytab");
    }

    @Test
    @Order(8)
    void kerberosProperties_absentInSparkConf_whenDisabled() {
        SparkYarnConfig config = SparkYarnConfig.builder()
                .hdfsUri("hdfs://host:8020")
                .build();

        SparkJobConfig job = SparkJobConfig.builder()
                .appName("NoKerberos")
                .mainClass("com.example.App")
                .localJarPath("/fake/app.jar")
                .deployMode("cluster")
                .build();

        SparkYarnSubmitter submitter = new SparkYarnSubmitter(config, null, null);
        Properties props = submitter.buildSparkProperties(
                job,
                "hdfs://host:8020/spark-apps/jars/app.jar",
                new Path("hdfs://host:8020/.sparkStaging/application_456_0001"));

        assertNull(props.getProperty("spark.kerberos.principal"),
                "Principal must NOT be in Spark conf when Kerberos is disabled");
        assertNull(props.getProperty("spark.kerberos.keytab"),
                "Keytab must NOT be in Spark conf when Kerberos is disabled");
    }
}
