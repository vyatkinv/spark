package com.github.sparktools.yarnclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.*;

import java.io.File;
import java.nio.file.Files;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the Kerberos authentication path in {@link SparkYarnClient}.
 *
 * <p>Uses an in-process {@link MiniKdc} to issue real Kerberos credentials — no external
 * KDC is required.  These tests verify:
 * <ol>
 *   <li>{@link SparkYarnConfig#isKerberosEnabled()} logic.</li>
 *   <li>{@link KerberosSupport#login} succeeds with valid MiniKdc credentials.</li>
 *   <li>Kerberos principal and keytab are forwarded to the Spark conf archive.</li>
 *   <li>{@link KerberosSupport#isKerberosAuthentication} reflects the post-login state.</li>
 * </ol>
 *
 * <p>This class runs after {@link SparkYarnClientIntegrationTest} (alphabetical order ensures
 * "Kerberos" sorts after "Client").  The global {@link UserGroupInformation} is reset in
 * {@code @AfterAll} so the JVM is left in a clean state.
 *
 * <p>Run with: {@code mvn test -pl spark-yarn-client -Dtest=SparkYarnKerberosTest}
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SparkYarnKerberosTest {

    private static MiniKdc kdc;
    private static File keytabFile;
    private static String testPrincipal;

    @BeforeAll
    static void startKdc() throws Exception {
        File workDir = Files.createTempDirectory("spark-yarn-kdc-").toFile();

        Properties kdcConf = MiniKdc.createConf();
        // Shorten ticket lifetime for faster tests
        kdcConf.setProperty(MiniKdc.MAX_TICKET_LIFETIME, "60000");
        kdcConf.setProperty(MiniKdc.MAX_RENEWABLE_LIFETIME, "600000");

        kdc = new MiniKdc(kdcConf, workDir);
        kdc.start();

        // principal name: spark/localhost@<REALM>
        testPrincipal = "spark/localhost@" + kdc.getRealm();
        keytabFile = new File(workDir, "spark.keytab");
        kdc.createPrincipal(keytabFile, "spark/localhost");
    }

    @AfterAll
    static void stopKdc() {
        if (kdc != null) kdc.stop();
        // Restore SIMPLE auth so this class does not contaminate other JVM state
        try { UserGroupInformation.reset(); } catch (Exception ignored) {}
    }

    // ── SparkYarnConfig.isKerberosEnabled() ──────────────────────────────────

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

    // ── KerberosSupport.login() with real MiniKdc ─────────────────────────────

    /**
     * Verifies that {@link KerberosSupport#login} obtains a real TGT from MiniKdc.
     *
     * <p>Note: this test changes the global {@link UserGroupInformation} to Kerberos mode.
     * It must run after all non-Kerberos tests to avoid side effects.  The {@code @AfterAll}
     * calls {@code UserGroupInformation.reset()} to restore SIMPLE auth.
     */
    @Test
    @Order(5)
    void kerberosLogin_succeedsWithMiniKdcCredentials() throws Exception {
        // MiniKdc.start() sets java.security.krb5.conf system property → UGI picks it up
        Configuration conf = new Configuration();
        UserGroupInformation ugi = KerberosSupport.login(
                conf, testPrincipal, keytabFile.getAbsolutePath());

        assertNotNull(ugi, "login must return a non-null UGI");
        assertTrue(ugi.isFromKeytab(), "UGI must be backed by a keytab");
        assertEquals(testPrincipal, ugi.getUserName(),
                "Logged-in principal must match the requested one");
    }

    @Test
    @Order(6)
    void kerberosLogin_setsHadoopSecurityAuthenticationOnConf() throws Exception {
        Configuration conf = new Configuration();
        KerberosSupport.login(conf, testPrincipal, keytabFile.getAbsolutePath());

        assertEquals("kerberos", conf.get("hadoop.security.authentication"),
                "KerberosSupport.login must set hadoop.security.authentication=kerberos");
        assertTrue(KerberosSupport.isKerberosAuthentication(conf),
                "isKerberosAuthentication must return true after login");
    }

    // ── Kerberos properties forwarded to Spark conf ───────────────────────────

    /**
     * Verifies that {@link SparkYarnSubmitter#buildSparkProperties} includes
     * {@code spark.kerberos.principal} and {@code spark.kerberos.keytab} when
     * the config has Kerberos enabled.
     *
     * <p>{@code buildSparkProperties} initially sets the keytab to the local
     * filesystem path.  During submission, {@code distributeKeytab} uploads the
     * keytab to HDFS and rewrites the property to just the filename — see
     * {@code SparkYarnClientIntegrationTest.distributeKeytab_uploadsAndRewritesProperty}.
     *
     * <p>Uses {@link SparkYarnSubmitter} directly (package-private) with null FileSystem
     * and YarnClient — {@code buildSparkProperties} does not use those fields.
     */
    @Test
    @Order(7)
    void kerberosProperties_includedInSparkConf() {
        SparkYarnConfig config = SparkYarnConfig.builder()
                .hdfsUri("hdfs://host:8020")
                .kerberos(testPrincipal, keytabFile.getAbsolutePath())
                .build();

        SparkJobConfig job = SparkJobConfig.builder()
                .appName("KerberosPropertyTest")
                .mainClass("com.example.App")
                .localJarPath("/fake/app.jar")
                .deployMode("cluster")
                .build();

        // SparkYarnSubmitter and buildSparkProperties are both package-private;
        // tests in the same package can access them directly.
        SparkYarnSubmitter submitter = new SparkYarnSubmitter(config, null, null);
        Properties props = submitter.buildSparkProperties(
                job,
                "hdfs://host:8020/spark-apps/jars/app.jar",
                new Path("hdfs://host:8020/.sparkStaging/application_123_0001"));

        assertEquals(testPrincipal, props.getProperty("spark.kerberos.principal"),
                "Principal must be forwarded to spark.kerberos.principal");
        assertEquals(keytabFile.getAbsolutePath(), props.getProperty("spark.kerberos.keytab"),
                "Keytab path must be forwarded to spark.kerberos.keytab");
    }

    /**
     * Verifies that {@code spark.kerberos.principal} / {@code spark.kerberos.keytab}
     * are NOT present in the Spark conf when Kerberos is disabled.
     */
    @Test
    @Order(8)
    void kerberosProperties_absentInSparkConf_whenDisabled() {
        SparkYarnConfig config = SparkYarnConfig.builder()
                .hdfsUri("hdfs://host:8020")
                .build();  // no kerberos()

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
