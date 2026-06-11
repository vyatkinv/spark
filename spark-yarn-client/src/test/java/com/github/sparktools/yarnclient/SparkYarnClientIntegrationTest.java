package com.github.sparktools.yarnclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests: spins up MiniDFSCluster + MiniYARNCluster and exercises
 * the full {@link SparkYarnClient} code path — including HDFS upload, conf-archive
 * creation, YARN ApplicationSubmissionContext assembly, and actual container
 * execution (using {@link TestApplicationMaster} instead of Spark's AM).
 *
 * <p>Run with: {@code mvn verify}
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SparkYarnClientIntegrationTest {

    private static MiniDFSCluster miniDFS;
    private static MiniYARNCluster miniYARN;
    private static Configuration conf;
    private static String hdfsUri;
    private static SparkYarnClient client;
    private static java.nio.file.Path tempDir;

    // Path to the test-AM jar built during @BeforeAll
    private static java.nio.file.Path testAmJar;

    @BeforeAll
    static void startCluster() throws Exception {
        tempDir = Files.createTempDirectory("spark-yarn-client-it");

        // ── MiniDFS ──────────────────────────────────────────────────────────
        conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tempDir.resolve("dfs").toString());
        conf.setBoolean("dfs.permissions.enabled", false);

        miniDFS = new MiniDFSCluster.Builder(conf)
                .numDataNodes(1).format(true).build();
        miniDFS.waitActive();

        hdfsUri = "hdfs://127.0.0.1:" + miniDFS.getNameNodePort();
        conf.set("fs.defaultFS", hdfsUri);

        // ── MiniYARN ─────────────────────────────────────────────────────────
        YarnConfiguration yarnConf = new YarnConfiguration(conf);
        yarnConf.setInt(YarnConfiguration.NM_PMEM_MB, 2048);
        yarnConf.setInt(YarnConfiguration.NM_VCORES, 4);
        yarnConf.setBoolean(YarnConfiguration.NM_PMEM_CHECK_ENABLED, false);
        yarnConf.setBoolean(YarnConfiguration.NM_VMEM_CHECK_ENABLED, false);
        yarnConf.set(YarnConfiguration.NM_AUX_SERVICES, "");

        // Make the current JVM classpath (which includes test-classes + all deps)
        // available inside YARN containers so TestApplicationMaster can run.
        String absoluteClasspath = absoluteClasspath();
        yarnConf.set(YarnConfiguration.YARN_APPLICATION_CLASSPATH, absoluteClasspath);

        miniYARN = new MiniYARNCluster("it-cluster", 1, 1, 1);
        miniYARN.init(yarnConf);
        miniYARN.start();

        // Merge dynamic YARN addresses back into shared conf
        conf.addResource(miniYARN.getConfig());

        waitForNodeManagersReady(30_000);

        // ── Build test-AM jar ─────────────────────────────────────────────────
        // The jar bundles only TestApplicationMaster.class from target/test-classes.
        // It is uploaded to HDFS to serve as the "app jar" argument; the actual
        // TestApplicationMaster code is found via the container classpath above.
        testAmJar = buildTestAmJar();

        // ── Create client ─────────────────────────────────────────────────────
        SparkYarnConfig clientConfig = SparkYarnConfig.builder()
                .hdfsUri(hdfsUri)
                .hdfsJarUploadDir("/spark-apps/jars")
                .hadoopConf(conf)
                // Use TestApplicationMaster instead of Spark's AM
                .amClass(TestApplicationMaster.class.getName())
                .build();

        client = new SparkYarnClient(clientConfig);
    }

    @AfterAll
    static void stopCluster() {
        closeSilently(client);
        if (miniYARN != null) try { miniYARN.stop(); } catch (Exception ignored) {}
        if (miniDFS != null) miniDFS.shutdown();
    }

    // ── HDFS upload tests ─────────────────────────────────────────────────────

    @Test
    @Order(1)
    void uploadJar_createsFileOnHdfs() throws Exception {
        java.nio.file.Path localJar = writeFile("upload-test.jar", "fake jar v1");

        String hdfsPath = client.uploadJar(localJar.toString());

        assertTrue(hdfsPath.startsWith("hdfs://"), "must be an HDFS URI");
        assertTrue(hdfsPath.endsWith("/upload-test.jar"));

        FileSystem fs = FileSystem.get(URI.create(hdfsUri), conf);
        assertTrue(fs.exists(new Path(hdfsPath)));
    }

    @Test
    @Order(2)
    void uploadJar_overwritesPreviousVersion() throws Exception {
        java.nio.file.Path jar1 = writeFile("upload-test.jar", "version 1");
        client.uploadJar(jar1.toString());

        java.nio.file.Path jar2 = writeFile("upload-test.jar", "version 2 — longer content");
        String hdfsPath = client.uploadJar(jar2.toString());

        FileSystem fs = FileSystem.get(URI.create(hdfsUri), conf);
        FileStatus status = fs.getFileStatus(new Path(hdfsPath));
        assertEquals(jar2.toFile().length(), status.getLen(),
                "HDFS file size must match the second upload");
    }

    @Test
    @Order(3)
    void uploadJar_preservesFileSize() throws Exception {
        byte[] payload = new byte[128 * 1024];
        Arrays.fill(payload, (byte) 0xAB);
        java.nio.file.Path jar = tempDir.resolve("large.jar");
        Files.write(jar, payload);

        String hdfsPath = client.uploadJar(jar.toString());
        FileSystem fs = FileSystem.get(URI.create(hdfsUri), conf);
        assertEquals(payload.length, fs.getFileStatus(new Path(hdfsPath)).getLen());
    }

    @Test
    @Order(4)
    void uploadJar_createsIntermediateDirectories() throws Exception {
        SparkYarnConfig deepConfig = SparkYarnConfig.builder()
                .hdfsUri(hdfsUri)
                .hdfsJarUploadDir("/deep/nested/dir")
                .hadoopConf(conf)
                .amClass(TestApplicationMaster.class.getName())
                .build();

        try (SparkYarnClient deepClient = new SparkYarnClient(deepConfig)) {
            java.nio.file.Path jar = writeFile("deep.jar", "x");
            String hdfsPath = deepClient.uploadJar(jar.toString());
            assertTrue(FileSystem.get(URI.create(hdfsUri), conf)
                    .exists(new Path(hdfsPath)));
        }
    }

    // ── YARN connectivity tests ───────────────────────────────────────────────

    @Test
    @Order(5)
    void listRunningApplications_returnsNonNullList() throws Exception {
        assertNotNull(client.listRunningApplications());
    }

    @Test
    @Order(6)
    void getApplicationStatus_throwsForUnknownId() {
        assertThrows(Exception.class,
                () -> client.getApplicationStatus("application_0_9999"));
    }

    // ── Full submission test (TestAM running in MiniYARN) ─────────────────────

    /**
     * Submits a YARN application using our library's submission path.
     * The AM is {@link TestApplicationMaster} — it registers with YARN and
     * immediately reports SUCCEEDED, verifying the complete code path:
     *
     * <ol>
     *   <li>{@code uploadJar} → fat-JAR lands on MiniHDFS</li>
     *   <li>{@code SparkYarnSubmitter} builds conf-archive + LocalResources</li>
     *   <li>MiniYARN accepts and launches the container</li>
     *   <li>TestAM registers + unregisters as SUCCEEDED</li>
     *   <li>{@code waitForTermination} observes terminal state</li>
     * </ol>
     */
    @Test
    @Order(7)
    @Timeout(120)
    void submitApplication_completesSuccessfully() throws Exception {
        SparkJobConfig job = SparkJobConfig.builder()
                .appName("TestApp-" + System.currentTimeMillis())
                .mainClass(TestApplicationMaster.class.getName())
                .localJarPath(testAmJar.toString())
                .deployMode("cluster")
                .numExecutors(1)
                .executorMemory("256m")
                .driverMemory("256m")
                .driverCores(1)
                .build();

        SubmittedApplication app = client.submit(job);
        assertNotNull(app.getApplicationId(), "Application ID must be assigned");

        ApplicationInfo result = app.waitForTermination(90_000);

        assertEquals(YarnApplicationState.FINISHED, result.getState(),
                "Application must finish. Diagnostics: " + result.getDiagnostics());
        assertEquals(FinalApplicationStatus.SUCCEEDED, result.getFinalStatus(),
                "Final status must be SUCCEEDED");
    }

    @Test
    @Order(8)
    @Timeout(30)
    void killApplication_terminatesRunningApp() throws Exception {
        // Submit but do not wait for completion — immediately kill
        SparkJobConfig job = SparkJobConfig.builder()
                .appName("KillTest-" + System.currentTimeMillis())
                .mainClass(TestApplicationMaster.class.getName())
                .localJarPath(testAmJar.toString())
                .deployMode("cluster")
                .numExecutors(1)
                .executorMemory("256m")
                .driverMemory("256m")
                .build();

        SubmittedApplication app = client.submit(job);
        String appIdStr = app.getApplicationId().toString();

        // Wait for the application to at least be accepted before killing
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            ApplicationInfo info = app.getStatus();
            if (info.getState() != YarnApplicationState.NEW
                    && info.getState() != YarnApplicationState.NEW_SAVING) break;
            Thread.sleep(500);
        }

        client.killApplication(appIdStr);

        ApplicationInfo afterKill = app.waitForTermination(15_000);
        assertTrue(afterKill.isFinished(),
                "Application must be in terminal state after kill: " + afterKill.getState());
    }

    // ── SparkYarnSubmitter unit tests (no cluster needed) ────────────────────

    @Test
    @Order(9)
    void memoryParser_parsesVariousFormats() {
        assertEquals(1024, SparkYarnSubmitter.MemoryParser.toMb("1g"));
        assertEquals(512,  SparkYarnSubmitter.MemoryParser.toMb("512m"));
        assertEquals(2048, SparkYarnSubmitter.MemoryParser.toMb("2G"));
        assertEquals(768,  SparkYarnSubmitter.MemoryParser.toMb("768"));
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static java.nio.file.Path writeFile(String name, String content) throws IOException {
        java.nio.file.Path p = tempDir.resolve(name);
        Files.write(p, content.getBytes(StandardCharsets.UTF_8));
        return p;
    }

    /**
     * Builds a jar containing only {@link TestApplicationMaster}.class from
     * {@code target/test-classes/}. The jar is uploaded to HDFS as the "app jar"
     * argument — the actual class is resolved via the container classpath.
     */
    private static java.nio.file.Path buildTestAmJar() throws IOException {
        java.nio.file.Path jarPath = tempDir.resolve("test-am.jar");
        String className = TestApplicationMaster.class.getName().replace('.', '/') + ".class";
        java.nio.file.Path classFile = Paths.get("target/test-classes", className);

        Manifest manifest = new Manifest();
        manifest.getMainAttributes().putValue("Manifest-Version", "1.0");
        manifest.getMainAttributes().putValue("Main-Class", TestApplicationMaster.class.getName());

        try (JarOutputStream jos = new JarOutputStream(
                new FileOutputStream(jarPath.toFile()), manifest)) {
            jos.putNextEntry(new JarEntry(className));
            Files.copy(classFile, jos);
            jos.closeEntry();
        }
        return jarPath;
    }

    /** Converts the current JVM classpath to absolute paths for YARN container use. */
    private static String absoluteClasspath() {
        return Arrays.stream(System.getProperty("java.class.path", "").split(File.pathSeparator))
                .map(p -> Paths.get(p).toAbsolutePath().toString())
                .collect(Collectors.joining(File.pathSeparator));
    }

    private static void waitForNodeManagersReady(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try {
                org.apache.hadoop.yarn.server.nodemanager.NodeManager nm =
                        miniYARN.getNodeManager(0);
                if (nm != null
                        && nm.getServiceState() == org.apache.hadoop.service.Service.STATE.STARTED) {
                    return;
                }
            } catch (Exception ignored) {}
            Thread.sleep(500);
        }
    }

    private static void closeSilently(AutoCloseable c) {
        if (c != null) try { c.close(); } catch (Exception ignored) {}
    }
}
