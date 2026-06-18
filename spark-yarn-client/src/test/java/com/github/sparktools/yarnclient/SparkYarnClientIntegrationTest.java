package com.github.sparktools.yarnclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.junit.jupiter.api.*;
import org.testcontainers.DockerClientFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration tests using Testcontainers: a Dockerized Hadoop cluster
 * (HDFS + YARN) runs in a container managed by {@link HadoopContainer}.
 *
 * <p>Spark jars from the test classpath are bind-mounted into the container
 * so that YARN containers have Spark classes available. The test submits
 * a real Spark word-count job ({@link SimpleSparkApp}) and verifies the output.
 *
 * <p>Requires Docker. Tests are automatically skipped if Docker is unavailable.
 *
 * <p>Run with: {@code mvn test -pl spark-yarn-client -Dtest=SparkYarnClientIntegrationTest}
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SparkYarnClientIntegrationTest {

    private static HadoopContainer hadoop;
    private static Configuration conf;
    private static String hdfsUri;
    private static SparkYarnClient client;
    private static java.nio.file.Path tempDir;
    private static java.nio.file.Path testAppJar;
    private static String inputHdfsPath;

    @BeforeAll
    static void startCluster() throws Exception {
        assumeTrue(DockerClientFactory.instance().isDockerAvailable(),
                "Skipped: Docker is not available");

        tempDir = Files.createTempDirectory("spark-yarn-client-it");

        // ── Start Hadoop container ───────────────────────────────────────────
        hadoop = new HadoopContainer();
        hadoop.start();

        hdfsUri = hadoop.getHdfsUri();
        conf = hadoop.getHadoopConf();

        // ── Upload sample text for word-count tests ──────────────────────────
        inputHdfsPath = hdfsUri + "/test-input/words.txt";
        FileSystem fs = FileSystem.newInstance(URI.create(hdfsUri), conf);
        try {
            Path inputPath = new Path("/test-input/words.txt");
            fs.mkdirs(inputPath.getParent());
            try (FSDataOutputStream out = fs.create(inputPath)) {
                out.write(("hello world hello spark\n"
                         + "world spark spark\n")
                        .getBytes(StandardCharsets.UTF_8));
            }
        } finally {
            fs.close();
        }

        // ── Build test-app JAR ───────────────────────────────────────────────
        testAppJar = buildTestAppJar();

        // ── Create client ────────────────────────────────────────────────────
        SparkYarnConfig clientConfig = SparkYarnConfig.builder()
                .hdfsUri(hdfsUri)
                .hdfsJarUploadDir("/spark-apps/jars")
                .hadoopConf(conf)
                .sparkConf("spark.yarn.jars", "local:" + testAppJar.toAbsolutePath())
                .sparkConf("spark.yarn.populateHadoopClasspath", "true")
                .sparkConf("spark.shuffle.service.enabled", "false")
                .sparkConf("spark.dynamicAllocation.enabled", "false")
                .build();

        client = new SparkYarnClient(clientConfig);
    }

    @AfterAll
    static void stopCluster() {
        closeSilently(client);
        if (hadoop != null) hadoop.stop();
    }

    // ── HDFS upload tests ────────────────────────────────────────────────────

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
                .build();

        try (SparkYarnClient deepClient = new SparkYarnClient(deepConfig)) {
            java.nio.file.Path jar = writeFile("deep.jar", "x");
            String hdfsPath = deepClient.uploadJar(jar.toString());
            assertTrue(FileSystem.get(URI.create(hdfsUri), conf)
                    .exists(new Path(hdfsPath)));
        }
    }

    // ── YARN connectivity tests ──────────────────────────────────────────────

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

    // ── Real Spark word-count test ───────────────────────────────────────────

    @Test
    @Order(7)
    @Timeout(300)
    void submitSparkApp_countsWordsOnHdfs() throws Exception {
        String outputHdfsPath = hdfsUri + "/test-output/wordcount-" + System.currentTimeMillis();

        SparkJobConfig job = SparkJobConfig.builder()
                .appName("WordCount-" + System.currentTimeMillis())
                .mainClass(SimpleSparkApp.class.getName())
                .localJarPath(testAppJar.toString())
                .addArg(inputHdfsPath)
                .addArg(outputHdfsPath)
                .deployMode("cluster")
                .numExecutors(1)
                .executorMemory("512m")
                .executorCores(1)
                .driverMemory("512m")
                .driverCores(1)
                .build();

        SubmittedApplication app = client.submit(job);
        assertNotNull(app.getApplicationId(), "Application ID must be assigned");

        ApplicationInfo result = app.waitForTermination(270_000);

        assertEquals(YarnApplicationState.FINISHED, result.getState(),
                "Application must finish. Diagnostics: " + result.getDiagnostics());
        assertEquals(FinalApplicationStatus.SUCCEEDED, result.getFinalStatus(),
                "Final status must be SUCCEEDED. Diagnostics: " + result.getDiagnostics());

        Map<String, Integer> expected = new LinkedHashMap<>();
        expected.put("hello", 2);
        expected.put("world", 2);
        expected.put("spark", 3);
        verifyWordCountOutput(outputHdfsPath, expected);
    }

    @Test
    @Order(8)
    @Timeout(120)
    void killApplication_terminatesRunningApp() throws Exception {
        String outputHdfsPath = hdfsUri + "/test-output/kill-test-" + System.currentTimeMillis();

        SparkJobConfig job = SparkJobConfig.builder()
                .appName("KillTest-" + System.currentTimeMillis())
                .mainClass(SimpleSparkApp.class.getName())
                .localJarPath(testAppJar.toString())
                .addArg(inputHdfsPath)
                .addArg(outputHdfsPath)
                .deployMode("cluster")
                .numExecutors(1)
                .executorMemory("512m")
                .driverMemory("512m")
                .build();

        SubmittedApplication app = client.submit(job);
        String appIdStr = app.getApplicationId().toString();

        long deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline) {
            ApplicationInfo info = app.getStatus();
            if (info.getState() != YarnApplicationState.NEW
                    && info.getState() != YarnApplicationState.NEW_SAVING) break;
            Thread.sleep(500);
        }

        client.killApplication(appIdStr);

        ApplicationInfo afterKill = app.waitForTermination(30_000);
        assertTrue(afterKill.isFinished(),
                "Application must be in terminal state after kill: " + afterKill.getState());
    }

    // ── Keytab distribution test ─────────────────────────────────────────────

    @Test
    @Order(9)
    void distributeKeytab_uploadsAndRewritesProperty() throws Exception {
        java.nio.file.Path fakeKeytab = tempDir.resolve("spark-test.keytab");
        Files.write(fakeKeytab, "fake-keytab-bytes".getBytes(StandardCharsets.UTF_8));

        SparkYarnConfig krbConfig = SparkYarnConfig.builder()
                .hdfsUri(hdfsUri)
                .hadoopConf(conf)
                .kerberos("spark/host@REALM", fakeKeytab.toAbsolutePath().toString())
                .build();

        FileSystem fs = FileSystem.newInstance(URI.create(hdfsUri), conf);
        try {
            SparkYarnSubmitter submitter = new SparkYarnSubmitter(krbConfig, fs, null);

            Path stagingDir = new Path(hdfsUri + "/.sparkStaging/keytab-test");
            fs.mkdirs(stagingDir);

            Properties props = new Properties();
            props.setProperty("spark.kerberos.keytab", fakeKeytab.toAbsolutePath().toString());

            Map<String, org.apache.hadoop.yarn.api.records.LocalResource> localResources =
                    new LinkedHashMap<>();

            submitter.distributeKeytab(stagingDir, props, localResources);

            assertEquals("spark-test.keytab", props.getProperty("spark.kerberos.keytab"),
                    "spark.kerberos.keytab must be rewritten to the localized filename");
            assertTrue(localResources.containsKey("spark-test.keytab"),
                    "keytab must be registered as a LocalResource");
            assertTrue(fs.exists(new Path(stagingDir, "spark-test.keytab")),
                    "keytab must be uploaded to the HDFS staging directory");
        } finally {
            fs.close();
        }
    }

    // ── Memory overhead tests ────────────────────────────────────────────────

    @Test
    @Order(10)
    void memoryOverhead_addedToAmResource() {
        int driverMb = SparkYarnSubmitter.MemoryParser.toMb("4g");
        int overhead = Math.max(
                (int) (driverMb * SparkYarnSubmitter.MEMORY_OVERHEAD_FACTOR),
                SparkYarnSubmitter.MEMORY_OVERHEAD_MIN_MB);
        assertEquals(4096, driverMb);
        assertEquals(409, overhead);
        assertEquals(4505, driverMb + overhead);
    }

    @Test
    @Order(11)
    void memoryOverhead_usesMinimumFor384mb() {
        int driverMb = SparkYarnSubmitter.MemoryParser.toMb("1g");
        int overhead = Math.max(
                (int) (driverMb * SparkYarnSubmitter.MEMORY_OVERHEAD_FACTOR),
                SparkYarnSubmitter.MEMORY_OVERHEAD_MIN_MB);
        assertEquals(384, overhead);
    }

    // ── Staging dir permissions test ─────────────────────────────────────────

    @Test
    @Order(12)
    void stagingDir_createdWith700Permissions() throws Exception {
        FileSystem fs = FileSystem.newInstance(URI.create(hdfsUri), conf);
        try {
            Path stagingDir = new Path(hdfsUri + "/.sparkStaging/perm-test");
            FileSystem.mkdirs(fs, stagingDir, SparkYarnSubmitter.STAGING_DIR_PERMISSION);

            assertEquals("rwx------",
                    fs.getFileStatus(stagingDir).getPermission().toString(),
                    "Staging dir must have 700 permissions");
        } finally {
            fs.close();
        }
    }

    // ── SparkYarnSubmitter unit tests ─────────────────────────────────────────

    @Test
    @Order(13)
    void memoryParser_parsesVariousFormats() {
        assertEquals(1024, SparkYarnSubmitter.MemoryParser.toMb("1g"));
        assertEquals(512,  SparkYarnSubmitter.MemoryParser.toMb("512m"));
        assertEquals(2048, SparkYarnSubmitter.MemoryParser.toMb("2G"));
        assertEquals(768,  SparkYarnSubmitter.MemoryParser.toMb("768"));
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static java.nio.file.Path writeFile(String name, String content) throws IOException {
        java.nio.file.Path p = tempDir.resolve(name);
        Files.write(p, content.getBytes(StandardCharsets.UTF_8));
        return p;
    }

    private static java.nio.file.Path buildTestAppJar() throws IOException {
        java.nio.file.Path jarPath = tempDir.resolve("test-app.jar");
        String className = SimpleSparkApp.class.getName().replace('.', '/') + ".class";
        java.nio.file.Path classFile = Paths.get("target/test-classes", className);

        Manifest manifest = new Manifest();
        manifest.getMainAttributes().putValue("Manifest-Version", "1.0");
        manifest.getMainAttributes().putValue("Main-Class", SimpleSparkApp.class.getName());

        try (JarOutputStream jos = new JarOutputStream(
                new FileOutputStream(jarPath.toFile()), manifest)) {
            jos.putNextEntry(new JarEntry(className));
            Files.copy(classFile, jos);
            jos.closeEntry();
        }
        return jarPath;
    }

    private void verifyWordCountOutput(String hdfsOutputDir, Map<String, Integer> expected)
            throws Exception {
        FileSystem fs = FileSystem.newInstance(URI.create(hdfsUri), conf);
        try {
            Path outputPath = new Path(hdfsOutputDir);
            assertTrue(fs.exists(outputPath), "Output directory must exist: " + hdfsOutputDir);

            StringBuilder content = new StringBuilder();
            for (FileStatus status : fs.listStatus(outputPath)) {
                if (status.getPath().getName().startsWith("part-")) {
                    try (FSDataInputStream in = fs.open(status.getPath())) {
                        ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        byte[] buf = new byte[4096];
                        int n;
                        while ((n = in.read(buf)) > 0) {
                            baos.write(buf, 0, n);
                        }
                        content.append(baos.toString(StandardCharsets.UTF_8.name()));
                    }
                }
            }

            String output = content.toString();
            for (Map.Entry<String, Integer> entry : expected.entrySet()) {
                String expectedLine = "(" + entry.getKey() + "," + entry.getValue() + ")";
                assertTrue(output.contains(expectedLine),
                        "Output must contain '" + expectedLine + "', actual:\n" + output);
            }
        } finally {
            fs.close();
        }
    }

    private static void closeSilently(AutoCloseable c) {
        if (c != null) try { c.close(); } catch (Exception ignored) {}
    }
}
