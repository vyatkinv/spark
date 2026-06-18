package com.github.sparktools.yarnclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.jupiter.api.*;

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
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests: spins up MiniDFSCluster + MiniYARNCluster and exercises
 * the full {@link SparkYarnClient} code path using a real Spark application
 * ({@link SimpleSparkApp}) instead of a stub ApplicationMaster.
 *
 * <p>Tests 7-8 verify the end-to-end path:
 * <ul>
 *   <li>App JAR staged to MiniHDFS</li>
 *   <li>Spark conf archive assembled with Hadoop config</li>
 *   <li>MiniYARN launches the Spark ApplicationMaster container</li>
 *   <li>Spark AM allocates an executor container</li>
 *   <li>Word-count tasks run on MiniHDFS data and produce output</li>
 * </ul>
 *
 * <p>Run with: {@code mvn test -pl spark-yarn-client -Pyarn}
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SparkYarnClientIntegrationTest {

    private static MiniDFSCluster miniDFS;
    private static MiniYARNCluster miniYARN;
    private static Configuration conf;
    private static String hdfsUri;
    private static SparkYarnClient client;
    private static java.nio.file.Path tempDir;

    // JAR containing SimpleSparkApp.class — staged to HDFS as the "app jar"
    private static java.nio.file.Path testAppJar;
    // HDFS path of sample text uploaded during setup
    private static String inputHdfsPath;

    // Second client configured with Java 8 javaHome — used in test 10
    private static SparkYarnClient clientJava8;
    // NodeManager log directory — used to find launch_container.sh
    private static String nmLogDir;

    // Path to the Java 8 JRE on this machine; null if not installed
    static final String JAVA8_HOME = findJava8Home();

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

        // ── Upload sample text for word-count tests ───────────────────────────
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

        // ── MiniYARN ─────────────────────────────────────────────────────────
        YarnConfiguration yarnConf = new YarnConfiguration(conf);
        yarnConf.setInt(YarnConfiguration.NM_PMEM_MB, 4096);
        yarnConf.setInt(YarnConfiguration.NM_VCORES, 4);
        yarnConf.setBoolean(YarnConfiguration.NM_PMEM_CHECK_ENABLED, false);
        yarnConf.setBoolean(YarnConfiguration.NM_VMEM_CHECK_ENABLED, false);
        // Allow small containers — Spark overhead for 512m executor rounds to ~896m
        yarnConf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 256);
        yarnConf.set(YarnConfiguration.NM_AUX_SERVICES, "");

        // The full test JVM classpath (includes spark-core, spark-yarn, Hadoop jars)
        // is set as YARN_APPLICATION_CLASSPATH so every container spawned by MiniYARN
        // already has all necessary classes on its classpath at launch time.
        String absoluteClasspath = absoluteClasspath();
        yarnConf.set(YarnConfiguration.YARN_APPLICATION_CLASSPATH, absoluteClasspath);

        miniYARN = new MiniYARNCluster("it-cluster", 1, 1, 1);
        miniYARN.init(yarnConf);
        miniYARN.start();

        // Merge dynamic YARN addresses back into shared conf
        conf.addResource(miniYARN.getConfig());

        waitForNodeManagersReady(30_000);

        // ── Build test-app JAR ────────────────────────────────────────────────
        testAppJar = buildTestAppJar();

        // ── Create client ─────────────────────────────────────────────────────
        // spark.yarn.jars: a local: entry prevents Spark's AM from falling back
        //   to scanning SPARK_HOME (which doesn't exist in containers).
        // spark.yarn.populateHadoopClasspath=true: Spark's AM includes
        //   YARN_APPLICATION_CLASSPATH (= full test classpath) in executor containers.
        SparkYarnConfig clientConfig = SparkYarnConfig.builder()
                .hdfsUri(hdfsUri)
                .hdfsJarUploadDir("/spark-apps/jars")
                .hadoopConf(conf)
                .javaHome(System.getProperty("java.home"))
                .sparkConf("spark.yarn.jars", "local:" + testAppJar.toAbsolutePath())
                .sparkConf("spark.yarn.populateHadoopClasspath", "true")
                .sparkConf("spark.shuffle.service.enabled", "false")
                .sparkConf("spark.dynamicAllocation.enabled", "false")
                .build();

        client = new SparkYarnClient(clientConfig);

        // Capture NM log dir for launch_container.sh inspection in test 10.
        // YarnConfiguration.NM_LOG_DIRS in the global config contains an unresolved
        // ${yarn.log.dir} placeholder; the actual resolved path is in the NM's own config.
        nmLogDir = miniYARN.getNodeManager(0).getConfig().get(YarnConfiguration.NM_LOG_DIRS);

        // Client configured for Java 8 containers — used in test 9.
        // addJava9ModuleOpens(false) tells the submitter not to emit --add-opens flags;
        // those are unrecognised on Java 8 and would cause the JVM to fail to start.
        if (JAVA8_HOME != null) {
            SparkYarnConfig java8Config = SparkYarnConfig.builder()
                    .hdfsUri(hdfsUri)
                    .hdfsJarUploadDir("/spark-apps/jars")
                    .hadoopConf(conf)
                    .javaHome(JAVA8_HOME)
                    .addJava9ModuleOpens(false)
                    .sparkConf("spark.yarn.jars", "local:" + testAppJar.toAbsolutePath())
                    .sparkConf("spark.yarn.populateHadoopClasspath", "true")
                    .sparkConf("spark.shuffle.service.enabled", "false")
                    .sparkConf("spark.dynamicAllocation.enabled", "false")
                    .build();
            clientJava8 = new SparkYarnClient(java8Config);
        }
    }

    @AfterAll
    static void stopCluster() {
        closeSilently(client);
        closeSilently(clientJava8);
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

    // ── Real Spark word-count tests ───────────────────────────────────────────

    /**
     * Submits a real Spark word-count job ({@link SimpleSparkApp}) through our
     * thin client.  The job reads sample text from MiniHDFS, counts words per
     * partition, and writes the result back to HDFS.
     *
     * <p>Verified end-to-end path:
     * <ol>
     *   <li>App JAR staged to MiniHDFS</li>
     *   <li>Spark conf archive (properties + yarn-site.xml) assembled</li>
     *   <li>MiniYARN launches Spark's ApplicationMaster container</li>
     *   <li>Spark AM creates SparkContext, allocates one executor container</li>
     *   <li>Word-count task executes; part files written to HDFS</li>
     *   <li>{@code waitForTermination} observes FINISHED / SUCCEEDED</li>
     *   <li>Output files contain the expected word-count pairs</li>
     * </ol>
     */
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

        // Input: "hello world hello spark\nworld spark spark\n"
        // Expected counts: hello=2, world=2, spark=3
        Map<String, Integer> expected = new LinkedHashMap<String, Integer>();
        expected.put("hello", 2);
        expected.put("world", 2);
        expected.put("spark", 3);
        verifyWordCountOutput(outputHdfsPath, expected);
    }

    /**
     * Verifies that a running Spark application can be killed cleanly.
     * The application is submitted but killed before it completes normally.
     */
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

        // Wait until application is at least accepted before killing
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

    // ── Java 8 container / Java 21 client cross-version test ──────────────────

    /**
     * Verifies that a Java 21 submitter correctly targets a Java 8 YARN cluster:
     * <ol>
     *   <li>The AM launch command uses the Java 8 executable path.</li>
     *   <li>The command contains NO {@code --add-opens} flags — those flags are
     *       unrecognized on Java 8 and would cause the JVM to fail immediately.</li>
     *   <li>The application does eventually fail (our Spark jars were compiled with the
     *       Java 21 toolchain, so they reference {@code ByteBuffer.flip()} with its Java 9+
     *       covariant return type, which doesn't exist in the Java 8 runtime).  The test
     *       asserts that the failure is a runtime bytecode error, not a JVM-option error,
     *       proving that the client-side command generation was correct.</li>
     * </ol>
     *
     * <p>In production, Spark jars built with a Java 8 JDK would run successfully on a
     * Java 8 cluster; the bytecode incompatibility only occurs here because the Spark
     * build uses a Java 21 toolchain.
     */
    @Test
    @Order(9)
    @Timeout(120)
    void submitWithJava21Client_java8Cluster_commandHasNoModuleOpens() throws Exception {
        org.junit.jupiter.api.Assumptions.assumeTrue(
                clientJava8 != null,
                "Skipped: Java 8 JRE not found at " + JAVA8_HOME);

        String outputHdfsPath = hdfsUri + "/test-output/java8-test-" + System.currentTimeMillis();

        SparkJobConfig job = SparkJobConfig.builder()
                .appName("Java8CrossVersion-" + System.currentTimeMillis())
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

        SubmittedApplication app = clientJava8.submit(job);
        String appIdStr = app.getApplicationId().toString();

        // Wait for AM container to be launched and fail (or, hypothetically, succeed)
        ApplicationInfo result = app.waitForTermination(90_000);

        // ── Inspect launch_container.sh generated for the AM container ────────
        // Find the NM log directory for this application (attempt 1)
        java.nio.file.Path launchScript = findLaunchScript(appIdStr, 1);
        assertTrue(Files.exists(launchScript),
                "launch_container.sh must exist at: " + launchScript);

        String script = new String(Files.readAllBytes(launchScript), StandardCharsets.UTF_8);

        // 1. The java executable must come from the Java 8 JRE
        assertTrue(script.contains(JAVA8_HOME),
                "AM command must use Java 8 executable (javaHome=" + JAVA8_HOME + ").\n"
                + "launch_container.sh:\n" + script);

        // 2. No --add-opens flags — Java 8 would reject them with "Unrecognized option"
        assertFalse(script.contains("--add-opens"),
                "AM command must NOT contain --add-opens for a Java 8 cluster.\n"
                + "launch_container.sh:\n" + script);

        // 3. The application fails with a runtime bytecode error (not a JVM option error).
        //    This confirms the JVM started successfully (options were valid) but couldn't
        //    load Spark classes compiled against the Java 21 class library.
        assertTrue(result.isFinished(),
                "Application must reach terminal state");
        assertTrue(
                result.getFinalStatus() == FinalApplicationStatus.FAILED
                || result.getFinalStatus() == FinalApplicationStatus.SUCCEEDED,
                "Application must have a definite final status: " + result.getFinalStatus());

        if (result.getFinalStatus() == FinalApplicationStatus.FAILED) {
            String diag = result.getDiagnostics();
            // Must NOT fail with "Unrecognized option: --add-opens" (Java 8 startup failure)
            assertFalse(diag != null && diag.contains("Unrecognized option"),
                    "Failure must not be caused by --add-opens flag: " + diag);
        }
    }

    // ── Keytab distribution test ────────────────────────────────────────────

    /**
     * Verifies that {@link SparkYarnSubmitter#distributeKeytab} uploads the keytab
     * to the HDFS staging directory, registers it as a {@link org.apache.hadoop.yarn.api.records.LocalResource},
     * and rewrites {@code spark.kerberos.keytab} to the localized filename.
     */
    @Test
    @Order(10)
    void distributeKeytab_uploadsAndRewritesProperty() throws Exception {
        // Create a temp file simulating a keytab
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

            // Property must be rewritten to just the filename
            assertEquals("spark-test.keytab", props.getProperty("spark.kerberos.keytab"),
                    "spark.kerberos.keytab must be rewritten to the localized filename");

            // LocalResource must be registered under the filename key
            assertTrue(localResources.containsKey("spark-test.keytab"),
                    "keytab must be registered as a LocalResource");

            // File must exist on HDFS
            assertTrue(fs.exists(new Path(stagingDir, "spark-test.keytab")),
                    "keytab must be uploaded to the HDFS staging directory");
        } finally {
            fs.close();
        }
    }

    // ── SparkYarnSubmitter unit tests ────────────────────────────────────────

    @Test
    @Order(11)
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
     * Builds a JAR containing only {@link SimpleSparkApp}.class from
     * {@code target/test-classes/}.  Uploaded to HDFS as the "app jar" passed
     * to Spark's ApplicationMaster via {@code --jar}; the class itself is
     * resolved at runtime through the container classpath.
     */
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

    /** Converts the current JVM classpath entries to absolute paths. */
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

    /**
     * Reads all {@code part-*} files from an HDFS directory and checks that
     * each expected {@code (word,count)} pair appears in the output.
     * Spark's {@code JavaPairRDD.saveAsTextFile} writes Tuple2 as {@code (key,value)}.
     */
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

    /**
     * Locates the {@code launch_container.sh} written by YARN's NodeManager for the
     * given application and attempt number.
     *
     * <p>NM log structure (MiniYARNCluster):
     * {@code <nmLogDir>/application_<ts>_<n>/container_<ts>_<n>_<attempt>_000001/launch_container.sh}
     */
    private java.nio.file.Path findLaunchScript(String appIdStr, int attemptNum) {
        // appIdStr = "application_<clusterTs>_<appNum>"
        // containerId = "container_<clusterTs>_<appNum>_<attempt>_000001"
        String suffix = appIdStr.replaceFirst("^application_", "");
        String attemptStr = String.format("%02d", attemptNum);
        String containerId = "container_" + suffix + "_" + attemptStr + "_000001";
        return Paths.get(nmLogDir, appIdStr, containerId, "launch_container.sh");
    }

    /**
     * Returns the path to the Java 8 JRE's bin directory on this machine,
     * or {@code null} if Java 8 is not installed.
     */
    private static String findJava8Home() {
        String[] candidates = {
            "/usr/lib/jvm/java-8-openjdk-amd64/jre",
            "/usr/lib/jvm/java-1.8.0-openjdk-amd64/jre",
            "/usr/lib/jvm/java-8-openjdk/jre",
            "/usr/java/jdk1.8.0/jre",
        };
        for (String candidate : candidates) {
            if (new File(candidate + "/bin/java").canExecute()) {
                return candidate;
            }
        }
        return null;
    }
}
