package com.github.sparktools.yarnclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.jupiter.api.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * True fat-JAR integration test: the application jar contains ALL classes
 * (Spark, Hadoop, Scala, user code). No external jars on the YARN nodes.
 *
 * <p>Prerequisites:
 * <ol>
 *   <li>Build the fat test jar:
 *       {@code mvn package -pl spark-yarn-client -Pfat-jar-test -DskipTests}</li>
 *   <li>Build the Hadoop Docker image (if not yet):
 *       {@code docker build -t spark-yarn-hadoop-test spark-yarn-client/src/test/docker/hadoop/}</li>
 *   <li>Start a bare Hadoop container (no spark-jars bind mount!):
 *       {@code docker run -d --name hadoop-test spark-yarn-hadoop-test:latest}</li>
 *   <li>Get IP: {@code docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' hadoop-test}</li>
 * </ol>
 *
 * <p>Run: {@code mvn test -pl spark-yarn-client -Dtest=ManualDockerFatJarTest -DHADOOP_TEST_IP=172.17.0.2}
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ManualDockerFatJarTest {

    private static String containerIp;
    private static String hdfsUri;
    private static Configuration conf;
    private static String fatJarPath;

    @BeforeAll
    static void setup() throws Exception {
        containerIp = System.getProperty("HADOOP_TEST_IP",
                System.getenv("HADOOP_TEST_IP"));
        assumeTrue(containerIp != null && !containerIp.isEmpty(),
                "Skipped: HADOOP_TEST_IP not set");

        fatJarPath = findFatJar();
        assumeTrue(fatJarPath != null,
                "Skipped: fat-test jar not found. Run: mvn package -Pfat-jar-test -DskipTests");

        hdfsUri = "hdfs://" + containerIp + ":9000";
        conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUri);
        conf.setBoolean("dfs.permissions.enabled", false);
        conf.set("yarn.resourcemanager.address", containerIp + ":8032");
        conf.set("yarn.resourcemanager.scheduler.address", containerIp + ":8030");
        conf.set("yarn.resourcemanager.resource-tracker.address", containerIp + ":8031");
        conf.set("yarn.resourcemanager.webapp.address", containerIp + ":8088");
        conf.setInt(YarnConfiguration.NM_PMEM_MB, 4096);
        conf.setInt(YarnConfiguration.NM_VCORES, 4);
        conf.setBoolean(YarnConfiguration.NM_PMEM_CHECK_ENABLED, false);
        conf.setBoolean(YarnConfiguration.NM_VMEM_CHECK_ENABLED, false);
        conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 256);

        FileSystem fs = FileSystem.newInstance(URI.create(hdfsUri), conf);
        try {
            Path inputPath = new Path("/test-input/words.txt");
            fs.mkdirs(inputPath.getParent());
            try (FSDataOutputStream out = fs.create(inputPath, true)) {
                out.write(("hello world hello spark\n"
                         + "world spark spark\n")
                        .getBytes(StandardCharsets.UTF_8));
            }
        } finally {
            fs.close();
        }
    }

    @Test
    @Order(1)
    @Timeout(300)
    void fatJarMode_countsWordsWithExecutors() throws Exception {
        String inputHdfsPath = hdfsUri + "/test-input/words.txt";
        String outputHdfsPath = hdfsUri + "/test-output/fatjar-" + System.currentTimeMillis();

        SparkYarnConfig fatJarConfig = SparkYarnConfig.builder()
                .hdfsUri(hdfsUri)
                .hdfsJarUploadDir("/spark-apps/jars")
                .hadoopConf(conf)
                .sparkConf("spark.shuffle.service.enabled", "false")
                .sparkConf("spark.dynamicAllocation.enabled", "false")
                .build();

        try (SparkYarnClient client = new SparkYarnClient(fatJarConfig)) {
            SparkJobConfig job = SparkJobConfig.builder()
                    .appName("FatJar-Test-" + System.currentTimeMillis())
                    .mainClass(SimpleSparkApp.class.getName())
                    .localJarPath(fatJarPath)
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
            assertNotNull(app.getApplicationId());

            System.out.println("Submitted application: " + app.getApplicationId());
            ApplicationInfo result = app.waitForTermination(270_000);
            System.out.println("Final state: " + result.getState()
                    + ", status: " + result.getFinalStatus());
            if (result.getDiagnostics() != null && !result.getDiagnostics().isEmpty()) {
                System.out.println("Diagnostics: " + result.getDiagnostics());
            }

            assertEquals(YarnApplicationState.FINISHED, result.getState(),
                    "Diagnostics: " + result.getDiagnostics());
            assertEquals(FinalApplicationStatus.SUCCEEDED, result.getFinalStatus(),
                    "Diagnostics: " + result.getDiagnostics());

            Map<String, Integer> expected = new LinkedHashMap<>();
            expected.put("hello", 2);
            expected.put("world", 2);
            expected.put("spark", 3);
            verifyWordCountOutput(outputHdfsPath, expected);
        }
    }

    private static String findFatJar() {
        // Look in the sibling spark-yarn-client-test-app module's target
        String[] candidates = {
            "../spark-yarn-client-test-app/target",
            "spark-yarn-client-test-app/target",
        };
        for (String dir : candidates) {
            File target = new File(dir);
            if (!target.isDirectory()) continue;
            File[] files = target.listFiles((d, name) ->
                    name.startsWith("spark-yarn-client-test-app")
                    && name.endsWith(".jar")
                    && !name.contains("original")
                    && !name.contains("sources")
                    && !name.contains("tests")
                    && !name.contains("test-sources"));
            if (files != null && files.length > 0) return files[0].getAbsolutePath();
        }
        return null;
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
}
