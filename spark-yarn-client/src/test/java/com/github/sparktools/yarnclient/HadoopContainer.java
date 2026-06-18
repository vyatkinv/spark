package com.github.sparktools.yarnclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.containers.BindMode;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;

/**
 * Testcontainers wrapper for a pseudo-distributed Hadoop cluster (HDFS + YARN).
 *
 * <p>The container exposes HDFS NameNode (9000), DataNode (9866), and
 * YARN ResourceManager (8032) ports. Spark jars from the test classpath
 * are bind-mounted into the container at {@code /opt/spark-jars/} so that
 * YARN containers have Spark classes available.
 *
 * <p>Uses the container's bridge IP for client connections (works on Linux;
 * tests are skipped on platforms where container IPs are not routable).
 */
class HadoopContainer extends GenericContainer<HadoopContainer> {

    private static final Logger log = LoggerFactory.getLogger(HadoopContainer.class);

    static final int HDFS_PORT = 9000;
    static final int DN_DATA_PORT = 9866;
    static final int RM_PORT = 8032;
    static final int RM_SCHEDULER_PORT = 8030;
    static final int RM_TRACKER_PORT = 8031;
    static final int RM_WEB_PORT = 8088;

    private Path sparkJarsDir;

    HadoopContainer() {
        super(new ImageFromDockerfile("spark-yarn-client-hadoop-test", false)
                .withDockerfile(resolveDockerfile()));
        withExposedPorts(HDFS_PORT, DN_DATA_PORT, RM_PORT, RM_SCHEDULER_PORT,
                RM_TRACKER_PORT, RM_WEB_PORT);
        waitingFor(Wait.forLogMessage(".*READY.*\\n", 1)
                .withStartupTimeout(Duration.ofMinutes(3)));
    }

    @Override
    public void start() {
        try {
            sparkJarsDir = prepareSparkJarsDir();
            withFileSystemBind(sparkJarsDir.toString(), "/opt/spark-jars", BindMode.READ_ONLY);
        } catch (IOException e) {
            throw new RuntimeException("Failed to prepare Spark jars directory", e);
        }
        super.start();
    }

    String getContainerIp() {
        return getContainerInfo()
                .getNetworkSettings().getNetworks().values().iterator().next().getIpAddress();
    }

    String getHdfsUri() {
        return "hdfs://" + getContainerIp() + ":" + HDFS_PORT;
    }

    String getYarnRmAddress() {
        return getContainerIp() + ":" + RM_PORT;
    }

    /**
     * Returns a Hadoop {@link Configuration} pre-configured for this container.
     */
    Configuration getHadoopConf() {
        String ip = getContainerIp();
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", getHdfsUri());
        conf.setBoolean("dfs.permissions.enabled", false);
        conf.set("yarn.resourcemanager.address", ip + ":" + RM_PORT);
        conf.set("yarn.resourcemanager.scheduler.address", ip + ":" + RM_SCHEDULER_PORT);
        conf.set("yarn.resourcemanager.resource-tracker.address", ip + ":" + RM_TRACKER_PORT);
        conf.set("yarn.resourcemanager.webapp.address", ip + ":" + RM_WEB_PORT);
        conf.setInt(YarnConfiguration.NM_PMEM_MB, 4096);
        conf.setInt(YarnConfiguration.NM_VCORES, 4);
        conf.setBoolean(YarnConfiguration.NM_PMEM_CHECK_ENABLED, false);
        conf.setBoolean(YarnConfiguration.NM_VMEM_CHECK_ENABLED, false);
        conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 256);
        return conf;
    }

    /**
     * Collects JAR files from the test classpath into a temporary directory
     * using symlinks. This directory is bind-mounted into the container at
     * {@code /opt/spark-jars/} so YARN containers have all necessary classes.
     */
    static Path prepareSparkJarsDir() throws IOException {
        Path dir = Files.createTempDirectory("spark-jars-");
        String[] entries = System.getProperty("java.class.path", "").split(File.pathSeparator);
        int count = 0;
        for (String entry : entries) {
            Path p = Paths.get(entry).toAbsolutePath();
            if (Files.isRegularFile(p) && p.toString().endsWith(".jar")) {
                Path link = dir.resolve(p.getFileName());
                if (!Files.exists(link)) {
                    Files.createSymbolicLink(link, p);
                    count++;
                }
            } else if (Files.isDirectory(p)) {
                // For directory classpath entries (target/classes, target/test-classes),
                // create a JAR so it's usable inside the container.
                String dirName = p.getFileName().toString();
                String parentName = p.getParent() != null
                        ? p.getParent().getFileName().toString() : "dir";
                Path jarLink = dir.resolve(parentName + "-" + dirName + ".jar");
                if (!Files.exists(jarLink)) {
                    // Create a jar from the directory
                    Path jarFile = createJarFromDir(p, dir, parentName + "-" + dirName + ".jar");
                    if (jarFile != null) count++;
                }
            }
        }
        log.info("Prepared {} classpath entries in {}", count, dir);
        return dir;
    }

    private static Path createJarFromDir(Path sourceDir, Path targetDir, String jarName)
            throws IOException {
        Path jarPath = targetDir.resolve(jarName);
        java.util.jar.Manifest manifest = new java.util.jar.Manifest();
        manifest.getMainAttributes().putValue("Manifest-Version", "1.0");
        try (java.util.jar.JarOutputStream jos =
                     new java.util.jar.JarOutputStream(Files.newOutputStream(jarPath), manifest)) {
            Files.walk(sourceDir).filter(Files::isRegularFile).forEach(file -> {
                String entryName = sourceDir.relativize(file).toString().replace('\\', '/');
                try {
                    jos.putNextEntry(new java.util.zip.ZipEntry(entryName));
                    Files.copy(file, jos);
                    jos.closeEntry();
                } catch (IOException e) {
                    // skip files that can't be read
                }
            });
        }
        return jarPath;
    }

    private static Path resolveDockerfile() {
        // Try both possible locations (from module root or from repo root)
        Path[] candidates = {
            Paths.get("src/test/docker/hadoop/Dockerfile"),
            Paths.get("spark-yarn-client/src/test/docker/hadoop/Dockerfile"),
        };
        for (Path p : candidates) {
            if (Files.exists(p)) return p;
        }
        throw new IllegalStateException(
                "Cannot find Hadoop Dockerfile. Run from the spark-yarn-client module directory.");
    }
}
