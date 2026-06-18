package com.github.sparktools.yarnclient;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Replicates the YARN cluster submission logic of {@code spark-submit --master yarn --deploy-mode cluster}
 * using only the Hadoop YARN client API — no Spark installation required on the submitting machine.
 *
 * <h3>What this class does (mirroring Spark's {@code Client.scala})</h3>
 * <ol>
 *   <li>Creates a per-submission staging directory in HDFS:
 *       {@code <fs.defaultFS>/.sparkStaging/<appId>/}</li>
 *   <li>Builds and uploads {@code __spark_conf__.zip} containing the serialised
 *       {@link Properties} file that the Spark ApplicationMaster reads on startup.</li>
 *   <li>Resolves Spark distribution jars from {@code spark.yarn.jars} (HDFS glob) or
 *       {@code spark.yarn.archive} (single archive) and registers them as YARN
 *       {@link LocalResource}s so the NodeManager downloads them into the container.</li>
 *   <li>Constructs the YARN container classpath and the AM launch command.</li>
 *   <li>Submits the application via {@link YarnClient#submitApplication}.</li>
 * </ol>
 *
 * <h3>Prerequisites</h3>
 * The Spark distribution jars must be pre-staged on HDFS.  Set one of:
 * <ul>
 *   <li>{@code spark.yarn.jars=hdfs:///spark/jars/*.jar} — glob of individual jars</li>
 *   <li>{@code spark.yarn.archive=hdfs:///spark/spark-libs.zip} — single archive extracted
 *       to {@code __spark_libs__/} inside the container</li>
 * </ul>
 */
class SparkYarnSubmitter {

    private static final Logger log = LoggerFactory.getLogger(SparkYarnSubmitter.class);

    static final String CONF_ARCHIVE_KEY = "__spark_conf__";
    static final String SPARK_LIBS_KEY   = "__spark_libs__";
    static final String PROPS_FILENAME   = "__spark_conf__.properties";

    private final SparkYarnConfig config;
    private final FileSystem hdfs;
    private final YarnClient yarnClient;

    SparkYarnSubmitter(SparkYarnConfig config, FileSystem hdfs, YarnClient yarnClient) {
        this.config = config;
        this.hdfs   = hdfs;
        this.yarnClient = yarnClient;
    }

    // -------------------------------------------------------------------------
    // Entry point
    // -------------------------------------------------------------------------

    ApplicationId submit(SparkJobConfig job, String hdfsJarUri) throws Exception {
        YarnClientApplication app = yarnClient.createApplication();
        ApplicationId appId = app.getNewApplicationResponse().getApplicationId();
        log.info("Created YARN application {}", appId);

        Path stagingDir = new Path(
            config.getHadoopConf().get("fs.defaultFS", config.getHdfsUri())
            + "/.sparkStaging/" + appId);
        hdfs.mkdirs(stagingDir);
        log.debug("Staging directory: {}", stagingDir);

        // Effective Spark properties — AM reads these from the conf archive
        Properties sparkProps = buildSparkProperties(job, hdfsJarUri, stagingDir);

        // YARN local resources that the NM downloads into the container
        Map<String, LocalResource> localResources = new LinkedHashMap<>();

        // Keytab must be distributed before conf archive upload — the archive
        // must contain the container-relative keytab path, not the local one.
        if (config.isKerberosEnabled()) {
            distributeKeytab(stagingDir, sparkProps, localResources);
        }

        Path confArchive = uploadConfArchive(stagingDir, sparkProps);
        localResources.put(CONF_ARCHIVE_KEY, archiveResource(confArchive));

        // Spark distribution jars → also determines extra CLASSPATH entries
        List<String> sparkLibCp = resolveSparkLibs(localResources, sparkProps);

        Map<String, String> env = buildContainerEnv(sparkLibCp, stagingDir);
        List<String> command    = buildAmCommand(job, hdfsJarUri);

        ByteBuffer tokens = config.isKerberosEnabled()
            ? obtainDelegationTokens() : null;

        ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(
            localResources, env, command, null, tokens, null);

        ApplicationSubmissionContext ctx = app.getApplicationSubmissionContext();
        ctx.setApplicationName(job.getAppName());
        ctx.setApplicationType("SPARK");
        ctx.setQueue(job.getQueue());
        ctx.setAMContainerSpec(amContainer);
        ctx.setResource(Resource.newInstance(
            MemoryParser.toMb(job.getDriverMemory()), job.getDriverCores()));
        ctx.setMaxAppAttempts(2);

        yarnClient.submitApplication(ctx);
        log.info("Submitted application '{}' as {}", job.getAppName(), appId);
        return appId;
    }

    // -------------------------------------------------------------------------
    // Spark properties file (read by ApplicationMaster via --properties-file)
    // -------------------------------------------------------------------------

    // Package-private for testing (verifying Kerberos property forwarding without a full cluster)
    Properties buildSparkProperties(
            SparkJobConfig job, String hdfsJarUri, Path stagingDir) {

        Properties p = new Properties();

        // Core identity
        p.setProperty("spark.master",            "yarn");
        p.setProperty("spark.submit.deployMode", job.getDeployMode());
        p.setProperty("spark.app.name",          job.getAppName());

        // Resources
        p.setProperty("spark.executor.instances", String.valueOf(job.getNumExecutors()));
        p.setProperty("spark.executor.memory",    job.getExecutorMemory());
        p.setProperty("spark.executor.cores",     String.valueOf(job.getExecutorCores()));
        p.setProperty("spark.driver.memory",      job.getDriverMemory());
        p.setProperty("spark.driver.cores",       String.valueOf(job.getDriverCores()));

        // YARN queue & staging
        p.setProperty("spark.yarn.queue",      job.getQueue());
        p.setProperty("spark.yarn.stagingDir", stagingDir.getParent().toUri().toString());

        // Forward HDFS / YARN connectivity so the AM finds the same cluster
        forwardHadoopConf(p);

        // On Java 9+, prepend module opens to executor JVM options so executor containers
        // can also access internal JDK APIs that Spark's storage/network layer requires.
        if (config.isAddJava9ModuleOpens()) {
            String opens = String.join(" ", JAVA9_MODULE_OPENS);
            p.setProperty("spark.executor.extraJavaOptions", opens);
        }

        // Cluster-level extras first, then job-level extras (job wins on conflicts)
        config.getExtraSparkConf().forEach(p::setProperty);
        job.getSparkConf().forEach(p::setProperty);

        // Kerberos
        if (config.isKerberosEnabled()) {
            p.setProperty("spark.kerberos.principal", config.getKerberosPrincipal());
            p.setProperty("spark.kerberos.keytab",    config.getKerberosKeytab());
        }

        return p;
    }

    /** Forward connectivity-relevant Hadoop conf keys as {@code spark.hadoop.*}. */
    private void forwardHadoopConf(Properties p) {
        String[] keysToForward = {
            "fs.defaultFS",
            "yarn.resourcemanager.address",
            "yarn.resourcemanager.hostname",
            "yarn.resourcemanager.ha.enabled",
            "yarn.resourcemanager.cluster-id",
            "dfs.nameservices",
        };
        for (String key : keysToForward) {
            String value = config.getHadoopConf().get(key);
            if (value != null) {
                p.setProperty("spark.hadoop." + key, value);
            }
        }
        // HA nameservice configs (dfs.ha.*, dfs.client.*)
        config.getHadoopConf().forEach(entry -> {
            String k = entry.getKey();
            if (k.startsWith("dfs.ha.") || k.startsWith("dfs.client.")
                    || k.startsWith("yarn.resourcemanager.ha.")) {
                p.setProperty("spark.hadoop." + k, entry.getValue());
            }
        });
    }

    // -------------------------------------------------------------------------
    // Conf archive (__spark_conf__.zip → localized to {{PWD}}/__spark_conf__/)
    // -------------------------------------------------------------------------

    private Path uploadConfArchive(Path stagingDir, Properties sparkProps) throws IOException {
        Path dest = new Path(stagingDir, CONF_ARCHIVE_KEY + ".zip");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(baos)) {
            // Spark properties — AM reads these via --properties-file
            zos.putNextEntry(new ZipEntry(PROPS_FILENAME));
            StringWriter sw = new StringWriter();
            sparkProps.store(sw, null);
            zos.write(sw.toString().getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();

            // Hadoop config as yarn-site.xml inside the archive.
            // After YARN extraction this lands at {{PWD}}/__spark_conf__/yarn-site.xml,
            // which is already on the container CLASSPATH. This lets new Configuration()
            // inside the AM pick up the YARN RM address, HDFS URI etc. without relying
            // on HADOOP_CONF_DIR — mirroring what Spark's Client.scala does.
            zos.putNextEntry(new ZipEntry("yarn-site.xml"));
            config.getHadoopConf().writeXml(zos);
            zos.closeEntry();
        }

        try (FSDataOutputStream out = hdfs.create(dest, true)) {
            out.write(baos.toByteArray());
        }
        log.debug("Uploaded conf archive to {}", dest);
        return dest;
    }

    // -------------------------------------------------------------------------
    // Spark lib jars (spark.yarn.jars or spark.yarn.archive)
    // -------------------------------------------------------------------------

    /**
     * Registers Spark distribution jars as LocalResources and returns the list
     * of classpath entries to add to the AM container's CLASSPATH.
     */
    private List<String> resolveSparkLibs(
            Map<String, LocalResource> localResources, Properties sparkProps) throws IOException {

        String archive = sparkProps.getProperty("spark.yarn.archive");
        String jars    = sparkProps.getProperty("spark.yarn.jars");

        if (archive != null) {
            return resolveFromArchive(localResources, archive);
        } else if (jars != null) {
            return resolveFromJarsGlob(localResources, jars);
        } else {
            log.warn("Neither spark.yarn.archive nor spark.yarn.jars is set. "
                + "The ApplicationMaster container may not have Spark classes on its classpath.");
            return Collections.emptyList();
        }
    }

    /** Single archive (zip/tar/tgz) → extracted by NM to {{PWD}}/__spark_libs__/ */
    private List<String> resolveFromArchive(
            Map<String, LocalResource> localResources, String archivePath) throws IOException {

        Path path = new Path(archivePath);
        FileStatus stat = hdfs.getFileStatus(path);
        localResources.put(SPARK_LIBS_KEY, buildResource(stat, LocalResourceType.ARCHIVE));
        log.debug("Registered Spark archive: {}", path);
        return Collections.singletonList(
            Environment.PWD.$$() + "/" + SPARK_LIBS_KEY + "/*");
    }

    /**
     * Comma-separated list of HDFS globs. Each matching jar is registered as a
     * LocalResource (key = filename) and added to the classpath.
     *
     * <p>Supports:
     * <ul>
     *   <li>{@code hdfs:///spark/jars/*.jar} — glob</li>
     *   <li>{@code local:/path/to/jar} — pre-installed on all nodes, only added to CLASSPATH</li>
     * </ul>
     */
    private List<String> resolveFromJarsGlob(
            Map<String, LocalResource> localResources, String jarsSpec) throws IOException {

        List<String> classpathEntries = new ArrayList<>();

        for (String entry : jarsSpec.split(",")) {
            entry = entry.trim();
            if (entry.isEmpty()) continue;

            if (entry.startsWith("local:")) {
                // Pre-installed on every node — no staging needed, just add to CP
                classpathEntries.add(entry.substring("local:".length()));
                continue;
            }

            // HDFS path (possibly a glob)
            FileStatus[] matches = hdfs.globStatus(new Path(entry));
            if (matches == null || matches.length == 0) {
                log.warn("spark.yarn.jars pattern matched no files: {}", entry);
                continue;
            }
            for (FileStatus stat : matches) {
                String key = stat.getPath().getName();
                // Guard against duplicate filenames from multiple globs
                if (!localResources.containsKey(key)) {
                    localResources.put(key, buildResource(stat, LocalResourceType.FILE));
                }
                classpathEntries.add(Environment.PWD.$$() + "/" + key);
            }
        }

        log.debug("Registered {} Spark jars as LocalResources", localResources.size() - 1);
        return classpathEntries;
    }

    // -------------------------------------------------------------------------
    // Container environment (CLASSPATH etc.)
    // -------------------------------------------------------------------------

    private Map<String, String> buildContainerEnv(List<String> sparkLibCp, Path stagingDir) {
        Map<String, String> env = new LinkedHashMap<>();
        env.put("SPARK_YARN_MODE", "true");
        // Required by Spark's ApplicationMaster to locate and clean up the staging directory
        env.put("SPARK_YARN_STAGING_DIR", stagingDir.toUri().toString());

        List<String> cp = new ArrayList<>();

        // Hadoop-provided classpath (Hadoop jars, config dir, etc.)
        String[] yarnCp = config.getHadoopConf().getStrings(
            YarnConfiguration.YARN_APPLICATION_CLASSPATH,
            YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH);
        cp.addAll(Arrays.asList(yarnCp));

        // Container working directory (catches any stray jars placed at root)
        cp.add(Environment.PWD.$$());
        // Extracted Spark conf archive
        cp.add(Environment.PWD.$$() + "/" + CONF_ARCHIVE_KEY);
        // Spark lib jars (from archive or individual files)
        cp.addAll(sparkLibCp);

        env.put("CLASSPATH", String.join(File.pathSeparator, cp));
        return env;
    }

    // -------------------------------------------------------------------------
    // ApplicationMaster launch command
    // -------------------------------------------------------------------------

    /**
     * Builds the shell command string that YARN uses to launch the AM container.
     *
     * <p>The command matches what Spark's {@code Client.scala} generates:
     * <pre>
     * $JAVA_HOME/bin/java -server -Xmx{driverMem}m
     *   -Djava.io.tmpdir={{PWD}}/tmp
     *   -Dspark.yarn.app.container.log.dir=<LOG_DIR>
     *   org.apache.spark.deploy.yarn.ApplicationMaster
     *   --class com.example.MyApp
     *   --jar hdfs://.../__spark_apps__/my-app.jar
     *   --properties-file {{PWD}}/__spark_conf__/__spark_conf__.properties
     *   [--arg arg1 --arg arg2 ...]
     *   1><LOG_DIR>/AppMaster.stdout 2><LOG_DIR>/AppMaster.stderr
     * </pre>
     */
    // --add-opens flags required by Spark on Java 9+. Mirror of Spark's extraJavaTestArgs
    // in the parent pom. Without these, internal JDK classes used by Spark's storage and
    // network layers throw IllegalAccessError at runtime.
    private static final String[] JAVA9_MODULE_OPENS = {
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
        "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
        "-Djdk.reflect.useDirectMethodHandle=false",
    };

    private List<String> buildAmCommand(SparkJobConfig job, String hdfsJarUri) {
        List<String> tokens = new ArrayList<>();

        // Java executable
        String javaExec = config.getJavaHome() != null
            ? config.getJavaHome() + "/bin/java"
            : Environment.JAVA_HOME.$$() + "/bin/java";
        tokens.add(javaExec);

        tokens.add("-server");
        tokens.add("-Xmx" + MemoryParser.toMb(job.getDriverMemory()) + "m");
        tokens.add("-Djava.io.tmpdir=" + Environment.PWD.$$() + "/tmp");
        tokens.add("-Dspark.yarn.app.container.log.dir="
            + ApplicationConstants.LOG_DIR_EXPANSION_VAR);

        // On Java 9+, Spark accesses internal JDK APIs that require explicit module opens.
        // Controlled by SparkYarnConfig.addJava9ModuleOpens (default true); set false
        // when targeting Java 8 clusters — Java 8 does not support these flags.
        if (config.isAddJava9ModuleOpens()) {
            Collections.addAll(tokens, JAVA9_MODULE_OPENS);
        }

        // Extra JVM opts from job config (e.g. GC flags, agent options)
        String extraOpts = effectiveSparkConf(job, "spark.driver.extraJavaOptions");
        if (extraOpts != null && !extraOpts.isEmpty()) {
            Collections.addAll(tokens, extraOpts.trim().split("\\s+"));
        }

        // ApplicationMaster main class (Spark's or test override)
        tokens.add(config.getAmClass());

        // AM arguments parsed by Spark's ApplicationMaster.parseArgs()
        tokens.add("--class"); tokens.add(job.getMainClass());
        tokens.add("--jar");   tokens.add(hdfsJarUri);
        tokens.add("--properties-file");
        tokens.add(Environment.PWD.$$() + "/" + CONF_ARCHIVE_KEY + "/" + PROPS_FILENAME);

        for (String arg : job.getAppArgs()) {
            tokens.add("--arg"); tokens.add(shellEscape(arg));
        }

        tokens.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
        tokens.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

        // YARN expects a single-element list where the element is the full command string
        String command = String.join(" ", tokens);
        log.debug("AM command: {}", command);
        return Collections.singletonList(command);
    }

    // -------------------------------------------------------------------------
    // Kerberos keytab distribution
    // -------------------------------------------------------------------------

    /**
     * Uploads the Kerberos keytab to the HDFS staging directory and registers it
     * as a {@link LocalResource} so YARN's NodeManager downloads it into the AM
     * container.  Rewrites {@code spark.kerberos.keytab} in {@code sparkProps}
     * to the localized filename (the file lands in the container's working directory).
     *
     * <p>Without this step the AM would reference a local filesystem path that
     * does not exist inside the container and would be unable to re-login from
     * the keytab for long-running jobs.
     */
    // Package-private for testing
    void distributeKeytab(Path stagingDir, Properties sparkProps,
            Map<String, LocalResource> localResources) throws IOException {
        String keytabPath = config.getKerberosKeytab();
        Path src = new Path(keytabPath);
        String keytabName = src.getName();
        Path dest = new Path(stagingDir, keytabName);

        hdfs.copyFromLocalFile(false, true, src, dest);
        localResources.put(keytabName,
                buildResource(hdfs.getFileStatus(dest), LocalResourceType.FILE));

        sparkProps.setProperty("spark.kerberos.keytab", keytabName);
        log.info("Distributed keytab to {} (localized as {})", dest, keytabName);
    }

    // -------------------------------------------------------------------------
    // Kerberos delegation tokens
    // -------------------------------------------------------------------------

    private ByteBuffer obtainDelegationTokens() throws Exception {
        Credentials creds = new Credentials();
        // HDFS delegation token
        hdfs.addDelegationTokens(config.getKerberosPrincipal(), creds);
        // RM delegation token
        yarnClient.getRMDelegationToken(
            new org.apache.hadoop.io.Text(config.getKerberosPrincipal()));

        DataOutputBuffer dob = new DataOutputBuffer();
        creds.writeTokenStorageToStream(dob);
        return ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    }

    // -------------------------------------------------------------------------
    // LocalResource helpers
    // -------------------------------------------------------------------------

    private LocalResource archiveResource(Path path) throws IOException {
        return buildResource(hdfs.getFileStatus(path), LocalResourceType.ARCHIVE);
    }

    private LocalResource buildResource(FileStatus stat, LocalResourceType type) {
        LocalResource r = Records.newRecord(LocalResource.class);
        r.setResource(URL.fromPath(stat.getPath()));
        r.setSize(stat.getLen());
        r.setTimestamp(stat.getModificationTime());
        r.setType(type);
        r.setVisibility(LocalResourceVisibility.APPLICATION);
        return r;
    }

    // -------------------------------------------------------------------------
    // Misc helpers
    // -------------------------------------------------------------------------

    /** Returns the effective value of a Spark conf key: job-level overrides cluster-level. */
    private String effectiveSparkConf(SparkJobConfig job, String key) {
        String v = job.getSparkConf().get(key);
        return v != null ? v : config.getExtraSparkConf().get(key);
    }

    /** Wraps an argument in single quotes if it contains spaces or shell meta-characters. */
    private static String shellEscape(String arg) {
        if (arg.matches("[\\w./:=-]+")) return arg;
        return "'" + arg.replace("'", "'\\''") + "'";
    }

    // -------------------------------------------------------------------------
    // Memory string parsing ("1g" → 1024, "512m" → 512)
    // -------------------------------------------------------------------------

    static final class MemoryParser {
        static int toMb(String mem) {
            if (mem == null) return 1024;
            String s = mem.trim().toLowerCase(Locale.ROOT);
            if (s.endsWith("g")) {
                return (int) (Double.parseDouble(s.substring(0, s.length() - 1)) * 1024);
            }
            if (s.endsWith("m")) {
                return Integer.parseInt(s.substring(0, s.length() - 1));
            }
            return Integer.parseInt(s); // bare number assumed to be MB
        }

        private MemoryParser() {}
    }
}
