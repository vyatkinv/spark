package com.github.sparktools.yarnclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
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
 *   <li>Creates a per-submission staging directory in HDFS with {@code 700} permissions.</li>
 *   <li>Builds and uploads {@code __spark_conf__.zip} containing the serialised
 *       {@link Properties} file, Hadoop XML configs from {@code HADOOP_CONF_DIR} /
 *       {@code YARN_CONF_DIR}, and distributed cache configuration.</li>
 *   <li>Resolves Spark distribution jars from {@code spark.yarn.jars} (HDFS glob) or
 *       {@code spark.yarn.archive} (single archive) and registers them as YARN
 *       {@link LocalResource}s so the NodeManager downloads them into the container.</li>
 *   <li>Distributes user-specified files, archives, and jars via {@code spark.yarn.dist.*}.</li>
 *   <li>Obtains delegation tokens for HDFS, YARN RM, and any configured services.</li>
 *   <li>Constructs the YARN container classpath (including {@code mapreduce.application.classpath}),
 *       environment, and the AM launch command.</li>
 *   <li>Sets memory overhead, ACLs, and submits via {@link YarnClient#submitApplication}.</li>
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
    static final String HADOOP_CONF_DIR  = "__hadoop_conf__";
    static final String DIST_CACHE_CONF  = "__spark_dist_cache__.properties";

    static final FsPermission STAGING_DIR_PERMISSION =
            FsPermission.createImmutable((short) 0700);
    static final FsPermission APP_FILE_PERMISSION =
            FsPermission.createImmutable((short) 0644);

    static final int MEMORY_OVERHEAD_MIN_MB = 384;
    static final double MEMORY_OVERHEAD_FACTOR = 0.10;

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
        FileSystem.mkdirs(hdfs, stagingDir, STAGING_DIR_PERMISSION);
        log.debug("Staging directory: {}", stagingDir);

        try {
            return doSubmit(app, appId, job, hdfsJarUri, stagingDir);
        } catch (Exception e) {
            cleanupStagingDir(stagingDir);
            throw e;
        }
    }

    private ApplicationId doSubmit(YarnClientApplication app, ApplicationId appId,
            SparkJobConfig job, String hdfsJarUri, Path stagingDir) throws Exception {

        Properties sparkProps = buildSparkProperties(job, hdfsJarUri, stagingDir);

        Map<String, LocalResource> localResources = new LinkedHashMap<>();

        // Keytab must be distributed before conf archive upload — the archive
        // must contain the container-relative keytab path, not the local one.
        if (config.isKerberosEnabled()) {
            distributeKeytab(stagingDir, sparkProps, localResources);
        }

        // Distribute user-specified files, archives, and jars
        Properties distCacheProps = distributeUserResources(
                job, stagingDir, sparkProps, localResources);

        Path confArchive = uploadConfArchive(stagingDir, sparkProps, distCacheProps);
        localResources.put(CONF_ARCHIVE_KEY, archiveResource(confArchive));

        List<String> sparkLibCp = resolveSparkLibs(localResources, sparkProps);

        Map<String, String> env = buildContainerEnv(sparkLibCp, stagingDir);
        List<String> command    = buildAmCommand(job, hdfsJarUri);

        ByteBuffer tokens = config.isKerberosEnabled()
            ? obtainDelegationTokens() : null;

        ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(
            localResources, env, command, null, tokens, null);

        // ACLs: view and modify permissions
        Map<ApplicationAccessType, String> acls = new HashMap<>();
        String currentUser = UserGroupInformation.getCurrentUser().getShortUserName();
        acls.put(ApplicationAccessType.VIEW_APP, currentUser);
        acls.put(ApplicationAccessType.MODIFY_APP, currentUser);
        amContainer.setApplicationACLs(acls);

        // Memory overhead: max(driverMemory * factor, 384MB)
        int driverMb = MemoryParser.toMb(job.getDriverMemory());
        int overhead = Math.max((int) (driverMb * MEMORY_OVERHEAD_FACTOR), MEMORY_OVERHEAD_MIN_MB);
        int totalAmMemory = driverMb + overhead;
        log.debug("AM memory: {}MB (driver) + {}MB (overhead) = {}MB",
                driverMb, overhead, totalAmMemory);

        ApplicationSubmissionContext ctx = app.getApplicationSubmissionContext();
        ctx.setApplicationName(job.getAppName());
        ctx.setApplicationType("SPARK");
        ctx.setQueue(job.getQueue());
        ctx.setAMContainerSpec(amContainer);
        ctx.setResource(Resource.newInstance(totalAmMemory, job.getDriverCores()));
        ctx.setMaxAppAttempts(2);

        yarnClient.submitApplication(ctx);
        log.info("Submitted application '{}' as {}", job.getAppName(), appId);
        return appId;
    }

    // -------------------------------------------------------------------------
    // Staging directory cleanup
    // -------------------------------------------------------------------------

    private void cleanupStagingDir(Path stagingDir) {
        try {
            if (hdfs.delete(stagingDir, true)) {
                log.info("Cleaned up staging directory {}", stagingDir);
            }
        } catch (IOException e) {
            log.warn("Failed to clean up staging directory {}", stagingDir, e);
        }
    }

    // -------------------------------------------------------------------------
    // Spark properties file (read by ApplicationMaster via --properties-file)
    // -------------------------------------------------------------------------

    // Package-private for testing
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
    // User resource distribution (--files, --archives, --jars)
    // -------------------------------------------------------------------------

    /**
     * Distributes user-specified files, archives, and jars to the HDFS staging
     * directory, registers them as {@link LocalResource}s, and sets the
     * corresponding {@code spark.yarn.dist.*} properties so the AM can
     * propagate them to executor containers.
     *
     * @return distributed cache properties for inclusion in the conf archive
     */
    private Properties distributeUserResources(SparkJobConfig job, Path stagingDir,
            Properties sparkProps, Map<String, LocalResource> localResources) throws IOException {

        Properties distCacheProps = new Properties();
        List<String> distFiles = new ArrayList<>();
        List<String> distArchives = new ArrayList<>();
        List<String> distJars = new ArrayList<>();

        for (String filePath : job.getFiles()) {
            Path uploaded = stageFile(stagingDir, filePath);
            String name = uploaded.getName();
            localResources.put(name,
                    buildResource(hdfs.getFileStatus(uploaded), LocalResourceType.FILE));
            distFiles.add(uploaded.toUri().toString());
        }

        for (String archivePath : job.getArchives()) {
            Path uploaded = stageFile(stagingDir, archivePath);
            String name = uploaded.getName();
            localResources.put(name,
                    buildResource(hdfs.getFileStatus(uploaded), LocalResourceType.ARCHIVE));
            distArchives.add(uploaded.toUri().toString());
        }

        for (String jarPath : job.getJars()) {
            Path uploaded = stageFile(stagingDir, jarPath);
            String name = uploaded.getName();
            localResources.put(name,
                    buildResource(hdfs.getFileStatus(uploaded), LocalResourceType.FILE));
            distJars.add(uploaded.toUri().toString());
        }

        if (!distFiles.isEmpty()) {
            sparkProps.setProperty("spark.yarn.dist.files", String.join(",", distFiles));
        }
        if (!distArchives.isEmpty()) {
            sparkProps.setProperty("spark.yarn.dist.archives", String.join(",", distArchives));
        }
        if (!distJars.isEmpty()) {
            sparkProps.setProperty("spark.yarn.dist.jars", String.join(",", distJars));
        }

        // Build dist cache properties that the AM reads via --dist-cache-conf
        int idx = 0;
        for (String uri : distFiles) {
            distCacheProps.setProperty("spark.yarn.cache.filenames." + idx, uri);
            distCacheProps.setProperty("spark.yarn.cache.types." + idx, "FILE");
            distCacheProps.setProperty("spark.yarn.cache.visibilities." + idx, "APPLICATION");
            idx++;
        }
        for (String uri : distArchives) {
            distCacheProps.setProperty("spark.yarn.cache.filenames." + idx, uri);
            distCacheProps.setProperty("spark.yarn.cache.types." + idx, "ARCHIVE");
            distCacheProps.setProperty("spark.yarn.cache.visibilities." + idx, "APPLICATION");
            idx++;
        }
        for (String uri : distJars) {
            distCacheProps.setProperty("spark.yarn.cache.filenames." + idx, uri);
            distCacheProps.setProperty("spark.yarn.cache.types." + idx, "FILE");
            distCacheProps.setProperty("spark.yarn.cache.visibilities." + idx, "APPLICATION");
            idx++;
        }
        if (idx > 0) {
            distCacheProps.setProperty("spark.yarn.cache.size", String.valueOf(idx));
        }

        return distCacheProps;
    }

    /**
     * Stages a file to the HDFS staging directory. If the path is already on HDFS,
     * it is used as-is. Local paths are uploaded.
     */
    private Path stageFile(Path stagingDir, String filePath) throws IOException {
        Path src = new Path(filePath);
        String scheme = src.toUri().getScheme();
        if (scheme != null && (scheme.equals("hdfs") || scheme.equals("s3a")
                || scheme.equals("gs") || scheme.equals("wasbs"))) {
            return src;
        }
        Path dest = new Path(stagingDir, src.getName());
        hdfs.copyFromLocalFile(false, true, src, dest);
        hdfs.setPermission(dest, APP_FILE_PERMISSION);
        log.debug("Staged {} → {}", filePath, dest);
        return dest;
    }

    // -------------------------------------------------------------------------
    // Conf archive (__spark_conf__.zip → localized to {{PWD}}/__spark_conf__/)
    // -------------------------------------------------------------------------

    private Path uploadConfArchive(Path stagingDir, Properties sparkProps,
            Properties distCacheProps) throws IOException {
        Path dest = new Path(stagingDir, CONF_ARCHIVE_KEY + ".zip");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(baos)) {
            zos.setLevel(0);

            // Spark properties — AM reads these via --properties-file
            zos.putNextEntry(new ZipEntry(PROPS_FILENAME));
            StringWriter sw = new StringWriter();
            sparkProps.store(sw, null);
            zos.write(sw.toString().getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();

            // Distributed cache config — AM reads via --dist-cache-conf
            if (!distCacheProps.isEmpty()) {
                zos.putNextEntry(new ZipEntry(DIST_CACHE_CONF));
                StringWriter dw = new StringWriter();
                distCacheProps.store(dw, null);
                zos.write(dw.toString().getBytes(StandardCharsets.UTF_8));
                zos.closeEntry();
            }

            // Hadoop config files from HADOOP_CONF_DIR / YARN_CONF_DIR
            zos.putNextEntry(new ZipEntry(HADOOP_CONF_DIR + "/"));
            zos.closeEntry();
            addHadoopConfFiles(zos);

            // Programmatic Hadoop config as __spark_hadoop_conf__.xml
            zos.putNextEntry(new ZipEntry("__spark_hadoop_conf__.xml"));
            config.getHadoopConf().writeXml(zos);
            zos.closeEntry();
        }

        try (FSDataOutputStream out = hdfs.create(dest, true)) {
            out.write(baos.toByteArray());
        }
        hdfs.setPermission(dest, APP_FILE_PERMISSION);
        log.debug("Uploaded conf archive to {}", dest);
        return dest;
    }

    /**
     * Adds XML config files from {@code HADOOP_CONF_DIR} and {@code YARN_CONF_DIR}
     * into the {@code __hadoop_conf__/} subdirectory of the conf archive.
     */
    private void addHadoopConfFiles(ZipOutputStream zos) throws IOException {
        Map<String, File> confFiles = new LinkedHashMap<>();

        for (String envKey : new String[]{"HADOOP_CONF_DIR", "YARN_CONF_DIR"}) {
            String dir = System.getenv(envKey);
            if (dir == null) continue;
            File dirFile = new File(dir);
            if (!dirFile.isDirectory()) continue;

            File[] files = dirFile.listFiles();
            if (files == null) continue;
            for (File f : files) {
                if (f.isFile() && !confFiles.containsKey(f.getName())) {
                    confFiles.put(f.getName(), f);
                }
            }
        }

        for (Map.Entry<String, File> entry : confFiles.entrySet()) {
            File f = entry.getValue();
            if (!f.canRead()) continue;
            zos.putNextEntry(new ZipEntry(HADOOP_CONF_DIR + "/" + entry.getKey()));
            java.nio.file.Files.copy(f.toPath(), zos);
            zos.closeEntry();
        }

        if (!confFiles.isEmpty()) {
            log.debug("Added {} Hadoop config files from HADOOP_CONF_DIR/YARN_CONF_DIR",
                    confFiles.size());
        }
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
                classpathEntries.add(entry.substring("local:".length()));
                continue;
            }

            FileStatus[] matches = hdfs.globStatus(new Path(entry));
            if (matches == null || matches.length == 0) {
                log.warn("spark.yarn.jars pattern matched no files: {}", entry);
                continue;
            }
            for (FileStatus stat : matches) {
                String key = stat.getPath().getName();
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
    // Container environment (CLASSPATH, SPARK_USER, etc.)
    // -------------------------------------------------------------------------

    private Map<String, String> buildContainerEnv(List<String> sparkLibCp, Path stagingDir) {
        Map<String, String> env = new LinkedHashMap<>();
        env.put("SPARK_YARN_MODE", "true");
        env.put("SPARK_YARN_STAGING_DIR", stagingDir.toUri().toString());

        // SPARK_USER — used by Spark internals for security context and web UI ACLs
        try {
            env.put("SPARK_USER", UserGroupInformation.getCurrentUser().getShortUserName());
        } catch (IOException e) {
            log.debug("Could not determine current user for SPARK_USER", e);
        }

        List<String> cp = new ArrayList<>();

        // Hadoop-provided classpath (Hadoop jars, config dir, etc.)
        String[] yarnCp = config.getHadoopConf().getStrings(
            YarnConfiguration.YARN_APPLICATION_CLASSPATH,
            YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH);
        cp.addAll(Arrays.asList(yarnCp));

        // MapReduce application classpath (some distros put essential jars here)
        String[] mrCp = config.getHadoopConf().getStrings(
            MRJobConfig.MAPREDUCE_APPLICATION_CLASSPATH,
            MRJobConfig.DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH);
        if (mrCp != null) {
            cp.addAll(Arrays.asList(mrCp));
        }

        // Container working directory
        cp.add(Environment.PWD.$$());
        // Extracted Spark conf archive
        cp.add(Environment.PWD.$$() + "/" + CONF_ARCHIVE_KEY);
        // Hadoop conf subdirectory inside the conf archive
        cp.add(Environment.PWD.$$() + "/" + CONF_ARCHIVE_KEY + "/" + HADOOP_CONF_DIR);
        // Spark lib jars (from archive or individual files)
        cp.addAll(sparkLibCp);

        env.put("CLASSPATH", String.join(File.pathSeparator, cp));
        return env;
    }

    // -------------------------------------------------------------------------
    // ApplicationMaster launch command
    // -------------------------------------------------------------------------

    // --add-opens flags required by Spark on Java 9+
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

        String javaExec = config.getJavaHome() != null
            ? config.getJavaHome() + "/bin/java"
            : Environment.JAVA_HOME.$$() + "/bin/java";
        tokens.add(javaExec);

        tokens.add("-server");
        tokens.add("-Xmx" + MemoryParser.toMb(job.getDriverMemory()) + "m");
        tokens.add("-Djava.io.tmpdir=" + Environment.PWD.$$() + "/tmp");
        tokens.add("-Dspark.yarn.app.container.log.dir="
            + ApplicationConstants.LOG_DIR_EXPANSION_VAR);

        if (config.isAddJava9ModuleOpens()) {
            Collections.addAll(tokens, JAVA9_MODULE_OPENS);
        }

        String extraOpts = effectiveSparkConf(job, "spark.driver.extraJavaOptions");
        if (extraOpts != null && !extraOpts.isEmpty()) {
            Collections.addAll(tokens, extraOpts.trim().split("\\s+"));
        }

        tokens.add(config.getAmClass());

        tokens.add("--class"); tokens.add(job.getMainClass());
        tokens.add("--jar");   tokens.add(hdfsJarUri);
        tokens.add("--properties-file");
        tokens.add(Environment.PWD.$$() + "/" + CONF_ARCHIVE_KEY + "/" + PROPS_FILENAME);
        tokens.add("--dist-cache-conf");
        tokens.add(Environment.PWD.$$() + "/" + CONF_ARCHIVE_KEY + "/" + DIST_CACHE_CONF);

        for (String arg : job.getAppArgs()) {
            tokens.add("--arg"); tokens.add(shellEscape(arg));
        }

        tokens.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
        tokens.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

        String command = String.join(" ", tokens);
        log.debug("AM command: {}", command);
        return Collections.singletonList(command);
    }

    // -------------------------------------------------------------------------
    // Kerberos keytab distribution
    // -------------------------------------------------------------------------

    // Package-private for testing
    void distributeKeytab(Path stagingDir, Properties sparkProps,
            Map<String, LocalResource> localResources) throws IOException {
        String keytabPath = config.getKerberosKeytab();
        Path src = new Path(keytabPath);
        String keytabName = src.getName();
        Path dest = new Path(stagingDir, keytabName);

        hdfs.copyFromLocalFile(false, true, src, dest);
        hdfs.setPermission(dest, APP_FILE_PERMISSION);
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

        // RM delegation token — YarnClient returns a YARN Token that must be
        // converted to a Hadoop security Token before adding to Credentials
        org.apache.hadoop.yarn.api.records.Token rmYarnToken =
                yarnClient.getRMDelegationToken(new Text(config.getKerberosPrincipal()));
        if (rmYarnToken != null) {
            org.apache.hadoop.security.token.Token<? extends
                    org.apache.hadoop.security.token.TokenIdentifier> rmToken =
                    org.apache.hadoop.yarn.util.ConverterUtils.convertFromYarn(
                            rmYarnToken, new Text(rmYarnToken.getService()));
            creds.addToken(rmToken.getService(), rmToken);
            log.debug("Obtained RM delegation token: {}", rmToken.getService());
        }

        // Tokens from the current user's credentials (e.g. Hive, HBase tokens
        // that were obtained during login or via external tooling)
        Credentials userCreds = UserGroupInformation.getCurrentUser().getCredentials();
        creds.addAll(userCreds);

        log.info("Obtained {} delegation tokens", creds.numberOfTokens());

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

    private String effectiveSparkConf(SparkJobConfig job, String key) {
        String v = job.getSparkConf().get(key);
        return v != null ? v : config.getExtraSparkConf().get(key);
    }

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
            return Integer.parseInt(s);
        }

        private MemoryParser() {}
    }
}
