package com.github.sparktools.yarnclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Lightweight client for submitting Spark jobs to YARN.
 *
 * <p>No Spark installation is required on the machine that calls {@link #submit} —
 * all submission logic is handled directly via the Hadoop YARN client API, replicating
 * what {@code spark-submit --master yarn --deploy-mode cluster} does internally.
 *
 * <p>Typical usage:
 * <pre>{@code
 * // Minimal — reads fs.defaultFS and YARN RM from HADOOP_CONF_DIR:
 * SparkYarnConfig config = SparkYarnConfig.builder().build();
 *
 * SparkJobConfig job = SparkJobConfig.builder()
 *     .appName("MyJob")
 *     .mainClass("com.example.MyApp")
 *     .localJarPath("/home/user/my-app-all.jar")
 *     .numExecutors(4)
 *     .executorMemory("4g")
 *     .addArg("--date").addArg("2026-06-12")
 *     .build();
 *
 * try (SparkYarnClient client = new SparkYarnClient(config)) {
 *     SubmittedApplication app = client.submit(job);
 *     ApplicationInfo result = app.waitForTermination(60 * 60_000L);
 *     System.out.println(result);
 * }
 * }</pre>
 */
public class SparkYarnClient implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(SparkYarnClient.class);

    private final SparkYarnConfig config;
    private final FileSystem hdfs;
    private final YarnClient yarnClient;
    private final SparkYarnSubmitter submitter;

    public SparkYarnClient(SparkYarnConfig config) throws IOException {
        this.config = config;

        if (config.isKerberosEnabled()) {
            KerberosSupport.login(
                config.getHadoopConf(),
                config.getKerberosPrincipal(),
                config.getKerberosKeytab());
        }

        Configuration hadoopConf = config.getHadoopConf();
        if (config.getHdfsUri() != null) {
            hadoopConf.set("fs.defaultFS", config.getHdfsUri());
        }
        // newInstance() creates an independent (non-cached) FileSystem so that
        // closing this client does not affect other FileSystem users in the JVM.
        this.hdfs = FileSystem.newInstance(hadoopConf);

        this.yarnClient = YarnClient.createYarnClient();
        yarnClient.init(config.getHadoopConf());
        yarnClient.start();
        log.info("YARN client started");

        this.submitter = new SparkYarnSubmitter(config, hdfs, yarnClient);
    }

    // -------------------------------------------------------------------------
    // Submission
    // -------------------------------------------------------------------------

    /**
     * Uploads the fat-JAR to HDFS and submits the Spark application to YARN
     * without invoking spark-submit.
     *
     * @param job job parameters
     * @return handle to the submitted application
     */
    public SubmittedApplication submit(SparkJobConfig job) throws Exception {
        String hdfsJarUri = uploadJar(job.getLocalJarPath());
        ApplicationId appId = submitter.submit(job, hdfsJarUri);
        return new SubmittedApplication(appId, yarnClient);
    }

    /**
     * Submits a job whose fat-JAR was already uploaded to HDFS.
     * Use this to avoid re-uploading when resubmitting the same version.
     *
     * @param job       job parameters
     * @param hdfsJarUri HDFS URI returned by a previous {@link #uploadJar} call
     */
    public SubmittedApplication submitFromHdfs(SparkJobConfig job, String hdfsJarUri)
            throws Exception {
        ApplicationId appId = submitter.submit(job, hdfsJarUri);
        return new SubmittedApplication(appId, yarnClient);
    }

    // -------------------------------------------------------------------------
    // HDFS upload
    // -------------------------------------------------------------------------

    /**
     * Uploads a local fat-JAR to the configured HDFS directory (overwrite = true).
     *
     * @param localJarPath absolute local path
     * @return HDFS URI of the uploaded file
     */
    public String uploadJar(String localJarPath) throws Exception {
        Path src     = new Path(localJarPath);
        Path destDir = resolveJarUploadDir();
        Path dest    = new Path(destDir, src.getName());

        if (config.isKerberosEnabled()) {
            UserGroupInformation.getCurrentUser().doAs(
                (PrivilegedExceptionAction<Void>) () -> {
                    doUpload(src, destDir, dest);
                    return null;
                });
        } else {
            doUpload(src, destDir, dest);
        }

        log.info("Uploaded {} → {}", localJarPath, dest.toUri());
        return dest.toUri().toString();
    }

    private Path resolveJarUploadDir() throws IOException {
        String explicit = config.getHdfsJarUploadDir();
        if (explicit != null) {
            return hdfs.makeQualified(new Path(explicit));
        }
        return new Path(hdfs.getHomeDirectory(), ".spark-uploads");
    }

    private void doUpload(Path src, Path destDir, Path dest) throws IOException {
        if (!hdfs.exists(destDir)) {
            hdfs.mkdirs(destDir);
        }
        hdfs.copyFromLocalFile(false, true, src, dest);
    }

    // -------------------------------------------------------------------------
    // YARN status & control
    // -------------------------------------------------------------------------

    public ApplicationInfo getApplicationStatus(String applicationIdStr) throws Exception {
        ApplicationId appId = ApplicationId.fromString(applicationIdStr);
        return toInfo(yarnClient.getApplicationReport(appId));
    }

    public void killApplication(String applicationIdStr) throws Exception {
        ApplicationId appId = ApplicationId.fromString(applicationIdStr);
        yarnClient.killApplication(appId);
        log.info("Kill signal sent to {}", applicationIdStr);
    }

    /** Returns all Spark applications currently in a non-terminal YARN state. */
    public List<ApplicationInfo> listRunningApplications() throws Exception {
        EnumSet<YarnApplicationState> active = EnumSet.of(
            YarnApplicationState.NEW,
            YarnApplicationState.NEW_SAVING,
            YarnApplicationState.SUBMITTED,
            YarnApplicationState.ACCEPTED,
            YarnApplicationState.RUNNING
        );
        return yarnClient
            .getApplications(Collections.singleton("SPARK"), active)
            .stream()
            .map(this::toInfo)
            .collect(Collectors.toList());
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void close() throws IOException {
        try {
            if (yarnClient != null) yarnClient.stop();
        } catch (Exception e) {
            log.warn("Error stopping YARN client", e);
        }
        if (hdfs != null) hdfs.close();
    }

    private ApplicationInfo toInfo(ApplicationReport r) {
        return new ApplicationInfo(
            r.getApplicationId(), r.getName(),
            r.getYarnApplicationState(), r.getFinalApplicationStatus(),
            r.getProgress(), r.getTrackingUrl(), r.getDiagnostics());
    }
}
