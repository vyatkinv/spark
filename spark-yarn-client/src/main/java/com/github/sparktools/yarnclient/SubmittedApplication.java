package com.github.sparktools.yarnclient;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Handle to a Spark application submitted to YARN.
 * Returned by {@link SparkYarnClient#submit}.
 *
 * <p>Status queries reuse the {@link YarnClient} from the parent
 * {@link SparkYarnClient}, so this object must not be used after the client
 * is closed.
 *
 * <p>When the application reaches a terminal state, the HDFS staging directory
 * (which may contain a keytab) is deleted automatically. This mirrors the
 * cleanup that Spark's ApplicationMaster performs, providing a safety net
 * for cases where the AM crashes before it can clean up.
 */
public final class SubmittedApplication {

    private static final Logger log = LoggerFactory.getLogger(SubmittedApplication.class);

    private final ApplicationId applicationId;
    private final YarnClient yarnClient;
    private final FileSystem hdfs;
    private final Path stagingDir;

    SubmittedApplication(ApplicationId applicationId, YarnClient yarnClient,
            FileSystem hdfs, Path stagingDir) {
        this.applicationId = applicationId;
        this.yarnClient = yarnClient;
        this.hdfs = hdfs;
        this.stagingDir = stagingDir;
    }

    public ApplicationId getApplicationId() { return applicationId; }

    public ApplicationInfo getStatus() throws Exception {
        return toInfo(yarnClient.getApplicationReport(applicationId));
    }

    public void kill() throws Exception {
        yarnClient.killApplication(applicationId);
        log.info("Kill signal sent to {}", applicationId);
    }

    /**
     * Blocks until the application reaches a terminal state (FINISHED, FAILED, or KILLED)
     * or the timeout expires. When the application terminates, the HDFS staging directory
     * is cleaned up (deletes keytab, conf archive, and other staged files).
     *
     * @param timeoutMs maximum wait time in milliseconds
     * @return final application info
     * @throws InterruptedException if the thread is interrupted while waiting
     * @throws Exception            on YARN communication error
     */
    public ApplicationInfo waitForTermination(long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        long pollIntervalMs = 3_000;

        while (System.currentTimeMillis() < deadline) {
            ApplicationInfo info;
            try {
                info = getStatus();
            } catch (ApplicationNotFoundException e) {
                log.warn("Application {} not found — cleaning up staging dir", applicationId);
                cleanupStagingDir();
                return new ApplicationInfo(applicationId, "",
                        YarnApplicationState.KILLED,
                        org.apache.hadoop.yarn.api.records.FinalApplicationStatus.KILLED,
                        0f, "", "Application not found");
            }
            log.debug("Application {} state={}", applicationId, info.getState());

            if (info.isFinished()) {
                cleanupStagingDir();
                return info;
            }
            long remaining = deadline - System.currentTimeMillis();
            Thread.sleep(Math.min(pollIntervalMs, Math.max(0, remaining)));
        }

        ApplicationInfo last = getStatus();
        log.warn("Timeout waiting for {} — last state: {}", applicationId, last.getState());
        return last;
    }

    /**
     * Deletes the HDFS staging directory (keytab, conf archive, etc.).
     * Safe to call multiple times — silently succeeds if already deleted
     * (e.g. by the Spark AM).
     */
    public void cleanupStagingDir() {
        if (stagingDir == null) return;
        try {
            if (hdfs.exists(stagingDir) && hdfs.delete(stagingDir, true)) {
                log.info("Cleaned up staging directory {}", stagingDir);
            }
        } catch (IOException e) {
            log.warn("Failed to clean up staging directory {}", stagingDir, e);
        }
    }

    private ApplicationInfo toInfo(org.apache.hadoop.yarn.api.records.ApplicationReport r) {
        return new ApplicationInfo(
            r.getApplicationId(), r.getName(),
            r.getYarnApplicationState(), r.getFinalApplicationStatus(),
            r.getProgress(), r.getTrackingUrl(), r.getDiagnostics());
    }

    @Override
    public String toString() {
        return "SubmittedApplication{id=" + applicationId + "}";
    }
}
