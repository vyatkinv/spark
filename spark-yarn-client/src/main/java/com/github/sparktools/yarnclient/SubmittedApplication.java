package com.github.sparktools.yarnclient;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle to a Spark application submitted to YARN.
 * Returned by {@link SparkYarnClient#submit}.
 *
 * <p>Status queries reuse the {@link YarnClient} from the parent
 * {@link SparkYarnClient}, so this object must not be used after the client
 * is closed.
 */
public final class SubmittedApplication {

    private static final Logger log = LoggerFactory.getLogger(SubmittedApplication.class);

    private final ApplicationId applicationId;
    private final YarnClient yarnClient;

    SubmittedApplication(ApplicationId applicationId, YarnClient yarnClient) {
        this.applicationId = applicationId;
        this.yarnClient = yarnClient;
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
     * or the timeout expires.
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
            ApplicationInfo info = getStatus();
            log.debug("Application {} state={}", applicationId, info.getState());

            if (info.isFinished()) {
                return info;
            }
            long remaining = deadline - System.currentTimeMillis();
            Thread.sleep(Math.min(pollIntervalMs, Math.max(0, remaining)));
        }

        ApplicationInfo last = getStatus();
        log.warn("Timeout waiting for {} — last state: {}", applicationId, last.getState());
        return last;
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
