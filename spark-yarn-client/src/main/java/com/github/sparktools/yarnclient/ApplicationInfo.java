package com.github.sparktools.yarnclient;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

/** Snapshot of a YARN application's current state. */
public final class ApplicationInfo {

    private final ApplicationId applicationId;
    private final String name;
    private final YarnApplicationState state;
    private final FinalApplicationStatus finalStatus;
    private final float progress;
    private final String trackingUrl;
    private final String diagnostics;

    public ApplicationInfo(
            ApplicationId applicationId,
            String name,
            YarnApplicationState state,
            FinalApplicationStatus finalStatus,
            float progress,
            String trackingUrl,
            String diagnostics) {
        this.applicationId = applicationId;
        this.name = name;
        this.state = state;
        this.finalStatus = finalStatus;
        this.progress = progress;
        this.trackingUrl = trackingUrl;
        this.diagnostics = diagnostics;
    }

    public ApplicationId getApplicationId() { return applicationId; }
    public String getName() { return name; }
    public YarnApplicationState getState() { return state; }
    public FinalApplicationStatus getFinalStatus() { return finalStatus; }
    public float getProgress() { return progress; }
    public String getTrackingUrl() { return trackingUrl; }
    public String getDiagnostics() { return diagnostics; }

    public boolean isRunning() {
        return state == YarnApplicationState.RUNNING
                || state == YarnApplicationState.ACCEPTED
                || state == YarnApplicationState.SUBMITTED
                || state == YarnApplicationState.NEW;
    }

    public boolean isFinished() {
        return state == YarnApplicationState.FINISHED
                || state == YarnApplicationState.FAILED
                || state == YarnApplicationState.KILLED;
    }

    public boolean isSucceeded() {
        return state == YarnApplicationState.FINISHED
                && finalStatus == FinalApplicationStatus.SUCCEEDED;
    }

    @Override
    public String toString() {
        return String.format("ApplicationInfo{id=%s, name='%s', state=%s, finalStatus=%s, progress=%.1f%%}",
                applicationId, name, state, finalStatus, progress * 100);
    }
}
