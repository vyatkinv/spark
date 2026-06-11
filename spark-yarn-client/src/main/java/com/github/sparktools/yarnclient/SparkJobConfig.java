package com.github.sparktools.yarnclient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Per-job Spark submission parameters.
 * Create via {@link #builder()}.
 */
public final class SparkJobConfig {

    private final String appName;
    private final String mainClass;
    private final String localJarPath;
    private final List<String> appArgs;
    private final Map<String, String> sparkConf;
    private final String deployMode;
    private final String queue;
    private final int numExecutors;
    private final String executorMemory;
    private final int executorCores;
    private final String driverMemory;
    private final int driverCores;

    private SparkJobConfig(Builder b) {
        this.appName = b.appName;
        this.mainClass = b.mainClass;
        this.localJarPath = b.localJarPath;
        this.appArgs = Collections.unmodifiableList(new ArrayList<>(b.appArgs));
        this.sparkConf = Collections.unmodifiableMap(new HashMap<>(b.sparkConf));
        this.deployMode = b.deployMode;
        this.queue = b.queue;
        this.numExecutors = b.numExecutors;
        this.executorMemory = b.executorMemory;
        this.executorCores = b.executorCores;
        this.driverMemory = b.driverMemory;
        this.driverCores = b.driverCores;
    }

    public String getAppName() { return appName; }
    public String getMainClass() { return mainClass; }
    public String getLocalJarPath() { return localJarPath; }
    public List<String> getAppArgs() { return appArgs; }
    public Map<String, String> getSparkConf() { return sparkConf; }
    public String getDeployMode() { return deployMode; }
    public String getQueue() { return queue; }
    public int getNumExecutors() { return numExecutors; }
    public String getExecutorMemory() { return executorMemory; }
    public int getExecutorCores() { return executorCores; }
    public String getDriverMemory() { return driverMemory; }
    public int getDriverCores() { return driverCores; }

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private String appName;
        private String mainClass;
        private String localJarPath;
        private final List<String> appArgs = new ArrayList<>();
        private final Map<String, String> sparkConf = new HashMap<>();
        private String deployMode = "cluster";
        private String queue = "default";
        private int numExecutors = 2;
        private String executorMemory = "1g";
        private int executorCores = 1;
        private String driverMemory = "1g";
        private int driverCores = 1;

        public Builder appName(String appName) {
            this.appName = Objects.requireNonNull(appName);
            return this;
        }

        public Builder mainClass(String mainClass) {
            this.mainClass = Objects.requireNonNull(mainClass);
            return this;
        }

        /** Local filesystem path to the fat-JAR that will be uploaded to HDFS. */
        public Builder localJarPath(String path) {
            this.localJarPath = Objects.requireNonNull(path);
            return this;
        }

        public Builder addArg(String arg) {
            appArgs.add(Objects.requireNonNull(arg));
            return this;
        }

        public Builder addArgs(List<String> args) {
            appArgs.addAll(args);
            return this;
        }

        /** Override or add a Spark configuration key for this job only. */
        public Builder sparkConf(String key, String value) {
            this.sparkConf.put(Objects.requireNonNull(key), Objects.requireNonNull(value));
            return this;
        }

        /** {@code cluster} (default) or {@code client}. */
        public Builder deployMode(String deployMode) {
            this.deployMode = deployMode;
            return this;
        }

        public Builder queue(String queue) {
            this.queue = Objects.requireNonNull(queue);
            return this;
        }

        public Builder numExecutors(int n) { this.numExecutors = n; return this; }
        public Builder executorMemory(String mem) { this.executorMemory = Objects.requireNonNull(mem); return this; }
        public Builder executorCores(int cores) { this.executorCores = cores; return this; }
        public Builder driverMemory(String mem) { this.driverMemory = Objects.requireNonNull(mem); return this; }
        public Builder driverCores(int cores) { this.driverCores = cores; return this; }

        public SparkJobConfig build() {
            Objects.requireNonNull(appName, "appName is required");
            Objects.requireNonNull(mainClass, "mainClass is required");
            Objects.requireNonNull(localJarPath, "localJarPath is required");
            return new SparkJobConfig(this);
        }
    }
}
