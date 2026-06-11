package com.github.sparktools.yarnclient;

import org.apache.hadoop.conf.Configuration;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable cluster-level configuration: HDFS, YARN, Kerberos, and Spark distribution location.
 *
 * <p>No Spark installation is required on the submitting machine. The Spark jars must
 * be pre-staged on HDFS and referenced via {@code spark.yarn.jars} or
 * {@code spark.yarn.archive} (set through {@link Builder#sparkConf}).
 *
 * <p>Example:
 * <pre>{@code
 * SparkYarnConfig config = SparkYarnConfig.builder()
 *     .hdfsUri("hdfs://namenode:8020")
 *     .sparkConf("spark.yarn.jars", "hdfs:///spark/jars/*.jar")
 *     .kerberos("svc-spark@CORP.COM", "/etc/keytabs/spark.keytab")
 *     .build();
 * }</pre>
 */
public final class SparkYarnConfig {

    /** Default AM class for Spark 2.x+. Override only for custom Spark forks or tests. */
    public static final String DEFAULT_AM_CLASS =
            "org.apache.spark.deploy.yarn.ApplicationMaster";

    private final String hdfsUri;
    private final String hdfsJarUploadDir;
    private final String javaHome;
    private final String amClass;
    private final String kerberosPrincipal;
    private final String kerberosKeytab;
    private final Configuration hadoopConf;
    private final Map<String, String> extraSparkConf;

    private SparkYarnConfig(Builder b) {
        this.hdfsUri = b.hdfsUri;
        this.hdfsJarUploadDir = b.hdfsJarUploadDir;
        this.javaHome = b.javaHome;
        this.amClass = b.amClass;
        this.kerberosPrincipal = b.kerberosPrincipal;
        this.kerberosKeytab = b.kerberosKeytab;
        this.hadoopConf = b.hadoopConf;
        this.extraSparkConf = Collections.unmodifiableMap(new HashMap<>(b.extraSparkConf));
    }

    public String getHdfsUri() { return hdfsUri; }
    public String getHdfsJarUploadDir() { return hdfsJarUploadDir; }
    /** Java home on YARN cluster nodes. Null means use {@code $JAVA_HOME} env variable. */
    public String getJavaHome() { return javaHome; }
    public String getAmClass() { return amClass; }
    public String getKerberosPrincipal() { return kerberosPrincipal; }
    public String getKerberosKeytab() { return kerberosKeytab; }
    public Configuration getHadoopConf() { return hadoopConf; }
    public Map<String, String> getExtraSparkConf() { return extraSparkConf; }

    public boolean isKerberosEnabled() {
        return kerberosPrincipal != null && !kerberosPrincipal.isEmpty()
                && kerberosKeytab != null && !kerberosKeytab.isEmpty();
    }

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private String hdfsUri;
        private String hdfsJarUploadDir = "/spark-apps/jars";
        private String javaHome;
        private String amClass = DEFAULT_AM_CLASS;
        private String kerberosPrincipal;
        private String kerberosKeytab;
        private Configuration hadoopConf = new Configuration();
        private final Map<String, String> extraSparkConf = new HashMap<>();

        /** HDFS namenode URI, e.g. {@code hdfs://namenode:8020}. Required. */
        public Builder hdfsUri(String hdfsUri) {
            this.hdfsUri = Objects.requireNonNull(hdfsUri);
            return this;
        }

        /** HDFS directory where fat-JARs are uploaded (default: /spark-apps/jars). */
        public Builder hdfsJarUploadDir(String dir) {
            this.hdfsJarUploadDir = Objects.requireNonNull(dir);
            return this;
        }

        /**
         * Path to Java on YARN cluster nodes (e.g. {@code /usr/lib/jvm/java-11}).
         * If not set, the AM command uses the {@code $JAVA_HOME} environment variable
         * that YARN injects into each container.
         */
        public Builder javaHome(String javaHome) {
            this.javaHome = javaHome;
            return this;
        }

        /**
         * Override the YARN ApplicationMaster class name.
         * Default: {@value #DEFAULT_AM_CLASS}.
         * Override only for custom Spark forks or integration tests.
         */
        public Builder amClass(String amClass) {
            this.amClass = Objects.requireNonNull(amClass);
            return this;
        }

        /** Enable Kerberos authentication with a keytab. */
        public Builder kerberos(String principal, String keytabPath) {
            this.kerberosPrincipal = Objects.requireNonNull(principal);
            this.kerberosKeytab = Objects.requireNonNull(keytabPath);
            return this;
        }

        /**
         * Supply a pre-built Hadoop {@link Configuration}.
         * Useful in tests (mini-cluster config) or when hadoop-site.xml is non-standard.
         */
        public Builder hadoopConf(Configuration conf) {
            this.hadoopConf = Objects.requireNonNull(conf);
            return this;
        }

        /**
         * Add a Spark configuration property applied to every submitted job.
         *
         * <p>Use this to set the Spark jars location — one of these is required for
         * job submission without a local Spark installation:
         * <ul>
         *   <li>{@code spark.yarn.jars=hdfs:///spark/jars/*.jar} — individual jars glob</li>
         *   <li>{@code spark.yarn.archive=hdfs:///spark/spark-libs.zip} — single archive</li>
         * </ul>
         */
        public Builder sparkConf(String key, String value) {
            extraSparkConf.put(Objects.requireNonNull(key), Objects.requireNonNull(value));
            return this;
        }

        public SparkYarnConfig build() {
            Objects.requireNonNull(hdfsUri, "hdfsUri is required");
            return new SparkYarnConfig(this);
        }
    }
}
