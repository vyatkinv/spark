# spark-yarn-client

Lightweight Java library for submitting Spark jobs to YARN **without a local Spark installation**.

Internally replicates what `spark-submit --master yarn --deploy-mode cluster` does:
uploads a conf archive to HDFS, resolves Spark distribution jars, builds a
`ContainerLaunchContext`, and calls the YARN API directly. The Spark ApplicationMaster
that runs inside the cluster is the standard one from your Spark distribution — the
library only eliminates the need for `spark-submit` on the submitting machine.

---

## Table of contents

1. [Prerequisites](#prerequisites)
2. [Dependency](#dependency)
3. [Quick start](#quick-start)
4. [Pre-staging Spark jars on HDFS](#pre-staging-spark-jars-on-hdfs)
5. [SparkYarnConfig reference](#sparkyarnconfig-reference)
6. [SparkJobConfig reference](#sparkjobconfig-reference)
7. [Monitoring submitted applications](#monitoring-submitted-applications)
8. [Kerberos](#kerberos)
9. [YARN HA and HDFS HA](#yarn-ha-and-hdfs-ha)
10. [Providing a custom Hadoop Configuration](#providing-a-custom-hadoop-configuration)
11. [How it works](#how-it-works)

---

## Prerequisites

| Requirement | Details |
|---|---|
| Java | 8 or later |
| Spark jars pre-staged on HDFS | See [Pre-staging Spark jars on HDFS](#pre-staging-spark-jars-on-hdfs) |
| Network access | The submitting machine must reach the YARN Resource Manager and HDFS NameNode |
| `core-site.xml` / `yarn-site.xml` | Optional but recommended — Hadoop config files on the submitting machine so the client can auto-discover the cluster |

No Spark installation is required on the submitting machine.

---

## Dependency

The module is part of the Apache Spark source tree (built with the `yarn` profile).
After building Spark locally, add the following to your project:

**Maven:**

```xml
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-yarn-client_2.12</artifactId>
    <version>3.5.9-SNAPSHOT</version>
</dependency>
```

**Gradle:**

```groovy
implementation 'org.apache.spark:spark-yarn-client_2.12:3.5.9-SNAPSHOT'
```

The library itself depends only on `hadoop-common`, `hadoop-hdfs-client`, and
`hadoop-yarn-client` (all at the Hadoop version Spark was built with), plus `slf4j-api`.
These are declared with `${hadoop.deps.scope}` — `compile` by default, `provided` in a
standard Spark distribution — so add the Hadoop artifacts to your project's classpath if
you are not already depending on them.

---

## Quick start

### 1. Build a minimal fat-JAR for your Spark application

Your application must be packaged as a self-contained fat-JAR. With Maven this is usually
`maven-shade-plugin`; with Gradle use `shadowJar`. The Spark and Hadoop libraries should
be `provided`/`compileOnly` to keep the JAR small — they are supplied by the cluster.

```java
// Minimal Spark application
public class WordCount {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("WordCount").getOrCreate();
        spark.read().textFile(args[0])
             .flatMap((String line) -> Arrays.asList(line.split(" ")).iterator(),
                      Encoders.STRING())
             .groupByKey(w -> w, Encoders.STRING())
             .count()
             .write().csv(args[1]);
        spark.stop();
    }
}
```

### 2. Pre-stage Spark jars on HDFS (one-time setup)

```bash
hdfs dfs -mkdir -p /spark/jars
hdfs dfs -put $SPARK_HOME/jars/*.jar /spark/jars/
```

### 3. Submit the job

```java
// Cluster-level config (reuse across many submissions)
SparkYarnConfig config = SparkYarnConfig.builder()
    .hdfsUri("hdfs://namenode:8020")
    .sparkConf("spark.yarn.jars", "hdfs:///spark/jars/*.jar")
    .build();

// Per-job parameters
SparkJobConfig job = SparkJobConfig.builder()
    .appName("WordCount")
    .mainClass("com.example.WordCount")
    .localJarPath("/home/user/wordcount-all.jar")
    .numExecutors(4)
    .executorMemory("2g")
    .executorCores(2)
    .driverMemory("1g")
    .addArg("hdfs:///data/input.txt")
    .addArg("hdfs:///data/output")
    .build();

try (SparkYarnClient client = new SparkYarnClient(config)) {
    SubmittedApplication app = client.submit(job);

    System.out.println("Submitted: " + app.getApplicationId());

    // Wait up to 30 minutes
    ApplicationInfo result = app.waitForTermination(30 * 60_000L);
    System.out.println(result); // ApplicationInfo{id=..., state=FINISHED, finalStatus=SUCCEEDED}

    if (!result.isSucceeded()) {
        System.err.println("Job failed: " + result.getDiagnostics());
    }
}
```

When `localJarPath` is set, the fat-JAR is automatically uploaded to HDFS before
submission. If you pre-upload the JAR yourself or reuse the same version across many
jobs, skip the upload:

```java
String hdfsJar = client.uploadJar("/home/user/wordcount-all.jar");

// Reuse the same HDFS jar for multiple submissions
SubmittedApplication app1 = client.submitFromHdfs(job1, hdfsJar);
SubmittedApplication app2 = client.submitFromHdfs(job2, hdfsJar);
```

---

## Pre-staging Spark jars on HDFS

The cluster containers must have the Spark runtime jars available. Configure one of:

### Option A — individual jars glob (recommended for development)

Upload the Spark `jars/` directory and set `spark.yarn.jars`:

```bash
hdfs dfs -mkdir -p /spark/3.5.0/jars
hdfs dfs -put $SPARK_HOME/jars/*.jar /spark/3.5.0/jars/
```

```java
config = SparkYarnConfig.builder()
    .hdfsUri("hdfs://namenode:8020")
    .sparkConf("spark.yarn.jars", "hdfs:///spark/3.5.0/jars/*.jar")
    .build();
```

### Option B — single archive (faster container startup for large clusters)

Pack the jars into a zip and set `spark.yarn.archive`:

```bash
cd $SPARK_HOME/jars
zip -j /tmp/spark-libs.zip *.jar
hdfs dfs -put /tmp/spark-libs.zip /spark/3.5.0/spark-libs.zip
```

```java
config = SparkYarnConfig.builder()
    .hdfsUri("hdfs://namenode:8020")
    .sparkConf("spark.yarn.archive", "hdfs:///spark/3.5.0/spark-libs.zip")
    .build();
```

The archive is extracted by the NodeManager into `__spark_libs__/` inside the container
working directory and added to the CLASSPATH automatically.

### Option C — pre-installed on every node (`local:` prefix)

If Spark is installed at the same path on every NodeManager host, skip HDFS staging:

```java
config = SparkYarnConfig.builder()
    .hdfsUri("hdfs://namenode:8020")
    .sparkConf("spark.yarn.jars", "local:/opt/spark/jars/*")
    .build();
```

Entries with the `local:` prefix are added to the CLASSPATH without being staged as YARN
LocalResources.

---

## SparkYarnConfig reference

`SparkYarnConfig` holds cluster-level settings shared across all job submissions from a
single client instance. Build it once and reuse.

| Builder method | Required | Default | Description |
|---|---|---|---|
| `hdfsUri(String)` | yes | — | HDFS NameNode URI, e.g. `hdfs://namenode:8020` or `hdfs://nameservice` for HA |
| `hdfsJarUploadDir(String)` | no | `/spark-apps/jars` | HDFS directory where `uploadJar` places fat-JARs |
| `sparkConf(String key, String value)` | no | — | Spark property applied to every job (call multiple times). Use this for `spark.yarn.jars` / `spark.yarn.archive` and any cluster-wide defaults |
| `hadoopConf(Configuration)` | no | `new Configuration()` | Pre-built Hadoop `Configuration`. Auto-loaded from `core-site.xml` / `yarn-site.xml` on the classpath by default |
| `kerberos(String principal, String keytabPath)` | no | disabled | Enable Kerberos — see [Kerberos](#kerberos) |
| `javaHome(String)` | no | `$JAVA_HOME` | Path to Java on YARN cluster nodes, e.g. `/usr/lib/jvm/java-11`. Uses the `$JAVA_HOME` container environment variable by default |
| `amClass(String)` | no | `org.apache.spark.deploy.yarn.ApplicationMaster` | Override the YARN ApplicationMaster class. Change only for custom Spark forks |

### Precedence for Spark properties

When the same key appears in both `SparkYarnConfig.sparkConf()` and
`SparkJobConfig.sparkConf()`, the **job-level value wins**. This lets you set
cluster-wide defaults in `SparkYarnConfig` and override them per-job.

---

## SparkJobConfig reference

`SparkJobConfig` holds per-submission parameters.

| Builder method | Required | Default | Description |
|---|---|---|---|
| `appName(String)` | yes | — | YARN application name shown in the ResourceManager UI |
| `mainClass(String)` | yes | — | Fully-qualified main class inside the fat-JAR |
| `localJarPath(String)` | yes | — | Local filesystem path to the fat-JAR. The JAR is uploaded to HDFS automatically before submission |
| `deployMode(String)` | no | `cluster` | `cluster` (driver runs in YARN container) or `client` (driver runs on the submitting machine — experimental) |
| `queue(String)` | no | `default` | YARN queue name |
| `numExecutors(int)` | no | `2` | Number of executor containers |
| `executorMemory(String)` | no | `1g` | Memory per executor — accepts `512m`, `2g`, or bare MB number |
| `executorCores(int)` | no | `1` | CPU cores per executor |
| `driverMemory(String)` | no | `1g` | Memory for the AM/driver container |
| `driverCores(int)` | no | `1` | CPU cores for the AM/driver container |
| `addArg(String)` | no | — | Append a positional argument passed to the application's `main(String[])`. Call multiple times |
| `addArgs(List<String>)` | no | — | Append multiple arguments at once |
| `sparkConf(String key, String value)` | no | — | Per-job Spark property override (call multiple times). Useful for `spark.driver.extraJavaOptions`, `spark.sql.shuffle.partitions`, etc. |

### Memory format

Memory values accept:
- `"512m"` or `"512M"` — megabytes
- `"4g"` or `"4G"` — gigabytes  
- `"2048"` — bare integer interpreted as megabytes

---

## Monitoring submitted applications

`SparkYarnClient` provides convenience methods for polling and controlling jobs:

```java
// Poll state
ApplicationInfo info = app.getStatus();
System.out.printf("State: %s  Progress: %.0f%%%n",
    info.getState(), info.getProgress() * 100);

// Block until terminal state or timeout
ApplicationInfo result = app.waitForTermination(60 * 60_000L);
if (result.isSucceeded()) {
    System.out.println("Done.");
} else if (result.isFinished()) {
    System.err.println("Failed: " + result.getDiagnostics());
} else {
    System.err.println("Timed out, still running.");
}

// Kill
app.kill();

// List all currently running Spark apps on the cluster
List<ApplicationInfo> running = client.listRunningApplications();

// Query a previously submitted job by ID
ApplicationInfo old = client.getApplicationStatus("application_1700000000000_0042");

// Kill by ID
client.killApplication("application_1700000000000_0042");
```

`ApplicationInfo` fields:

| Method | Type | Description |
|---|---|---|
| `getApplicationId()` | `ApplicationId` | YARN application ID |
| `getName()` | `String` | Application name |
| `getState()` | `YarnApplicationState` | Current state: `NEW`, `SUBMITTED`, `ACCEPTED`, `RUNNING`, `FINISHED`, `FAILED`, `KILLED` |
| `getFinalStatus()` | `FinalApplicationStatus` | Terminal status: `SUCCEEDED`, `FAILED`, `KILLED`, `UNDEFINED` |
| `getProgress()` | `float` | Progress 0.0–1.0 |
| `getTrackingUrl()` | `String` | YARN tracking / Spark History Server URL |
| `getDiagnostics()` | `String` | Error message when the job fails |
| `isRunning()` | `boolean` | `true` if in a non-terminal state |
| `isFinished()` | `boolean` | `true` if in `FINISHED`, `FAILED`, or `KILLED` |
| `isSucceeded()` | `boolean` | `true` if `FINISHED` + `SUCCEEDED` |

---

## Kerberos

Enable Kerberos by calling `.kerberos(principal, keytabPath)` on the config builder.
The client performs a keytab login at construction time via Hadoop `UserGroupInformation`:

```java
SparkYarnConfig config = SparkYarnConfig.builder()
    .hdfsUri("hdfs://nameservice")
    .sparkConf("spark.yarn.jars", "hdfs:///spark/jars/*.jar")
    .kerberos("svc-spark@CORP.COM", "/etc/security/keytabs/spark.keytab")
    .build();

SparkYarnClient client = new SparkYarnClient(config);
// All HDFS writes and YARN API calls now use the Kerberos-authenticated identity.
```

The library also:
- Obtains HDFS and YARN RM delegation tokens and embeds them in the
  `ContainerLaunchContext` so the ApplicationMaster can start without a Kerberos ticket.
- Sets `spark.kerberos.principal` and `spark.kerberos.keytab` in the conf archive so Spark
  renews tokens inside the AM automatically.

### Token renewal for long-running services

For services that submit jobs continuously, call `KerberosSupport.reloginIfNeeded()` from
a background thread to prevent TGT expiry:

```java
ScheduledExecutorService renewer = Executors.newSingleThreadScheduledExecutor();
renewer.scheduleAtFixedRate(
    () -> {
        try { KerberosSupport.reloginIfNeeded(); }
        catch (IOException e) { log.error("Kerberos relogin failed", e); }
    },
    1, 1, TimeUnit.HOURS);
```

### Hadoop configuration for Kerberos clusters

On a Kerberos-secured cluster the `core-site.xml` typically contains:

```xml
<property>
  <name>hadoop.security.authentication</name>
  <value>kerberos</value>
</property>
<property>
  <name>hadoop.security.authorization</name>
  <value>true</value>
</property>
```

If these files are on the classpath, `new Configuration()` picks them up automatically. If
not, pass a pre-configured `Configuration`:

```java
Configuration conf = new Configuration();
conf.set("hadoop.security.authentication", "kerberos");
conf.set("fs.defaultFS", "hdfs://nameservice");
conf.set("yarn.resourcemanager.address", "rm-host:8032");

SparkYarnConfig config = SparkYarnConfig.builder()
    .hdfsUri("hdfs://nameservice")
    .hadoopConf(conf)
    .kerberos("svc-spark@CORP.COM", "/etc/security/keytabs/spark.keytab")
    .build();
```

---

## YARN HA and HDFS HA

HA configurations are handled by Hadoop's `Configuration` object — pass the full
`yarn-site.xml` / `hdfs-site.xml` settings and the Hadoop client libraries will perform
failover automatically.

### Loading config from files

The simplest approach: put `core-site.xml`, `hdfs-site.xml`, and `yarn-site.xml` on the
classpath. `new Configuration()` auto-discovers them and `SparkYarnConfig` uses them:

```java
// Config files on the classpath — no extra code needed
SparkYarnConfig config = SparkYarnConfig.builder()
    .hdfsUri("hdfs://mycluster")           // HA nameservice
    .sparkConf("spark.yarn.jars", "hdfs:///spark/jars/*.jar")
    .build();
```

### Configuring programmatically

```java
Configuration conf = new Configuration(false); // false = don't load defaults
conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
conf.addResource(new Path("/etc/hadoop/conf/yarn-site.xml"));

SparkYarnConfig config = SparkYarnConfig.builder()
    .hdfsUri("hdfs://mycluster")
    .hadoopConf(conf)
    .build();
```

The library automatically forwards connectivity-related keys to the Spark AM:
`fs.defaultFS`, `yarn.resourcemanager.address`, HDFS HA nameservice config
(`dfs.nameservices`, `dfs.ha.*`, `dfs.client.*`), and YARN HA config
(`yarn.resourcemanager.ha.*`). This ensures the ApplicationMaster running inside
the cluster uses the same cluster topology as the submitting client.

---

## Providing a custom Hadoop Configuration

You may need to supply a `Configuration` explicitly in these scenarios:

- Hadoop config files are not on the classpath
- You connect to multiple clusters from the same JVM
- You run integration tests with an embedded mini-cluster

```java
Configuration conf = new Configuration();
conf.set("fs.defaultFS", "hdfs://namenode:8020");
conf.set("yarn.resourcemanager.address", "rm-host:8032");
// Optional: tune block size, replication, timeouts, etc.
conf.setInt("dfs.replication", 1);

SparkYarnConfig config = SparkYarnConfig.builder()
    .hdfsUri("hdfs://namenode:8020")
    .hadoopConf(conf)
    .sparkConf("spark.yarn.jars", "hdfs:///spark/jars/*.jar")
    .build();
```

Each `SparkYarnClient` instance owns its own `FileSystem` connection (created with
`FileSystem.newInstance()`) so closing one client does not affect another, even when they
share the same `Configuration` or target the same HDFS cluster.

---

## How it works

The submission flow mirrors Spark's internal `Client.scala` step by step:

```
SparkYarnClient.submit(job)
 │
 ├─ uploadJar(job.localJarPath)
 │     └─ hdfs.copyFromLocalFile → hdfs:///spark-apps/jars/app.jar
 │
 └─ SparkYarnSubmitter.submit(job, hdfsJarUri)
       │
       ├─ yarnClient.createApplication()
       │     └─ allocates application_XXXXX_NNNN
       │
       ├─ hdfs.mkdirs(.sparkStaging/<appId>/)
       │
       ├─ uploadConfArchive()
       │     builds __spark_conf__.zip containing:
       │       __spark_conf__.properties  ← all Spark properties
       │       yarn-site.xml              ← full Hadoop conf for the AM
       │     → staged as ARCHIVE LocalResource
       │       extracted to {{PWD}}/__spark_conf__/ by NodeManager
       │
       ├─ resolveSparkLibs()
       │     spark.yarn.archive  → single ARCHIVE LocalResource
       │     spark.yarn.jars     → one FILE LocalResource per jar
       │
       ├─ buildContainerEnv()
       │     CLASSPATH = yarn.application.classpath
       │               + {{PWD}}
       │               + {{PWD}}/__spark_conf__
       │               + spark lib entries
       │
       ├─ buildAmCommand()
       │     $JAVA_HOME/bin/java -server -Xmx{driverMem}m
       │       org.apache.spark.deploy.yarn.ApplicationMaster
       │       --class com.example.MyApp
       │       --jar hdfs:///spark-apps/jars/app.jar
       │       --properties-file {{PWD}}/__spark_conf__/__spark_conf__.properties
       │       [--arg ...]
       │
       └─ yarnClient.submitApplication(ctx)
             → returns ApplicationId to caller
```

The Spark ApplicationMaster running inside the YARN container reads
`__spark_conf__.properties` on startup, finds the HDFS and YARN addresses embedded there,
and proceeds to request executor containers exactly as it would after a normal
`spark-submit` invocation.
